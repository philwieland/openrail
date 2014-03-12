/*
    Copyright (C) 2013 Phil Wieland

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.

    phil@philwieland.com

*/

#include <unistd.h>

#include <stdio.h>
#include <string.h>
#include <signal.h>
#include <time.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/prctl.h>
#include <sys/resource.h>
#include <mysql.h>
#include <sys/sysinfo.h>

#include "stomp.h"
#include "jsmn.h"
#include "misc.h"
#include "db.h"

#define NAME  "vstpdb"
#define BUILD "V312"

static void perform(void);
static void process_message(const char * body);
static void process_vstp(const char * string, const jsmntok_t * tokens);
static void process_delete_schedule(const char * string, const jsmntok_t * tokens);
static void process_create_schedule(const char * string, const jsmntok_t * tokens, const word update);
static word process_create_schedule_location(const char * string, const jsmntok_t * tokens, const int index, const unsigned long schedule_id);
static void process_update_schedule(const char * string, const jsmntok_t * tokens);

static void jsmn_dump_tokens(const char const * string, const jsmntok_t const * tokens, const word object_index);
static void dump_headers(void);
static char * report_error(int error);
static void report_stats(void);
#define INVALID_SORT_TIME 9999
static word get_sort_time_vstp(const char const * buffer);
static char * vstp_to_CIF_time(const char * buffer);
static void log_message(const char * message);
static char * tiploc_name(const char * const tiploc);

static word debug, run, interrupt, holdoff, huyton_flag;
static char zs[4096];

static char headers[1024], body[65536];
#define NUM_TOKENS 8192
static jsmntok_t tokens[NUM_TOKENS];

#define NOT_DELETED 0xffffffffL

#define REPORT_INTERVAL (24*60*60)

// Stats
time_t start_time;
enum stats_categories {Bytes, ConnectAttempt, GoodMessage, DeleteHit, DeleteMiss, DeleteMulti, Create, 
                       UpdateCreate, UpdateDeleteMiss, UpdateDeleteMulti,
                       NotMessage, NotVSTP, NotTransaction, MAXstats};
qword stats[MAXstats];
qword grand_stats[MAXstats];
const char * stats_category[MAXstats] = 
   {
      "Bytes", "Connect Attempt", "Good Message", "Delete Hit", "Delete Miss", "Delete Multiple Hit", "Create",
      "Update", "Update Delete Miss", "Update Delete Mult. Hit",
      "Not a message", "Invalid or Not VSTP", "Unknown Transaction",
   };

// Signal handling
void termination_handler(int signum)
{
   if(signum != SIGHUP)
   {
      run = false;
      interrupt = true;
   }
}

int main(int argc, char *argv[])
{
   int c;
   word usage = true;
   while ((c = getopt (argc, argv, "c:")) != -1)
   {
      switch (c)
      {
      case 'c':
         if(load_config(optarg))
         {
            printf("Failed to read config file \"%s\".\n", optarg);
            usage = true;
         }
         else
         {
            usage = false;
         }
         break;
      case '?':
      default:
         usage = true;
         break;
      }
   }

   if(usage)
   {
      printf("No config file passed.\n\n\tUsage: %s -c /path/to/config/file.conf\n\n", argv[0] );
      exit(1);
   }

   int lfp = 0;

   /* Determine debug mode
     
      We don't always want to run in production mode, so we
      read the content of the debug config variable and act 
      on it accordingly.
     
      If we do not have a variable set, we assume production 
      mode */
   if ( strcmp(conf.debug,"true") == 0  )
   {
      debug = 1;
   }
   else
   {
      debug = 0;
   }
   
   start_time = time(NULL);

   // Set up log
   _log_init(debug?"/tmp/vstpdb.log":"/var/log/garner/vstpdb.log", debug?1:0);

   // Enable core dumps
   struct rlimit limit;
   if(!getrlimit(RLIMIT_CORE, &limit))
   {
      limit.rlim_cur = RLIM_INFINITY;
      setrlimit(RLIMIT_CORE, &limit);
   }

   // DAEMONISE
   if(debug != 1)
   {
      int i=fork();
      if (i<0)
      {
         /* fork error */
         _log(CRITICAL, "fork() error.  Aborting.");
         exit(1);
      }
      if (i>0) exit(0); /* parent exits */
      /* child (daemon) continues */
        
      pid_t sid = setsid(); /* obtain a new process group */   
      if(sid < 0)
      {
         /* setsid error */
         _log(CRITICAL, "setsid() error.  Aborting.");
         exit(1);
      }

      for (i=getdtablesize(); i>=0; --i) close(i); /* close all descriptors */

      umask(022); // Created files will be rw for root, r for all others

      i = chdir("/var/run/");  
      if(i < 0)
      {
         /* chdir error */
         _log(CRITICAL, "chdir() error.  Aborting.");
         exit(1);
      }
        
      if((lfp = open("/var/run/vstpdb.pid", O_RDWR|O_CREAT, 0640)) < 0)
      {
         _log(CRITICAL, "Unable to open pid file \"/var/run/vstpdb.pid\".  Aborting.");
         exit(1); /* can not open */
      }
           
      if (lockf(lfp,F_TLOCK,0)<0)
      {
         _log(CRITICAL, "Failed to obtain lock.  Aborting.");
         exit(1); /* can not lock */
      }
        
      char str[128];
      sprintf(str, "%d\n", getpid());
      i = write(lfp, str, strlen(str)); /* record pid to lockfile */
      
      _log(GENERAL, "");
      sprintf(zs, "%s %s", NAME, BUILD);
      _log(GENERAL, zs);
      _log(GENERAL, "Running as daemon.");
   }
   else
   {
      _log(GENERAL, "");
      sprintf(zs, "%s %s", NAME, BUILD);
      _log(GENERAL, zs);
      _log(GENERAL, "Running in local mode.");
   }
     
   // Check core dumps
   if(prctl(PR_GET_DUMPABLE, 0, 0, 0, 0) != 1)
   {
      _log(MAJOR, "PR_GET_DUMPABLE not 1.");
   }
   else
   {
      _log(DEBUG, "PR_GET_DUMPABLE is 1.");
   }
   if(!getrlimit(RLIMIT_CORE, &limit))
   {
      if(limit.rlim_cur != RLIM_INFINITY)
      {
         _log(MAJOR, "RLIMIT_CORE not RLIM_INFINITY.");
      }
      else
      {
         _log(DEBUG, "RLIMIT_CORE is RLIM_INFINITY.");
      }
   }
   else
   {
      _log(MAJOR, "Unable to determine RLIMIT_CORE.");
   }

   run = true;
   interrupt = false;

   if(signal(SIGTERM, termination_handler) == SIG_IGN) signal(SIGTERM, SIG_IGN);
   if(signal(SIGINT,  termination_handler) == SIG_IGN) signal(SIGINT,  SIG_IGN);
   if(signal(SIGHUP,  termination_handler) == SIG_IGN) signal(SIGHUP,  SIG_IGN);
   if(!debug) signal(SIGCHLD,SIG_IGN); /* ignore child */
   if(!debug) signal(SIGTSTP,SIG_IGN); /* ignore tty signals */
   if(!debug) signal(SIGTTOU,SIG_IGN);
   if(!debug) signal(SIGTTIN,SIG_IGN);

   // Zero the stats
   {
      word i;
      for(i=0; i < MAXstats; i++) 
      {
         stats[i] = 0;
         grand_stats[i] = 0;
      }
   }

   // Startup delay
   if(!debug)
   {
      struct sysinfo info;
      // If uptime query fails OR uptime is small, wait for system to stabilise.
      if(sysinfo(&info) || info.uptime < 64)
      {
         _log(GENERAL, "Startup delay ...");
         word i;
         for(i = 0; i < 64 && run; i++) sleep(1);
      }
   }

   perform();

   if(lfp) close(lfp);

   return 0;
}

static void perform(void)
{
   int rc;
   time_t last_report;

   last_report = time(NULL) / REPORT_INTERVAL;

   // Initialise database
   db_init(conf.db_server, conf.db_user, conf.db_pass, conf.db_name);

   log_message("");
   log_message("");
   log_message("vstpdb started.");
   while(run)
   {   
      stats[ConnectAttempt]++;
      _log(GENERAL, "Connecting socket ...");
      rc=stomp_connect("datafeeds.networkrail.co.uk", 61618);
      if(rc)
      {
         sprintf(zs,"Failed to connect.  Error %d %s", rc, report_error(rc));
         _log(CRITICAL, zs);
      }
      else
      {
         _log(GENERAL, "Connected.");
         holdoff = 0;
         {
            strcpy(headers, "CONNECT\n");
            strcat(headers, "login:");
            strcat(headers, conf.nr_user);  
            strcat(headers, "\npasscode:");
            strcat(headers, conf.nr_pass);
            strcat(headers, "\n");          
            if(debug)
            {
               sprintf(zs, "client-id:%s-vstpdb-debug\n", conf.nr_user);
               strcat(headers, zs);
            }
            else
            {
               sprintf(zs, "client-id:%s-vstpdb-%s\n", conf.nr_user, abbreviated_host_id());
               strcat(headers, zs);
            }          
            strcat(headers, "heart-beat:0,20000\n");          
            strcat(headers, "\n");
     
            rc = stomp_tx(headers);
         }
         if(rc)
         {
            sprintf(zs,"Failed to transmit CONNECT message.  Error %d %s", rc, report_error(rc));
            _log(CRITICAL, zs);
         }
         else
         {
            _log(GENERAL, "Sent CONNECT message.  Reading response.");
            rc = stomp_rx(headers, sizeof(headers), body, sizeof(body));
            if(rc)
            {
               sprintf(zs,"Failed to receive.  Error %d %s", rc, report_error(rc));
               _log(CRITICAL, zs);
            }
            else
            {
               sprintf(zs, "Response: Body=\"%s\", Headers:", body);
               _log(GENERAL, zs);
               dump_headers();
               {
                  strcpy(headers, "SUBSCRIBE\n");

                  // Headers
                  strcat(headers, "destination:/topic/VSTP_ALL\n");      
                  if(debug)
                  {
                     sprintf(zs, "activemq.subscriptionName:%s-vstpdb-debug\n", conf.nr_user);
                     strcat(headers, zs);
                  }
                  else
                  {
                     sprintf(zs, "activemq.subscriptionName:%s-vstpdb-%s\n", conf.nr_user, abbreviated_host_id());
                     strcat(headers, zs);
                  }
                  strcat(headers, "id:1\n");      
                  strcat(headers, "ack:client\n");   
                  strcat(headers, "\n");

                  rc = stomp_tx(headers);
               }
               if(rc)
               {
                  sprintf(zs,"Failed to transmit SUBSCRIBE message.  Error %d %s", rc, report_error(rc));
                  _log(CRITICAL, zs);
               }
               else
               {
                  _log(GENERAL, "Sent SUBSCRIBE message.  Waiting for messages ...");

                  int run_receive = 1;
                  while(run && run_receive)
                  {
                     if( time(NULL) / REPORT_INTERVAL != last_report)
                     {
                        report_stats();
                        last_report = time(NULL) / REPORT_INTERVAL;
                     }
                     rc = stomp_rx(headers, sizeof(headers), body, sizeof(body));
                     run_receive = (rc == 0);
                     if(rc && run)
                     {
                        // Don't report if the error is due to an interrupt
                        sprintf(zs, "Error receiving frame: %d %s", rc, report_error(rc));
                        _log(MAJOR, zs);
                     }
                     
                     if(run_receive)
                     {
                        char message_id[256];
                        message_id[0] = '\0';

                        stats[Bytes] += strlen(headers);
                        stats[Bytes] += strlen(body);

                        if(!strstr(headers, "MESSAGE"))
                        {
                           _log(MAJOR, "Frame received is not a MESSAGE:");
                           dump_headers();
                           run_receive = false;
                           stats[NotMessage]++;
                        }
                     
                        // Find message ID
                        char * message_id_header = strstr(headers, "message-id:");
                        if(run_receive)
                        {
                           if(message_id_header)
                           {
                              size_t i = 0;
                              message_id_header += 11;
                              while(*message_id_header != '\n') message_id[i++] = *message_id_header++;
                              message_id[i++] = '\0';
                           }
                           else
                           {
                              _log(MAJOR, "No message ID found:");
                              dump_headers();
                           }
                        }

                        //sprintf(zs, "Message id = \"%s\"", message_id);
                        //_log(GENERAL, zs);
                        
                        // Process the message
                        if(run_receive) process_message(body);

                        // Send ACK
                        if(run_receive && message_id[0])
                        {
                           strcpy(headers, "ACK\n");
                           strcat(headers, "subscription:1\n");
                           strcat(headers, "message-id:");
                           strcat(headers, message_id);
                           strcat(headers, "\n\n");
                           rc = stomp_tx(headers);
                           if(rc)
                           {
                              sprintf(zs,"Failed to transmit ACK message.  Error %d %s", rc, report_error(rc));
                              _log(CRITICAL, zs);
                              run_receive = false;
                           }
                           else
                           {
                              //_log(GENERAL, "Ack sent OK.");
                           }
                           //sprintf(zs, "%d messages, total size %ld bytes.", count, size);
                           //_log(GENERAL, zs);
                        }
                     }
                  } // while(run && run_receive)
               }
            }
         }
      }
      strcpy(headers, "DISCONNECT\n\n");
      rc = stomp_tx(headers);
      if(rc)
      {
         sprintf(zs, "Failed to send DISCONNECT:  Error %d %s", rc, report_error(rc));
         _log(GENERAL, zs);
      }
      else _log(GENERAL, "Sent DISCONNECT.");
      
      _log(GENERAL, "Disconnecting socket ...");
      rc = stomp_disconnect(); 
      if(rc)
      {
         sprintf(zs, "Failed to disconnect:  Error %d %s", rc, report_error(rc));
         _log(GENERAL, zs);
      }
      else _log(GENERAL, "Disconnected.");

      {      
         word i;
         if(holdoff < 128) holdoff += 32;
         else holdoff = 128;
         sprintf(zs, "Retry holdoff %d seconds.", holdoff);
         if(run) _log(GENERAL, zs);
         for(i = 0; i < holdoff && run; i++) sleep(1);
      }
   }  // while(run)

   db_disconnect();

   if(interrupt)
   {
      _log(CRITICAL, "Terminating due to interrupt.");
   }
   report_stats();
}

static void process_message(const char * body)
{
   jsmn_parser parser;
   time_t elapsed = time(NULL);
   
   log_message(body);

   jsmn_init(&parser);
   int r = jsmn_parse(&parser, body, tokens, NUM_TOKENS);
   if(r != 0) 
   {
   sprintf(zs, "Parser result %d.  Message discarded.", r);
      _log(MAJOR, zs);
      stats[NotVSTP]++;
   }
   else
   {
      // Good message
      char message_name[128];
      jsmn_extract_token(body, tokens, 1, message_name, sizeof(message_name));
      if(!strcmp(message_name, "VSTPCIFMsgV1")) 
      {
         process_vstp(body, tokens);
      }
      else
      {
         sprintf(zs, "Unrecognised message name \"%s\".", message_name);
         _log(MINOR, zs);
         jsmn_dump_tokens(body, tokens, 0);
         stats[NotVSTP]++;
      }
   }
   elapsed = time(NULL) - elapsed;
   if(elapsed > 1 || debug)
   {
      char zs[128];
      sprintf(zs, "Transaction took %ld seconds.", elapsed);
      _log(MINOR, zs);
   }
}

static void process_vstp(const char * string, const jsmntok_t * tokens)
{
   char zs[128], zs1[1024];

   stats[GoodMessage]++;

   jsmn_find_extract_token(string, tokens, 0, "transaction_type", zs, sizeof(zs));
   if(zs[0])
   {
      // printf("   Transaction type: \"%s\"", zs);
      if(!strcasecmp(zs, "Delete")) process_delete_schedule(string, tokens);
      else if(!strcasecmp(zs, "Create")) process_create_schedule(string, tokens, false);
      else if(!strcasecmp(zs, "Update")) process_update_schedule(string, tokens);
      else 
      {
         sprintf(zs1, "process_schedule():  Unrecognised transaction type \"%s\".", zs);
         _log(MAJOR, zs1);
         jsmn_dump_tokens(body, tokens, 0);
         stats[NotTransaction]++;
      }
   }
   else
   {
      _log(MAJOR, "process_schedule():  Failed to determine transaction type.");
      jsmn_dump_tokens(body, tokens, 0);
      stats[NotTransaction]++;
   }
}

#define EXTRACT(a,b) jsmn_find_extract_token(string, tokens, 0, a, b, sizeof( b ))
#define EXTRACT_OBJECT(a,b) jsmn_find_extract_token(string, tokens, index, a, b, sizeof( b ))
#define EXTRACT_APPEND_SQL(a) { jsmn_find_extract_token(string, tokens, 0, a, zs, sizeof( zs )); sprintf(zs1, ", \"%s\"", zs); strcat(query, zs1); }
#define EXTRACT_APPEND_SQL_OBJECT(a) { jsmn_find_extract_token(string, tokens, index, a, zs, sizeof( zs )); sprintf(zs1, ", \"%s\"", zs); strcat(query, zs1); }

static void process_delete_schedule(const char * string, const jsmntok_t * tokens)
{
   char query[1024], CIF_train_uid[16], schedule_start_date[16], schedule_end_date[16], CIF_stp_indicator[8]; 
   dword id;
   word update_id;
   MYSQL_RES * result0;
   MYSQL_ROW row0;

   word deleted = 0;

   EXTRACT("CIF_train_uid", CIF_train_uid);
   EXTRACT("schedule_start_date", schedule_start_date);
   EXTRACT("schedule_end_date", schedule_end_date);
   EXTRACT("CIF_stp_indicator", CIF_stp_indicator);

   time_t schedule_start_date_stamp = parse_datestamp(schedule_start_date);
   time_t schedule_end_date_stamp   = parse_datestamp(schedule_end_date);

   // Find the id
   sprintf(query, "SELECT id, update_id FROM cif_schedules where CIF_train_uid = '%s' and schedule_start_date = '%ld' and schedule_end_date = %ld and CIF_stp_indicator = '%s' AND update_id = 0 AND deleted > %ld",
           CIF_train_uid, schedule_start_date_stamp, schedule_end_date_stamp, CIF_stp_indicator, time(NULL));

   // DO WE NEED DAYS RUNS AS WELL????
   // Note:  Only find VSTP ones.

   if (db_query(query))
   {
      db_disconnect();
      return;
   }

   result0 = db_store_result();
   word num_rows = mysql_num_rows(result0);

   if(num_rows > 1)
   {
      char zs[256];
      sprintf(zs, "Delete schedule found %d matches.", num_rows);
      _log(MAJOR, zs);
      jsmn_dump_tokens(string, tokens, 0);
      stats[DeleteMulti]++;
      // Bodge!
      query[7] = '*'; query[8] = ' ';
      dump_mysql_result_query(query);
   }
 
   while((row0 = mysql_fetch_row(result0)) && row0[0]) 
   {
      id = atol(row0[0]);
      update_id = atoi(row0[1]);

      //sprintf(query, "DELETE FROM cif_schedule_locations WHERE cif_schedule_id = %ld", id);
      //if(!db_query(query))
      //{
      //}

      sprintf(query, "UPDATE cif_schedules SET deleted = %ld where id = %ld", time(NULL), id);
   
      if(!db_query(query))
      {
         char zs[256];
         deleted++;
         if(update_id)
         {
            // Can never happen!
            sprintf(zs, "Deleted non-VSTP schedule %ld.", id);
            _log(MINOR, zs);
         }
         else
         {
            sprintf(zs, "Deleted VSTP schedule %ld \"%s\".", id, CIF_train_uid);
            _log(GENERAL, zs);
         }
      }
   }
   mysql_free_result(result0);

   if(deleted) 
   {
      stats[DeleteHit]++;
   }
   else
   {
      _log(MAJOR, "Delete schedule miss.");
      jsmn_dump_tokens(string, tokens, 0);
      stats[DeleteMiss]++;
   }
}

static void process_create_schedule(const char * string, const jsmntok_t * tokens, const word update)
{
   // update true indicates this is as the result of a VSTP update.
   char zs[128], zs1[128];
   char query[2048];
   word i;

   if(debug) jsmn_dump_tokens(string, tokens, 0);

   time_t now = time(NULL);
   sprintf(query, "INSERT INTO cif_schedules VALUES(0, %ld, %lu", now, NOT_DELETED); // update_id == 0 => VSTP

   EXTRACT_APPEND_SQL("CIF_bank_holiday_running");
   EXTRACT_APPEND_SQL("CIF_stp_indicator");
   EXTRACT_APPEND_SQL("CIF_train_uid");

   EXTRACT_APPEND_SQL("applicable_timetable");

   EXTRACT_APPEND_SQL("atoc_code");
   // EXTRACT_APPEND_SQL("traction_class");
   EXTRACT_APPEND_SQL("uic_code");
   EXTRACT("schedule_days_runs", zs);
   for(i=0; i<7; i++)
   {
      strcat(query, ", ");
      strcat(query, (zs[i]=='1')?"1":"0");
   }

   EXTRACT("schedule_end_date", zs);
   time_t z = parse_datestamp(zs);
   sprintf(zs1, ", %ld", z);
   strcat(query, zs1);

   EXTRACT_APPEND_SQL("signalling_id");
   EXTRACT_APPEND_SQL("CIF_train_category");
   EXTRACT_APPEND_SQL("CIF_headcode");
   //EXTRACT_APPEND_SQL("CIF_course_indicator");
   EXTRACT_APPEND_SQL("CIF_train_service_code");
   EXTRACT_APPEND_SQL("CIF_business_sector");
   EXTRACT_APPEND_SQL("CIF_power_type");
   EXTRACT_APPEND_SQL("CIF_timing_load");
   EXTRACT_APPEND_SQL("CIF_speed");
   EXTRACT_APPEND_SQL("CIF_operating_characteristics");
   EXTRACT_APPEND_SQL("CIF_train_class");

   EXTRACT_APPEND_SQL("CIF_sleepers");

   EXTRACT_APPEND_SQL("CIF_reservations");
   EXTRACT_APPEND_SQL("CIF_connection_indicator");
   EXTRACT_APPEND_SQL("CIF_catering_code");
   EXTRACT_APPEND_SQL("CIF_service_branding");
   
   EXTRACT("schedule_start_date", zs);
   z = parse_datestamp(zs);
   sprintf(zs1, ", %ld", z);
   strcat(query, zs1);

   EXTRACT_APPEND_SQL("train_status");

   strcat(query, ", 0)"); // id filled by MySQL

   if(!db_query(query))
   {
      stats[update?UpdateCreate:Create]++;
   }
      
   dword id = db_insert_id();

   word index = jsmn_find_name_token(string, tokens, 0, "schedule_location");
   word locations = tokens[index+1].size;

   huyton_flag = false;
   index += 2;
   for(i = 0; i < locations; i++)
   {
      index = process_create_schedule_location(string, tokens, index, id);
   }

   if(huyton_flag) 
   {
      sprintf(zs, "Created schedule %ld%s.  +++ Passes Huyton +++", id, update?" as a result of an Update transaction":"");
      _log(GENERAL, zs);
   }

   if(huyton_flag)
   {
      char title[64], message[256];
      MYSQL_RES * result0;
      MYSQL_ROW row0;
      sprintf(title, "Huyton Schedule Created.");
      sprintf(message, "Created schedule which passes Huyton.");
      if(update) strcat(message, "  Due to a VSTP Update transaction.");
      strcat(message, "\n\n");
      EXTRACT("CIF_train_uid", zs1);
      sprintf(zs, "%ld (%s) ", id, zs1);
      EXTRACT("signalling_id", zs1);
      strcat(zs, zs1);
      sprintf(query, "SELECT tiploc_code, departure FROM cif_schedule_locations WHERE record_identity = 'LO' AND cif_schedule_id = %ld", id);
      if(!db_query(query))
      {
         result0 = db_store_result();
         if((row0 = mysql_fetch_row(result0)))
         {
            sprintf(zs1, " %s %s to ", show_time_text(row0[1]), tiploc_name(row0[0]));
            strcat(zs, zs1);
         }
         mysql_free_result(result0);
      }
      sprintf(query, "SELECT tiploc_code FROM cif_schedule_locations WHERE record_identity = 'LT' AND cif_schedule_id = %ld", id);
      if(!db_query(query))
      {
         result0 = db_store_result();
         if((row0 = mysql_fetch_row(result0)))
         {
            strcat (zs, tiploc_name(row0[0]));
         }
         mysql_free_result(result0);
      }

      strcat(message, zs);
      strcat(message, "\n\n");

      sprintf(query, "SELECT schedule_start_date, schedule_end_date FROM cif_schedules WHERE id = %ld", id);
      if(!db_query(query))
      {
         result0 = db_store_result();
         if((row0 = mysql_fetch_row(result0)))
         {
            dword from = atol(row0[0]);
            dword to   = atol(row0[1]);
            if(from == to)
            {
               strcat(message, "Service runs on ");
               strcat(message, date_text(from, true));
            }
            else
            {
               strcat(message, "Service runs from ");
               strcat(message, date_text(from, true));
               strcat(message, " to ");
               strcat(message, date_text(to,   true));
            }
            strcat(message, "\n\n");
         }
         mysql_free_result(result0);
      }
      email_alert(NAME, BUILD, title, message);
   }
}

static word process_create_schedule_location(const char * string, const jsmntok_t * tokens, const int index, const unsigned long schedule_id)
{
   char query[2048], zs[1024], zs1[1024];

   word sort_arrive, sort_depart, sort_pass, sort_time;
   static word origin_sort_time;

   byte type_LO;

   sprintf(zs, "process_create_schedule_location(%d, %ld)", index, schedule_id);
   _log(PROC, zs);

   sprintf(query, "INSERT INTO cif_schedule_locations VALUES(%ld, %ld",
           0L, schedule_id);

   EXTRACT_APPEND_SQL_OBJECT("CIF_activity");
   //EXTRACT_APPEND_SQL_OBJECT("record_identity");
   type_LO = false;
   if(strstr(zs, "TB"))
   {
      type_LO = true;
      strcat(query, ", 'LO'");
   }
   else if(strstr(zs, "TF"))
   {
      strcat(query, ", 'LT'");
   }
   else
   {
      strcat(query, ", 'LI'");
   }
   EXTRACT_APPEND_SQL_OBJECT("tiploc_id");
   if((!strcmp(zs, "HUYTON")) || (!strcmp(zs, "HUYTJUN")))
   {
      huyton_flag = true;
   }

   // EXTRACT_APPEND_SQL_OBJECT("tiploc_instance");// Missing from VSTPDB
   strcat(query, ", ''");

   // Times
   EXTRACT_OBJECT("scheduled_arrival_time", zs);
   sort_arrive = get_sort_time_vstp(zs);
   sprintf(zs1, ", '%s'", vstp_to_CIF_time(zs));
   strcat(query, zs1);
   EXTRACT_OBJECT("scheduled_departure_time", zs);
   sort_depart = get_sort_time_vstp(zs) + 1;
   sprintf(zs1, ", '%s'", vstp_to_CIF_time(zs));
   strcat(query, zs1);
   EXTRACT_OBJECT("scheduled_pass_time", zs);
   sort_pass = get_sort_time_vstp(zs) + 1;
   sprintf(zs1, ", '%s'", vstp_to_CIF_time(zs));
   strcat(query, zs1);
   EXTRACT_OBJECT("public_arrival_time", zs);
   sprintf(zs1, ", '%s'", vstp_to_CIF_time(zs));
   strcat(query, zs1);
   EXTRACT_OBJECT("public_departure_time", zs);
   sprintf(zs1, ", '%s'", vstp_to_CIF_time(zs));
   strcat(query, zs1);

   // Evaluate the sort_time and next_day fields
   if(sort_arrive < INVALID_SORT_TIME) sort_time = sort_arrive;
   else if(sort_depart < INVALID_SORT_TIME) sort_time = sort_depart;
   else sort_time = sort_pass;
   if(type_LO) origin_sort_time = sort_time;
   // N.B. Calculation of next_day field assumes that the LO record will be processed before the others.  Can we assume this?
   sprintf(zs, ", %d, %d", sort_time, (sort_time < origin_sort_time)?1:0);
   strcat(query, zs);
   EXTRACT_APPEND_SQL_OBJECT("CIF_platform");
   EXTRACT_APPEND_SQL_OBJECT("CIF_line");
   EXTRACT_APPEND_SQL_OBJECT("CIF_path");
   EXTRACT_APPEND_SQL_OBJECT("CIF_engineering_allowance");
   EXTRACT_APPEND_SQL_OBJECT("CIF_pathing_allowance");
   EXTRACT_APPEND_SQL_OBJECT("CIF_performance_allowance");
   strcat(query, ")");

   (void) db_query(query); 

   return (index + tokens[index].size + 5);
}

static void process_update_schedule(const char * string, const jsmntok_t * tokens)
{
   char query[1024], CIF_train_uid[16], schedule_start_date[16], schedule_end_date[16], CIF_stp_indicator[8]; 
   dword id;
   MYSQL_RES * result0;
   MYSQL_ROW row0;

   EXTRACT("CIF_train_uid", CIF_train_uid);
   EXTRACT("schedule_start_date", schedule_start_date);
   EXTRACT("schedule_end_date", schedule_end_date);
   EXTRACT("CIF_stp_indicator", CIF_stp_indicator);

   // time_t schedule_start_date_stamp = parse_datestamp(schedule_start_date);
   // time_t schedule_end_date_stamp   = parse_datestamp(schedule_end_date);

   sprintf(query, "SELECT id FROM cif_schedules WHERE update_id = 0 AND CIF_train_uid = '%s' AND CIF_stp_indicator = '%s' AND deleted > %ld", CIF_train_uid, CIF_stp_indicator, time(NULL));
   if(!db_query(query))
   {
      result0 = db_store_result();
      word num_rows = mysql_num_rows(result0);
      if(num_rows != 1)
      {
         sprintf(zs, "Update for schedule \"%s\" found %d existing records.  Delete phase skipped.", CIF_train_uid, num_rows);
         _log(MAJOR, zs);
         jsmn_dump_tokens(string, tokens, 0);
         if(num_rows) stats[UpdateDeleteMulti]++; else stats[UpdateDeleteMiss]++;
      }
      else
      {
         row0 = mysql_fetch_row(result0);
         id = atol(row0[0]);
         //sprintf(query, "DELETE FROM cif_schedule_locations WHERE cif_schedule_id = %ld", id);
         //db_query(query);
         sprintf(query, "UPDATE cif_schedules SET deleted = %ld WHERE id = %ld", time(NULL), id);
         db_query(query);
      }
      mysql_free_result(result0);
   }

   // Create phase.
   process_create_schedule(string, tokens, true);

   sprintf(zs, "Updated schedule \"%s\".", CIF_train_uid);
   _log(GENERAL, zs);

}

static void jsmn_dump_tokens(const char const * string, const jsmntok_t const * tokens, const word object_index)
{
   char zs[256], zs1[256];

   word i;
   for(i = object_index + 1; tokens[i].start >= 0 && tokens[i].start < tokens[object_index].end; i++)
   {
      sprintf(zs, "Token %2d: Type=", i);
      switch(tokens[i].type)
      {
      case JSMN_PRIMITIVE: strcat(zs, "Primitive"); break;
      case JSMN_OBJECT:    strcat(zs, "Object   "); break;
      case JSMN_ARRAY:     strcat(zs, "Array    "); break;
      case JSMN_STRING:    strcat(zs, "String   "); break;
      case JSMN_NAME:      strcat(zs, "Name     "); break;
      }
      sprintf(zs1, " Start=%d End=%d", tokens[i].start, tokens[i].end);
      strcat(zs, zs1);
      if(tokens[i].type == JSMN_OBJECT || tokens[i].type == JSMN_ARRAY)
      {
         sprintf(zs1, " Size=%d", tokens[i].size);
         strcat(zs, zs1);
      }
      if(tokens[i].type == JSMN_STRING || tokens[i].type == JSMN_NAME || tokens[i].type == JSMN_PRIMITIVE)
      {
         jsmn_extract_token(string, tokens, i, zs1, sizeof(zs1));
         strcat(zs, " \"");
         strcat(zs, zs1);
         strcat(zs, "\"");
      }
      _log(MINOR, zs);
   }

   return;
}

static void dump_headers(void)
{
   char zs[256];
   size_t i,j;

   i=0;
   j=0;
   while(headers[i])
   {
      if(headers[i] == '\n')
      {
         zs[j] = '\0';
         _log(MINOR, zs);
         j = 0;
         i++;
      }
      else
      {
         zs[j++] = headers[i++];
      }
   }
}

static char * report_error(int error)
{
   static char stomp_error[256];
   if(error < 0)
   {
      switch(error)
      {
      case  -2: sprintf(stomp_error, "Stomp error:  Unknown host.");         break;
      case  -6: sprintf(stomp_error, "Stomp error:  Failed setsockopt.");    break;
      case  -7: sprintf(stomp_error, "Stomp error:  End of file.");          break;
      case -11: sprintf(stomp_error, "Stomp error:  No socket connection."); break;
      default:  sprintf(stomp_error, "Stomp error %d", error);               break;
      }
      return stomp_error;
   }
   return strerror(error);
}

static void report_stats(void)
{
   char zs[128];
   word i;
   char report[2048];

   _log(GENERAL, "");
   sprintf(zs, "%25s: %-12s Total", "", "Day");
   _log(GENERAL, zs);
   strcpy(report, zs);
   strcat(report, "\n");

   sprintf(zs, "%25s: %-12s %ld days", "Run time", "", (time(NULL)-start_time)/(24*60*60));
   _log(GENERAL, zs);
   strcat(report, zs);
   strcat(report, "\n");
   for(i=0; i<MAXstats; i++)
   {
      grand_stats[i] += stats[i];
      sprintf(zs, "%25s: %-12s ", stats_category[i], commas_q(stats[i]));
      strcat(zs, commas_q(grand_stats[i]));
      _log(GENERAL, zs);
      strcat(report, zs);
      strcat(report, "\n");
      stats[i] = 0;
   }
   email_alert(NAME, BUILD, "Statistics Report", report);
}

static word get_sort_time_vstp(const char const * buffer)
{
   word result;
   char zs[8];
   if(strlen(buffer) < 4 || buffer[0] < '0' || buffer[0] > '9') return INVALID_SORT_TIME;

   zs[0]=buffer[0];
   zs[1]=buffer[1];
   zs[2]='\0';
   result = atoi(zs)*4*60;
   zs[0]=buffer[2];
   zs[1]=buffer[3];
   result += 4*atoi(zs);
   if(buffer[4] == 'H' || buffer[4] == '3') result += 2;

   return result;
}

static char * vstp_to_CIF_time(const char * buffer)
{

   static char cif[8];
   if(strlen(buffer) < 4)
   {
      // ????? Seem to get times like "00" which ought to be ""
      strcpy(cif, "");
   }
   else 
   {
      strncpy(cif, buffer, sizeof(cif));
      if(buffer[4] == '3')
      {
         cif[4] = 'H';
         cif[5] = '\0';
      }
      else
      {
         cif[4] = '\0';
      }
   }

   {
      char zs[256];
      sprintf(zs, "vstp_to_CIF_time(\"%s\") returns \"%s\"", buffer, cif);
      _log(PROC, zs);
   }

   return cif;
}

static void log_message(const char * message)
{
   FILE * fp;
   char filename[128];
   
   time_t now = time(NULL);
   struct tm * broken = gmtime(&now);
   
   sprintf(filename, "/tmp/vstpdb-messages-%04d-%02d-%02d.log", broken->tm_year + 1900, broken->tm_mon + 1, broken->tm_mday);
   
   if((fp = fopen(filename, "a")))
   {
      fprintf(fp, "%s\n", message);
      fclose(fp);
   }
}
static char * tiploc_name(const char * const tiploc)
{
   // Not re-entrant
   char query[256];
   MYSQL_RES * result0;
   MYSQL_ROW row0;
   static char result[128];

   sprintf(query, "select fn from corpus where tiploc = '%s'", tiploc);
   db_query(query);
   result0 = db_store_result();
   if((row0 = mysql_fetch_row(result0)) && row0[0][0]) 
   {
      strncpy(result, row0[0], 127);
      result[127] = '\0';
   }
   else
   {
      strcpy(result, tiploc);
   }
   return result;
}

