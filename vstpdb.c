/*
    Copyright (C) 2013, 2014, 2015, 2016, 2017 Phil Wieland

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
#include <sys/socket.h>
#include <netinet/in.h>
#include <errno.h>
#include <netdb.h>

#include "jsmn.h"
#include "misc.h"
#include "db.h"
#include "database.h"
#include "build.h"

#define NAME  "vstpdb"

#ifndef RELEASE_BUILD
#define BUILD "Y305p"
#else
#define BUILD RELEASE_BUILD
#endif

static void perform(void);
static void process_frame(const char * body);
static void process_vstp(const char * string, const jsmntok_t * tokens);
static void process_delete_schedule(const char * string, const jsmntok_t * tokens);
static void process_create_schedule(const char * string, const jsmntok_t * tokens, const word update);
static word process_create_schedule_location(const char * string, const jsmntok_t * tokens, const int index, const unsigned long schedule_id);
static void process_update_schedule(const char * string, const jsmntok_t * tokens);

static void jsmn_dump_tokens(const char const * string, const jsmntok_t const * tokens, const word object_index);
static void report_stats(void);
#define INVALID_SORT_TIME 9999
static word get_sort_time_vstp(const char const * buffer);
static char * vstp_to_CIF_time(const char * buffer);
static char * tiploc_name(const char * const tiploc);

static word debug, run, interrupt, holdoff, huyton_flag;

#define FRAME_SIZE 64000
static char body[FRAME_SIZE];

#define NUM_TOKENS 8192
static jsmntok_t tokens[NUM_TOKENS];

#define NOT_DELETED 0xffffffffL

// stompy port for vstp stream
#define STOMPY_PORT 55840

// Time in hours (local) when daily statistical report is produced.
// (Set > 23 to disable daily report.)
#define REPORT_HOUR 4
#define REPORT_MINUTE 1

// Stats
static time_t start_time;
enum stats_categories {ConnectAttempt, GoodMessage, DeleteHit, DeleteMiss, DeleteMulti, Create, 
                       UpdateCreate, UpdateDeleteMiss, UpdateDeleteMulti, HeadcodeDeduced,
                       NotMessage, NotVSTP, NotTransaction, MAXstats};
static qword stats[MAXstats];
static qword grand_stats[MAXstats];
static const char * stats_category[MAXstats] = 
   {
      "Stompy Connect Attempt", "Good Message", "Delete Hit", "Delete Miss", "Delete Multiple Hit", "Create",
      "Update", "Update Delete Miss", "Update Delete Mult. Hit", "Deduced Schedule Headcode",
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
   char config_file_path[256];
   word usage = false;
   while ((c = getopt (argc, argv, ":c:")) != -1)
   {
      switch (c)
      {
      case 'c':
         strcpy(config_file_path, optarg);
         break;
      case ':':
         break;
      case '?':
      default:
         usage = true;
         break;
      }
   }

   char * config_fail;
   if((config_fail = load_config(config_file_path)))
   {
      printf("Failed to read config file \"%s\":  %s\n", config_file_path, config_fail);
      usage = true;
   }

   debug = *conf[conf_debug];

   if(usage)
   {
      printf("\tUsage: %s [-c /path/to/config/file.conf]\n\n", argv[0] );
      exit(1);
   }

   int lfp = 0;

   // Set up log
   _log_init(debug?"/tmp/vstpdb.log":"/var/log/garner/vstpdb.log", debug?1:0);

   // Enable core dumps
   struct rlimit limit;
   if(!getrlimit(RLIMIT_CORE, &limit))
   {
      limit.rlim_cur = RLIM_INFINITY;
      setrlimit(RLIMIT_CORE, &limit);
   }

   start_time = time(NULL);

   // DAEMONISE
   if(!debug)
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
      _log(GENERAL, "%s %s", NAME, BUILD);
      _log(GENERAL, "Running as daemon.");
   }
   else
   {
      _log(GENERAL, "");
      _log(GENERAL, "%s %s", NAME, BUILD);
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

   {
      // Sort out the signal handlers
      struct sigaction act;
      sigset_t block_mask;
     
      sigemptyset(&block_mask);
      act.sa_handler = termination_handler;
      act.sa_mask = block_mask;
      act.sa_flags = 0;
      if(sigaction(SIGTERM, &act, NULL) || sigaction(SIGINT, &act, NULL))
      {
         _log(CRITICAL, "Failed to set up signal handler.");
         exit(1);
      }
   }

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
   {
      struct sysinfo info;
      word logged = false;
      word i;
      while(run && !debug && !sysinfo(&info) && info.uptime < (512 + 128))
      {
         if(!logged) _log(GENERAL, "Startup delay...");
         logged = true;
         for(i = 0; i < 8 && run; i++) sleep(1);
      }
   }

   if(run) perform();

   if(lfp) close(lfp);

   return 0;
}

static void perform(void)
{
   word last_report_day;
   // Initialise database
   {
      db_init(conf[conf_db_server], conf[conf_db_user], conf[conf_db_password], conf[conf_db_name]);

      word e;
      if((e=database_upgrade(vstpdb)))
      {
         _log(CRITICAL, "Error %d in upgrade_database().  Aborting.", e);
         exit(1);
      }
   }

   {
      time_t now = time(NULL);
      struct tm * broken = localtime(&now);
      last_report_day = broken->tm_wday;
   }
   while(run)
   {   
      stats[ConnectAttempt]++;
      int run_receive = !open_stompy(STOMPY_PORT);
      while(run_receive && run)
      {
         holdoff = 0;
         {
            time_t now = time(NULL);
            struct tm * broken = localtime(&now);
            if(broken->tm_wday != last_report_day && broken->tm_hour >= REPORT_HOUR && broken->tm_min >= REPORT_MINUTE)
            {
               last_report_day = broken->tm_wday;
               report_stats();
            }
         }

         word r = read_stompy(body, FRAME_SIZE, 64);
         _log(DEBUG, "read_stompy() returned %d.", r);
         if(!r && run && run_receive)
         {
            if(db_start_transaction())
            {
               run_receive = false;
            }
            if(run_receive) process_frame(body);

            if(!db_errored)
            {
               if(db_commit_transaction())
               {
                  db_rollback_transaction();
                  run_receive = false;
               }
               else
               {
                  // Send ACK
                  if(ack_stompy())
                  {
                     _log(CRITICAL, "Failed to write message ack.  Error %d %s", errno, strerror(errno));
                     run_receive = false;
                  }
               }
            }
            else
            {
               // DB error occurred during processing of frame.
               db_rollback_transaction();
               run_receive = false;
            }
         }
         else if(run && run_receive)
         {
            if(r != 3)
            {
               run_receive = false;
               _log(CRITICAL, "Receive error %d on stompy connection.", r);
            }
            else
            {
               // Don't report these because it is normal on VSTP stream
               // _log(MINOR, "Receive timeout on stompy connection."); 
            }
         }
      } // while(run_receive && run)
      close_stompy();
      {      
         word i;
         if(holdoff < 256) holdoff += 38;
         else holdoff = 256;
         for(i = 0; i < holdoff + 64 && run; i++) sleep(1);
      }
   }  // while(run)

   if(interrupt)
   {
      _log(CRITICAL, "Terminating due to interrupt.");
   }

   db_disconnect();
   report_stats();
}

static void process_frame(const char * body)
{
   jsmn_parser parser;
   time_t elapsed = time(NULL);
   
   jsmn_init(&parser);
   int r = jsmn_parse(&parser, body, tokens, NUM_TOKENS);
   if(r != 0) 
   {
      _log(MAJOR, "Parser result %d.  Message discarded.", r);
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
         _log(MINOR, "Unrecognised message name \"%s\".", message_name);
         jsmn_dump_tokens(body, tokens, 0);
         stats[NotVSTP]++;
      }
   }
   elapsed = time(NULL) - elapsed;
   if(elapsed > 1 || debug)
   {
      _log(MINOR, "Transaction took %ld seconds.", elapsed);
   }
}

static void process_vstp(const char * string, const jsmntok_t * tokens)
{
   char zs[128];

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
         _log(MAJOR, "process_schedule():  Unrecognised transaction type \"%s\".", zs);
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

      sprintf(query, "UPDATE cif_schedules SET deleted = %ld where id = %u", time(NULL), id);
      if(!db_query(query))
      {
         deleted++;
         if(update_id)
         {
            // Can never happen!
            _log(MAJOR, "Deleted non-VSTP schedule %u.", id);
         }
         else
         {
            _log(DEBUG, "Deleted VSTP schedule %u \"%s\".", id, CIF_train_uid);
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
   char zs[1024], zs1[1024];
   char query[2048];
   word i;
   char uid[16], stp_indicator[2];
   char signalling_id[8];

   if(debug) jsmn_dump_tokens(string, tokens, 0);

   time_t now = time(NULL);
   sprintf(query, "INSERT INTO cif_schedules VALUES(0, %ld, %lu", now, NOT_DELETED); // update_id == 0 => VSTP

   EXTRACT_APPEND_SQL("CIF_bank_holiday_running");

   //EXTRACT_APPEND_SQL("CIF_stp_indicator");
   EXTRACT("CIF_stp_indicator", stp_indicator);
   sprintf(zs1, ", '%s'", stp_indicator); strcat(query, zs1); 

   //EXTRACT_APPEND_SQL("CIF_train_uid");
   EXTRACT("CIF_train_uid", uid);
   sprintf(zs1, ", '%s'", uid); strcat(query, zs1); 

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

   EXTRACT("signalling_id", signalling_id);
   sprintf(zs1, ", '%s'", signalling_id); strcat(query, zs1);

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

   strcat(query, ", 0, '', '')"); // id filled by MySQL

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

   if(stp_indicator[0] == 'O' && (signalling_id[0] == '\0' || signalling_id[0] == ' '))
   {
      // Search db for schedules with a deduced headcode, and add it to this one, status = D
      // Bug:  Really this should also look for schedules with a signalling_id
      MYSQL_RES * result;
      MYSQL_ROW row;
      sprintf(query, "SELECT deduced_headcode FROM cif_schedules WHERE CIF_train_uid = '%s' AND deduced_headcode != '' AND schedule_end_date > %ld ORDER BY created DESC", uid, now - (64L * 24L * 60L * 60L));
      if(!db_query(query))
      {
         result = db_store_result();
         if((row = mysql_fetch_row(result)))
         {
            sprintf(query, "UPDATE cif_schedules SET deduced_headcode = '%s', deduced_headcode_status = 'D' WHERE id = %u", row[0], id);
            db_query(query);
            _log(DEBUG, "Deduced headcode \"%s\" applied to overlay schedule %u, uid \"%s\".", row[0], id, uid);
            stats[HeadcodeDeduced]++;
         }
         else
         {
            _log(DEBUG, "Deduced headcode not found for overlay schedule %u, uid \"%s\".", id, uid);
         }
         mysql_free_result(result);
      }
   }

   sprintf(query, "UPDATE status SET last_vstp_processed = %ld", now);
   db_query(query);

   if(huyton_flag) 
   {
      _log(DEBUG, "Created schedule %u%s.  +++ Passes Huyton +++", id, update?" as a result of an Update transaction":"");
   }

   if(huyton_flag)
   {
      char title[64], message[512];
      MYSQL_RES * result0;
      MYSQL_ROW row0;
      char stp[4];
      sprintf(title, "Huyton Schedule Created.");
      sprintf(message, "Created schedule which passes Huyton.");
      if(update) strcat(message, "  Due to a VSTP Update transaction.");
      strcat(message, "\n\n");
      EXTRACT("CIF_train_uid", zs1);
      EXTRACT("CIF_stp_indicator", stp);
      sprintf(zs, "%u (%s %s) ", id, zs1, stp);
      EXTRACT("signalling_id", zs1);
      strcat(zs, zs1);
      sprintf(query, "SELECT tiploc_code, departure FROM cif_schedule_locations WHERE record_identity = 'LO' AND cif_schedule_id = %u", id);
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
      sprintf(query, "SELECT tiploc_code FROM cif_schedule_locations WHERE record_identity = 'LT' AND cif_schedule_id = %u", id);
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

      sprintf(query, "SELECT schedule_start_date, schedule_end_date, CIF_stp_indicator FROM cif_schedules WHERE id = %u", id);
      if(!db_query(query))
      {
         result0 = db_store_result();
         if((row0 = mysql_fetch_row(result0)))
         {
            dword from = atol(row0[0]);
            dword to   = atol(row0[1]);
            if(from == to)
            {
               strcat(message, "  Runs on ");
               strcat(message, date_text(from, true));
            }
            else
            {
               strcat(message, "  Runs from ");
               strcat(message, date_text(from, true));
               strcat(message, " to ");
               strcat(message, date_text(to,   true));
            }
            if(row0[2][0] == 'C') strcat(message, "  CANCELLED");
            strcat(message, "\n");
         }
         mysql_free_result(result0);
      }
      sprintf(query, "SELECT departure, arrival, pass, tiploc_code FROM cif_schedule_locations WHERE (tiploc_code = 'HUYTON' OR tiploc_code = 'HUYTJUN') AND cif_schedule_id = %u", id);
      if(!db_query(query))
      {
         result0 = db_store_result();
         while((row0 = mysql_fetch_row(result0)))
         {
            char where[32], z[128];
            if(row0[3][4] == 'J') strcpy(where, "Huyton Junction"); else strcpy(where, "Huyton"); 
            if(row0[0][0]) 
            {
               sprintf(z, "Depart %s at %s.\n", where, row0[0]);
               strcat(message, z);
            }
            else if(row0[1][0])
            {
               sprintf(z, "Arrive %s at %s.\n", where, row0[1]); 
               strcat(message, z);
            }
            else if(row0[2][0])
            {
               sprintf(z, "Pass %s at %s.\n", where, row0[2]);
               strcat(message, z);
            }
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
   if(*conf[conf_huyton_alerts] && ((!strcmp(zs, "HUYTON")) || (!strcmp(zs, "HUYTJUN"))))
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

   time_t schedule_start_date_stamp = parse_datestamp(schedule_start_date);
   // time_t schedule_end_date_stamp   = parse_datestamp(schedule_end_date);

   sprintf(query, "SELECT id FROM cif_schedules WHERE update_id = 0 AND CIF_train_uid = '%s' AND CIF_stp_indicator = '%s' AND schedule_start_date = %ld AND deleted > %ld", CIF_train_uid, CIF_stp_indicator, schedule_start_date_stamp, time(NULL));
   if(!db_query(query))
   {
      result0 = db_store_result();
      word num_rows = mysql_num_rows(result0);
      if(num_rows != 1)
      {
         _log(MAJOR, "Update for schedule \"%s\" found %d existing records.  Delete phase skipped.", CIF_train_uid, num_rows);
         jsmn_dump_tokens(string, tokens, 0);
         if(num_rows) stats[UpdateDeleteMulti]++; else stats[UpdateDeleteMiss]++;
      }
      else
      {
         row0 = mysql_fetch_row(result0);
         id = atol(row0[0]);
         sprintf(query, "UPDATE cif_schedules SET deleted = %ld WHERE id = %u", time(NULL), id);
         db_query(query);
      }
      mysql_free_result(result0);
   }

   // Create phase.
   process_create_schedule(string, tokens, true);

   _log(DEBUG, "Updated schedule \"%s\".", CIF_train_uid);
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
      _log(GENERAL, zs);
   }

   return;
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

   _log(PROC, "vstp_to_CIF_time(\"%s\") returns \"%s\"", buffer, cif);

   return cif;
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
   mysql_free_result(result0);

   return result;
}

