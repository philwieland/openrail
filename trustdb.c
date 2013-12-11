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
#include "private.h"

#define NAME  "trustdb"
#define BUILD "UC09"

static void perform(void);
static void process_message(const char * const body);
static void process_trust_0001(const char * const string, const jsmntok_t * const tokens, const int index);
static void process_trust_0002(const char * const string, const jsmntok_t * const tokens, const int index);
static void process_trust_0003(const char * const string, const jsmntok_t * const tokens, const int index);
static void process_trust_0005(const char * const string, const jsmntok_t * const tokens, const int index);
static void process_trust_0006(const char * const string, const jsmntok_t * const tokens, const int index);
static void process_trust_0007(const char * const string, const jsmntok_t * const tokens, const int index);
static void create_database(void);

static void jsmn_dump_tokens(const char * const string, const jsmntok_t * const tokens, const word object_index);
static void dump_headers(void);
static char * report_error(const int error);
static void report_stats(void);
#define INVALID_SORT_TIME 9999
static void log_message(const char * const message);
static time_t correct_trust_timestamp(const time_t in);

static time_t last_idle;
static word debug, run, interrupt, holdoff, high_load;
static char zs[4096];

static char headers[1024], body[65536];
#define NUM_TOKENS 8192
static jsmntok_t tokens[NUM_TOKENS];

#define REPORT_INTERVAL (24*60*60)
static time_t start_time;

// Stats
enum stats_categories {Bytes, ConnectAttempt, GoodMessage, // Don't insert any here
                       Mess1, Mess2, Mess3, Mess4, Mess5, Mess6, Mess7, Mess8,
                       NotMessage, NotRecog, Mess1Miss, MovtNoAct, DeducedAct, MAXstats};
unsigned long long int stats[MAXstats];
unsigned long long grand_stats[MAXstats];
const char * stats_category[MAXstats] = 
   {
      "Bytes", "Connect Attempt", "Good Message", 
      "Message type 1","Message type 2","Message type 3","Message type 4","Message type 5","Message type 6","Message type 7","Message type 8",
      "Not a message", "Invalid or Not Recognised", "Mess 1 schedule not found", "Movement without act.", "Deduced activation",
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
   int lfp = 0;

   // Determine debug mode
   if(geteuid() == 0)
   {
      debug = false;
   }
   else
   {
      debug = true;
   }

   // Set up log
   _log_init(debug?"/tmp/trustdb.log":"/var/log/garner/trustdb.log", debug?1:0);

   // Enable core dumps
   struct rlimit limit;
   if(!getrlimit(RLIMIT_CORE, &limit))
   {
      limit.rlim_cur = RLIM_INFINITY;
      setrlimit(RLIMIT_CORE, &limit);
   }

   start_time  = time(NULL);

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
      
      if((lfp = open("/var/run/trustdb.pid", O_RDWR|O_CREAT, 0640)) < 0)
      {
         _log(CRITICAL, "Unable to open pid file \"/var/run/trustdb.pid\".  Aborting.");
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
   high_load = true;
   last_idle = 0;

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
      for(i=0; i < MAXstats; i++) stats[i] = 0;
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
   db_init(DB_SERVER, DB_USER, DB_PASSWORD, debug?"rail_test":"rail");

   log_message("");
   log_message("");
   log_message("trustdb started.");

   create_database();

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
            strcat(headers, NATIONAL_RAIL_USERNAME);  
            strcat(headers, "\npasscode:");
            strcat(headers, NATIONAL_RAIL_PASSWORD);
            strcat(headers, "\n");          
            if(debug)
            {
               sprintf(zs, "client-id:%s-trustdb-debug\n", NATIONAL_RAIL_USERNAME);
               strcat(headers, zs);
            }
            else
            {
               sprintf(zs, "client-id:%s-trustdb-%s\n", NATIONAL_RAIL_USERNAME, abbreviated_host_id());
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
                  strcat(headers, "destination:/topic/TRAIN_MVT_ALL_TOC\n");      
                  if(debug)
                  {
                     sprintf(zs, "activemq.subscriptionName:%s-trustdb-debug\n", NATIONAL_RAIL_USERNAME);
                     strcat(headers, zs);
                  }
                  else
                  {
                     sprintf(zs, "activemq.subscriptionName:%s-trustdb-%s\n", NATIONAL_RAIL_USERNAME, abbreviated_host_id());
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
                     time_t waited = time(NULL);
                     if( waited / REPORT_INTERVAL != last_report)
                     {
                        report_stats();
                        last_report = waited / REPORT_INTERVAL;
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
                        time_t now = time(NULL);
                        waited = now - waited;
                        if(waited > 1) 
                        {
                           last_idle = now;
                           if(high_load) _log(MINOR, "Ceasing task shedding.");
                           high_load = false;
                        }
                        else
                        {
                           if(!high_load && now - last_idle > 64)
                           {
                              // Enter high load
                              high_load = true;
                              _log(MINOR, "High load detected.  Shedding tasks.");
                           }
                        }
                              
                        if(debug || waited < 2)
                        {
                           sprintf(zs, "Message receive wait time was %ld seconds.", waited);
                           _log(MINOR, zs);
                        }
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
         if(holdoff < 128) holdoff += 16;
         else holdoff = 128;
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

static void process_message(const char * const body)
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
      stats[NotRecog]++;
   }
   else
   {
      size_t messages, i, index;
      // Is it an array?
      if(tokens[0].type == JSMN_ARRAY)
      {
         messages = tokens[0].size;
         index = 1;
         sprintf(zs, "STOMP message is array of %d TRUST messages.", messages);
      }
      else
      {
         messages = 1;
         index = 0;
         sprintf(zs, "STOMP message contains a single TRUST message.");
      }
      _log(DEBUG,zs);

      for(i=0; i < messages; i++)
      {
         char message_name[128];
         jsmn_find_extract_token(body, tokens, index, "msg_type", message_name, sizeof(message_name));
         word message_type = atoi(message_name);
         if(debug)
         {
            sprintf(zs, "Processing TRUST message %d", i);
            _log(DEBUG, zs);
         }
         if(!strncmp(message_name, "000", 3) && message_type > 0 && message_type < 9) 
         {
            stats[GoodMessage]++;
            stats[GoodMessage + message_type]++;
            switch(message_type)
            {
            case 1: process_trust_0001(body, tokens, index); break;
            case 2: process_trust_0002(body, tokens, index); break;
            case 3: process_trust_0003(body, tokens, index); break;
            case 5: process_trust_0005(body, tokens, index); break;
            case 6: process_trust_0006(body, tokens, index); break;
            case 7: process_trust_0007(body, tokens, index); break;
            default:
               sprintf(zs, "Message type \"%s\" discarded.", message_name);
               _log(MINOR, zs);
               break;
            }
         }
         else
         {
            sprintf(zs, "Unrecognised message type \"%s\".", message_name);
            _log(MINOR, zs);
            jsmn_dump_tokens(body, tokens, index);
            stats[NotRecog]++;
         }
         
         size_t message_ends = tokens[index].end;
         do  index++; 
         while ( tokens[index].start < message_ends && tokens[index].start >= 0 && index < NUM_TOKENS);
      }
   }
   elapsed = time(NULL) - elapsed;
   if(debug || elapsed > 1)
   {
      char zs[128];
      sprintf(zs, "Transaction took %ld seconds.", elapsed);
      _log(MINOR, zs);
   }
}

static void process_trust_0001(const char * const string, const jsmntok_t * const tokens, const int index)
{
   char zs[128], zs1[128], report[1024];
   char train_id[64], train_uid[64];
   char query[1024];
   dword cif_schedule_id;
   MYSQL_RES * result0;
   MYSQL_ROW row0;
   
   sprintf(report, "Activation message:");

   jsmn_find_extract_token(string, tokens, index, "train_id", train_id, sizeof(train_id));
   sprintf(zs1, " train_id=\"%s\"", train_id);
   strcat(report, zs1);

   jsmn_find_extract_token(string, tokens, index, "schedule_start_date", zs, sizeof(zs));
   time_t schedule_start_date_stamp = parse_datestamp(zs);
   sprintf(zs1, " schedule_start_date=\"%s\" %ld", zs, schedule_start_date_stamp);
   strcat(report, zs1);

   jsmn_find_extract_token(string, tokens, index, "schedule_end_date", zs, sizeof(zs));
   time_t schedule_end_date_stamp   = parse_datestamp(zs);
   sprintf(zs1, " schedule_end_date=\"%s\" %ld", zs, schedule_end_date_stamp);
   strcat(report, zs1);

   jsmn_find_extract_token(string, tokens, index, "train_uid", train_uid, sizeof(train_uid));
   sprintf(zs1, " train_uid=\"%s\"", train_uid);
   strcat(report, zs1);

   jsmn_find_extract_token(string, tokens, index, "schedule_source", zs, sizeof(zs));
   sprintf(zs1, " schedule_source=\"%s\"", zs);
   strcat(report, zs1);

   jsmn_find_extract_token(string, tokens, index, "schedule_wtt_id", zs, sizeof(zs));
   sprintf(zs1, " schedule_wtt_id=\"%s\"", zs);
   strcat(report, zs1);

   // Bodge.  The ORDER BY here will *usually* get the correct one out first!
   // Idea:  If we store and index on cif_train_uid, we can guarantee to get the right one.
   sprintf(query, "select id from cif_schedules where cif_train_uid = '%s' AND schedule_start_date = %ld AND schedule_end_date = %ld AND deleted > %ld ORDER BY LOCATE(CIF_stp_indicator, 'OCPN')", train_uid, schedule_start_date_stamp, schedule_end_date_stamp, time(NULL));
   if(!db_query(query))
   {
      result0 = db_store_result();
      word num_rows = mysql_num_rows(result0);
      sprintf(zs, "  Schedule hit count %d.  Message contents:", num_rows);
      strcat(report, zs);
      if(num_rows < 1) 
      {
         stats[Mess1Miss]++;
         _log(MINOR, report);
         time_t now = time(NULL);
         sprintf(query, "INSERT INTO trust_activation VALUES(%ld, '%s', %ld, 0)", now, train_id, 0L);
         db_query(query);
         jsmn_dump_tokens(string, tokens, index);
      }
      else
      {
         row0 = mysql_fetch_row(result0);
         cif_schedule_id = atol(row0[0]);
         time_t now = time(NULL);
         sprintf(query, "INSERT INTO trust_activation VALUES(%ld, '%s', %ld, 0)", now, train_id, cif_schedule_id);
         db_query(query);
      }
      mysql_free_result(result0);
   }
   return;
}

static void process_trust_0002(const char * string, const jsmntok_t * tokens, const int index)
{
   char query[1024];
   char train_id[128], reason[128], type[128], stanox[128];
   
   jsmn_find_extract_token(string, tokens, index, "train_id", train_id, sizeof(train_id));
   jsmn_find_extract_token(string, tokens, index, "canx_reason_code", reason, sizeof(reason));
   jsmn_find_extract_token(string, tokens, index, "canx_type", type, sizeof(type));
   jsmn_find_extract_token(string, tokens, index, "loc_stanox", stanox, sizeof(stanox));

   time_t now = time(NULL);
   sprintf(query, "INSERT INTO trust_cancellation VALUES(%ld, '%s', '%s', '%s', '%s', 0)", now, train_id, reason, type, stanox);
   db_query(query);
   
   return;
}

static void process_trust_0003(const char * string, const jsmntok_t * tokens, const int index)
{
   char query[1024], zs[32], zs1[32], train_id[128], loc_stanox[128];
   time_t planned_timestamp, timestamp;

   time_t now = time(NULL);
   
   sprintf(query, "INSERT INTO trust_movement VALUES(%ld, '", now);
   jsmn_find_extract_token(string, tokens, index, "train_id", train_id, sizeof(train_id));
   strcat(query, train_id);
   strcat(query, "', '");
   jsmn_find_extract_token(string, tokens, index, "event_type", zs, sizeof(zs));
   strcat(query, zs);
   strcat(query, "', '");
   jsmn_find_extract_token(string, tokens, index, "planned_event_type", zs, sizeof(zs));
   strcat(query, zs);
   strcat(query, "', '");
   jsmn_find_extract_token(string, tokens, index, "platform", zs, sizeof(zs));
   strcat(query, zs);
   strcat(query, "', '");
   jsmn_find_extract_token(string, tokens, index, "loc_stanox", loc_stanox, sizeof(loc_stanox));
   strcat(query, loc_stanox);
   strcat(query, "', ");
   jsmn_find_extract_token(string, tokens, index, "actual_timestamp", zs, sizeof(zs));
   zs[10] = '\0';
   timestamp = correct_trust_timestamp(atol(zs));
   sprintf(zs, "%ld", timestamp);
   strcat(query, zs);
   strcat(query, ", ");
   jsmn_find_extract_token(string, tokens, index, "gbtt_timestamp", zs, sizeof(zs));
   zs[10] = '\0';
   timestamp = correct_trust_timestamp(atol(zs));
   sprintf(zs, "%ld", timestamp);
   strcat(query, zs);
   strcat(query, ", ");
   jsmn_find_extract_token(string, tokens, index, "planned_timestamp", zs, sizeof(zs));
   zs[10] = '\0';
   planned_timestamp = correct_trust_timestamp(atol(zs));
   sprintf(zs, "%ld", planned_timestamp);
   strcat(query, zs);
   strcat(query, ", ");
   jsmn_find_extract_token(string, tokens, index, "timetable_variation", zs, sizeof(zs));
   strcat(query, zs);
   strcat(query, ", '");
   jsmn_find_extract_token(string, tokens, index, "event_source", zs, sizeof(zs));
   strcat(query, zs);
   strcat(query, "', ");
   jsmn_find_extract_token(string, tokens, index, "offroute_ind", zs, sizeof(zs));
   strcat(query, (zs[0] == 't')?"1":"0");
   strcat(query, ", ");
   jsmn_find_extract_token(string, tokens, index, "train_terminated", zs, sizeof(zs));
   strcat(query, (zs[0] == 't')?"1":"0");
   strcat(query, ", '");
   jsmn_find_extract_token(string, tokens, index, "variation_status", zs, sizeof(zs));
   strcat(query, zs);
   strcat(query, "', '");
   jsmn_find_extract_token(string, tokens, index, "next_report_stanox", zs, sizeof(zs));
   strcat(query, zs);
   strcat(query, "', ");
   jsmn_find_extract_token(string, tokens, index, "next_report_run_time", zs, sizeof(zs));
   sprintf(zs1, "%d", atoi(zs));
   strcat(query, zs1);
   strcat(query, ")");

   db_query(query);

   sprintf(query, "SELECT * from trust_activation where trust_id = '%s' and created > %ld and cif_schedule_id > 0", train_id, now - (4*24*60*60));
   if(!db_query(query))
   {
      MYSQL_RES * result0 = db_store_result();

      word num_rows = mysql_num_rows(result0);
      mysql_free_result(result0);
      if(num_rows > 1)
      {
         // This is not actually invalid, if there's some cancellations as well
         // sprintf(query, "Movement message received with %d matching activations, train_id = \"%s\".", num_rows, train_id);
         // _log(MAJOR, query);
      }
      else if(num_rows < 1)
      {
         MYSQL_ROW row0;
         char tiploc[128], reason[128];
         word sort_time;
         time_t now = time(NULL);

         strcpy(reason, "");
         tiploc[0] = '\0';

         sprintf(query, "Movement message received with %d matching activations, train_id = \"%s\".", num_rows, train_id);
         _log(MINOR, query);
         stats[MovtNoAct]++;
         if(planned_timestamp ==0)
         {
            strcpy(reason, "No planned timestamp");
         }
         if(high_load) strcpy(reason, "Message load too high");

         if(!reason[0])
         {
            sprintf(query, "SELECT tiploc FROM corpus WHERE stanox = %s AND tiploc != ''", loc_stanox);
            if(!db_query(query))
            {
               result0 = db_store_result();
               num_rows = mysql_num_rows(result0);
               if(num_rows)
               {
                  row0 = mysql_fetch_row(result0);
                  strcpy(tiploc, row0[0]);
               }
               else
               {
                  strcpy(reason, "Unable to determine TIPLOC");
               }
               mysql_free_result(result0);
            }
         }

         if(!reason[0])
         {
            char query1[256];
            struct tm * broken = localtime(&planned_timestamp);
            // Select the day
            word day = broken->tm_wday;
            word yest = (day + 6) % 7;
            // word tom = (day + 1) % 7;
            broken->tm_hour = 12;
            broken->tm_min = 0;
            broken->tm_sec = 0;
            time_t when = timegm(broken);
            sort_time = broken->tm_hour * 4 * 60 + broken->tm_min * 4;
            sprintf(query, "SELECT cif_schedules.id, cif_schedules.CIF_train_uid, signalling_id FROM cif_schedules INNER JOIN cif_schedule_locations ON cif_schedules.id = cif_schedule_locations.cif_schedule_id WHERE cif_schedule_locations.tiploc_code = '%s'",
                    tiploc);
            sprintf(query1, " AND cif_schedule_locations.sort_time > %d AND cif_schedule_locations.sort_time < %d",
                    sort_time - 1, sort_time + 4);
            strcat(query, query1);
            strcat(query, " AND (cif_schedules.CIF_stp_indicator = 'N' OR cif_schedules.CIF_stp_indicator = 'P' OR cif_schedules.CIF_stp_indicator = 'O')");


            static const char * days_runs[8] = {"runs_su", "runs_mo", "runs_tu", "runs_we", "runs_th", "runs_fr", "runs_sa", "runs_su"};

            //
            sprintf(query1, " AND (((%s) AND (schedule_start_date <= %ld) AND (schedule_end_date >= %ld) AND (NOT next_day))",   days_runs[day],  when + 12*60*60, when - 12*60*60);
            strcat(query, query1);
            sprintf(query1, " OR   ((%s) AND (schedule_start_date <= %ld) AND (schedule_end_date >= %ld) AND (    next_day)))",  days_runs[yest], when - 12*60*60, when - 36*60*60);
            strcat(query, query1);

            sprintf(query1, " AND deleted > %ld", now);
            strcat(query, query1);

            if(!db_query(query))
            {
               word count = 0;
               dword cif_schedule_id;
               result0 = db_store_result();
               num_rows = mysql_num_rows(result0);
               if(!num_rows)
               {
                  strcpy(reason, "No schedules found");
               }

               while((!reason[0]) && (row0 = mysql_fetch_row(result0)))
               {
                  sprintf(query, "   Found potential match: %s (%s) %s", row0[0], row0[1], row0[2]);
                  _log(MINOR, query);
                  count++;
                  cif_schedule_id = atol(row0[0]);

               }
               mysql_free_result(result0);

               if(!reason[0])
               {
                  if(count == 1)
                  {
                     sprintf(query, "INSERT INTO trust_activation VALUES(%ld, '%s', %ld, 1)", now, train_id, cif_schedule_id);
                     db_query(query);
                     _log(MINOR, "   Successfully deduced activation.");
                  }
                  else
                  {
                     strcpy(reason, "Multiple schedules found");
                  }
               }
            }
         }

         if(!reason[0])
         {
            stats[DeducedAct]++;
         }
         else
         {
            sprintf(query, "   Failed to deduce an activation - Reason:  %s.", reason );
            _log(MINOR, query);
            
            sprintf(query, "      stanox = %s, tiploc = \"%s\", planned_timestamp %s, derived sort time = %d", loc_stanox, tiploc, time_text(planned_timestamp, true), sort_time);
            _log(MINOR, query);
            // jsmn_dump_tokens(string, tokens, index);
         }
      }
   }
   return;
}

static void process_trust_0005(const char * const string, const jsmntok_t * const tokens, const int index)
{
   char query[1024];
   char train_id[128], stanox[128];
   
   jsmn_find_extract_token(string, tokens, index, "train_id", train_id, sizeof(train_id));
   jsmn_find_extract_token(string, tokens, index, "loc_stanox", stanox, sizeof(stanox));

   time_t now = time(NULL);
   sprintf(query, "INSERT INTO trust_cancellation VALUES(%ld, '%s', '', '', '%s', 1)", now, train_id, stanox);
   db_query(query);
   
   return;
}

static void process_trust_0006(const char * const string, const jsmntok_t * const tokens, const int index)
{
   char query[1024];
   char train_id[128], reason[128], stanox[128];
   
   jsmn_find_extract_token(string, tokens, index, "train_id", train_id, sizeof(train_id));
   jsmn_find_extract_token(string, tokens, index, "reason_code", reason, sizeof(reason));
   jsmn_find_extract_token(string, tokens, index, "loc_stanox", stanox, sizeof(stanox));

   time_t now = time(NULL);
   sprintf(query, "INSERT INTO trust_changeorigin VALUES(%ld, '%s', '%s', '%s')", now, train_id, reason, stanox);
   db_query(query);
   
   return;
}

static void process_trust_0007(const char * const string, const jsmntok_t * const tokens, const int index)
{
   char query[1024];
   char train_id[128], new_id[128];
   
   jsmn_find_extract_token(string, tokens, index, "train_id", train_id, sizeof(train_id));
   jsmn_find_extract_token(string, tokens, index, "revised_train_id", new_id, sizeof(new_id));

   time_t now = time(NULL);
   sprintf(query, "INSERT INTO trust_changeid VALUES(%ld, '%s', '%s')", now, train_id, new_id);
   db_query(query);
   
   return;
}

static void create_database(void)
{
   MYSQL_RES * result0;
   MYSQL_ROW row0;
   word create_trust_activation, create_trust_cancellation, create_trust_movement;
   word create_trust_changeorigin, create_trust_changeid;

   create_trust_activation = create_trust_cancellation = create_trust_movement = true;
   create_trust_changeorigin = create_trust_changeid = true;

   db_query("show tables");
   result0 = db_store_result();
   while((row0 = mysql_fetch_row(result0))) 
   {
      if(!strcasecmp(row0[0], "trust_activation")) create_trust_activation = false;
      if(!strcasecmp(row0[0], "trust_cancellation")) create_trust_cancellation = false;
      if(!strcasecmp(row0[0], "trust_movement")) create_trust_movement = false;
      if(!strcasecmp(row0[0], "trust_changeorigin")) create_trust_changeorigin = false;
      if(!strcasecmp(row0[0], "trust_changeid")) create_trust_changeid = false;
   }
   mysql_free_result(result0);

   if(create_trust_activation)
   {
      db_query(
"CREATE TABLE trust_activation "
"(created INT UNSIGNED NOT NULL, "
"trust_id VARCHAR(16) NOT NULL, "
"cif_schedule_id INT UNSIGNED NOT NULL, "
"deduced         TINYINT UNSIGNED NOT NULL, "
"INDEX(cif_schedule_id), INDEX(trust_id), INDEX(created)"
") ENGINE = InnoDB"
               );
      _log(GENERAL, "Created database table trust_activation.");
   }
   if(create_trust_cancellation)
   {
      db_query(
"CREATE TABLE trust_cancellation "
"(created INT UNSIGNED NOT NULL, "
"trust_id VARCHAR(16) NOT NULL, "
"reason VARCHAR(8) NOT NULL, "
"type   VARCHAR(32) NOT NULL, "
"loc_stanox VARCHAR(8) NOT NULL, "
"reinstate TINYINT UNSIGNED NOT NULL, "
"INDEX(trust_id), INDEX(created) "
") ENGINE = InnoDB"
               );
      _log(GENERAL, "Created database table trust_cancellation.");
   }
   if(create_trust_movement)
   {
      db_query(
"CREATE TABLE trust_movement "
"(created            INT UNSIGNED NOT NULL, "
"trust_id            VARCHAR(16) NOT NULL, "
"event_type          VARCHAR(16) NOT NULL, "
"planned_event_type  VARCHAR(16) NOT NULL, "
"platform            VARCHAR(8) NOT NULL, "
"loc_stanox          VARCHAR(8) NOT NULL, "
"actual_timestamp    INT UNSIGNED NOT NULL, "
"gbtt_timestamp      INT UNSIGNED NOT NULL, "
"planned_timestamp   INT UNSIGNED NOT NULL, "
"timetable_variation INT NOT NULL, "
"event_source        VARCHAR(16) NOT NULL, "
"offroute_ind        BOOLEAN NOT NULL, "
"train_terminated    BOOLEAN NOT NULL, "
"variation_status    VARCHAR(16) NOT NULL, "
"next_report_stanox  VARCHAR(8) NOT NULL, "
"next_report_run_time INT UNSIGNED NOT NULL, "
"INDEX(trust_id), INDEX(created) "
") ENGINE = InnoDB"
               );
      _log(GENERAL, "Created database table trust_movement.");
   }
   if(create_trust_changeorigin)
   {
      db_query(
"CREATE TABLE trust_changeorigin "
"(created INT UNSIGNED NOT NULL, "
"trust_id VARCHAR(16) NOT NULL, "
"reason VARCHAR(8) NOT NULL, "
"loc_stanox VARCHAR(8) NOT NULL, "
"INDEX(trust_id), INDEX(created) "
") ENGINE = InnoDB"
               );
      _log(GENERAL, "Created database table trust_changeorigin.");
   }
   if(create_trust_changeid)
   {
      db_query(
"CREATE TABLE trust_changeid "
"(created INT UNSIGNED NOT NULL, "
"trust_id VARCHAR(16) NOT NULL, "
"new_trust_id VARCHAR(16) NOT NULL, "
"INDEX(trust_id), INDEX(new_trust_id), INDEX(created) "
") ENGINE = InnoDB"
               );
      _log(GENERAL, "Created database table trust_changeid.");
   }
}

static void jsmn_dump_tokens(const char * const string, const jsmntok_t * const tokens, const word object_index)
{
   char zs[256], zs1[256];

   word i;
   for(i = object_index; tokens[i].start >= 0 && tokens[i].start < tokens[object_index].end; i++)
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
   j=3;
   zs[0]=zs[1]=zs[2]=' ';
   while(headers[i])
   {
      if(headers[i] == '\n')
      {
         zs[j] = '\0';
         _log(GENERAL, zs);
         j = 3;
         i++;
      }
      else
      {
         zs[j++] = headers[i++];
      }
   }
}

static char * report_error(const int error)
{
   static char stomp_error[256];
   if(error < 0)
   {
      switch(error)
      {
      case -2: sprintf(stomp_error, "Stomp error:  Unknown host.");      break;
      case -6: sprintf(stomp_error, "Stomp error:  Failed setsockopt."); break;
      case -7: sprintf(stomp_error, "Stomp error:  End of file.");       break;
      default: sprintf(stomp_error, "Stomp error %d", error);            break;
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
      sprintf(zs, "%25s: %-12s ", stats_category[i], commas_ll(stats[i]));
      strcat(zs, commas_ll(grand_stats[i]));
      _log(GENERAL, zs);
      strcat(report, zs);
      strcat(report, "\n");
      stats[i] = 0;
   }
   email_alert(NAME, BUILD, "Statistics Report", report);
}

static void log_message(const char * const message)
{
   FILE * fp;
   char filename[128];

   time_t now = time(NULL);
   struct tm * broken = gmtime(&now);
   
   sprintf(filename, "/tmp/trustdb-messages-%04d-%02d-%02d.log", broken->tm_year + 1900, broken->tm_mon + 1, broken->tm_mday);
   
   if((fp = fopen(filename, "a")))
   {
      fprintf(fp, "%s\n%s\n", time_text(now, false), message);
      fclose(fp);
   }
}

static time_t correct_trust_timestamp(const time_t in)
{
   struct tm * broken = localtime(&in);

   if(in && broken->tm_isdst) return in - 60*60;
   return in;
}
