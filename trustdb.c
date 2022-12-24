/*
    Copyright (C) 2013, 2014, 2015, 2016, 2017, 2018, 2022 Phil Wieland

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
#include <netinet/in.h>
#include <errno.h>
#include <netdb.h>

#include "jsmn.h"
#include "misc.h"
#include "db.h"
#include "database.h"
#include "build.h"

#define NAME  "trustdb"

#ifndef RELEASE_BUILD
#define BUILD "3724p"
#else
#define BUILD RELEASE_BUILD
#endif

static void perform(void);
static void process_frame(const char * const body);
static void process_trust_0001(const char * const string, const jsmntok_t * const tokens, const int index);
static void process_trust_0002(const char * const string, const jsmntok_t * const tokens, const int index);
static void process_trust_0003(const char * const string, const jsmntok_t * const tokens, const int index);
static void process_trust_0005(const char * const string, const jsmntok_t * const tokens, const int index);
static void process_trust_0006(const char * const string, const jsmntok_t * const tokens, const int index);
static void process_trust_0007(const char * const string, const jsmntok_t * const tokens, const int index);
static void process_trust_0008(const char * const string, const jsmntok_t * const tokens, const int index);
static void jsmn_dump_tokens(const char * const string, const jsmntok_t * const tokens, const word object_index);
static void report_stats(void);
#define INVALID_SORT_TIME 9999
static time_t correct_trust_timestamp(const time_t in);
static void init_deferred_activations(void);
static void defer_activation(const char * const uid, const time_t schedule_start_date, const time_t schedule_end_date, const char * const trust_id);
static void process_deferred_activations(void);
static word count_deferred_activations(void);
static void check_timeout(void);

static word debug, run, interrupt, holdoff;
static char zs[4096];

#define FRAME_SIZE 64000
static char body[FRAME_SIZE];

#define NUM_TOKENS 8192
static jsmntok_t tokens[NUM_TOKENS];

// stompy port for trust stream
#define STOMPY_PORT 55841

// Time in hours (local) when daily statistical report is produced.
// (Set > 23 to disable daily report.)
#define REPORT_HOUR 4
#define REPORT_MINUTE 2
static word last_report_day;

static time_t start_time, now;

// Status
static time_t status_last_trust_processed, status_last_trust_actual;

// Stats
enum stats_categories {ConnectAttempt, GoodMessage, // Don't insert any here
                       Mess1, Mess2, Mess3, Mess4, Mess5, Mess6, Mess7, Mess8,
                       NotMessage, NotRecog, Mess1Miss, Mess1MissHit, Mess1Cape, MovtNoAct, DeducedAct, 
                       DeducedHC, DeducedHCReplaced, DeducedTSC, MAXstats};
static qword stats[MAXstats];
static qword grand_stats[MAXstats];
static const char * stats_category[MAXstats] = 
   {
      "Stompy connect attempt", "Good message", 
      "Message type 1","Message type 2","Message type 3","Message type 4","Message type 5","Message type 6","Message type 7","Message type 8",
      "Not a message", "Invalid or not recognised", "Activation no schedule", "Found by second search", "Act. cancelled schedule", "Movement without act.", "Deduced activation",
      "Deduced headcode", "Changed deduced headcode", "Deduced TSC",
   };

// Message count
word message_count;
time_t message_count_report_due;
#define MESSAGE_COUNT_REPORT_INTERVAL 64

// Latency check
static qword latency_sum, latency_count, latency_max;
static time_t latency_check_due;
static byte latency_alarm_raised;
#define LATENCY_CHECK_INTERVAL 256
#define LATENCY_ALARM_THRESHOLD 16

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
   strcpy(config_file_path, "/etc/openrail.conf");
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
   _log_init(debug?"/tmp/trustdb.log":"/var/log/garner/trustdb.log", debug?1:0);

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
      for(i=0; i < MAXstats; i++) { stats[i] = 0; grand_stats[i] = 0; }
   }
   init_deferred_activations();

   // Startup delay
   {
      struct sysinfo info;
      word logged = false;
      word i;
      while(run && !debug && !sysinfo(&info) && info.uptime < (512 + 0))
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
   word stompy_timeout = true;

   // Initialise database
   while(db_init(conf[conf_db_server], conf[conf_db_user], conf[conf_db_password], conf[conf_db_name], DB_MODE_NORMAL) && run) 
   {
      _log(CRITICAL, "Failed to initialise database connection.  Will retry...");
      word i;
      for(i = 0; i < 64 && run; i++) sleep(1);
   }

   word e;
   if((e = database_upgrade(trustdb)))
   {
      _log(CRITICAL, "Error %d in database_upgrade().  Aborting.", e);
      exit(1);
   }

   {
      now = time(NULL);
      struct tm * broken = localtime(&now);
      last_report_day = broken->tm_wday;
      message_count_report_due = now + MESSAGE_COUNT_REPORT_INTERVAL;
      latency_check_due = now + LATENCY_CHECK_INTERVAL;
      message_count = 0;
      latency_alarm_raised = latency_sum = latency_count = latency_max = 0;
   }

   // Status
   status_last_trust_processed = status_last_trust_actual = 0;

   while(run)
   {   
      stats[ConnectAttempt]++;
      int run_receive = !open_stompy(STOMPY_PORT);
      while(run && run_receive)
      {
         holdoff = 0;
         check_timeout();

         int r = read_stompy(body, FRAME_SIZE, 128);
         _log(DEBUG, "read_stompy() returned %d.", r);
         if(!r && run && run_receive)
         {
            if(stompy_timeout)
            {
               _log(MINOR, "TRUST message stream - Receive OK.");
               stompy_timeout = false;
            }
            if(db_start_transaction())
            {
               run_receive = false;
            }
            if(run_receive) process_deferred_activations();
            if(run_receive && !db_errored) process_frame(body);

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
               if(!stompy_timeout) _log(MINOR, "Receive timeout on stompy connection."); 
               stompy_timeout = true;
            }
         }
      } // while(run_receive && run)
      close_stompy();
      if(run) check_timeout();
      {      
         word i;
         if(holdoff < 256) holdoff += 30;
         else holdoff = 256;
         for(i = 0; i < holdoff + 64 && run; i++) sleep(1);
      }
   }  // while(run)

   if(interrupt)
   {
      _log(CRITICAL, "Terminating due to interrupt.");
   }

   db_disconnect();
   word lost = count_deferred_activations();
   if(lost) _log(MINOR, "%d deferred activation%s discarded.", lost, (lost == 1)?"":"s");
   report_stats();
}

static void process_frame(const char * const body)
{
   jsmn_parser parser;
   char query[256];
   qword elapsed = time_ms();
   
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
          _log(DEBUG, "STOMP message is array of %d TRUST messages.", messages);
      }
      else
      {
         messages = 1;
         index = 0;
          _log(DEBUG, "STOMP message contains a single TRUST message.");
      }

      for(i=0; i < messages && !db_errored; i++)
      {
         char message_name[128], queue_timestamp_s[128];
         jsmn_find_extract_token(body, tokens, index, "msg_type", message_name, sizeof(message_name));
         word message_type = atoi(message_name);

         now = time(NULL);
         
         jsmn_find_extract_token(body, tokens, index, "msg_queue_timestamp", queue_timestamp_s, sizeof(queue_timestamp_s));
         queue_timestamp_s[10] = '\0';
         status_last_trust_actual = atol(queue_timestamp_s);
         time_t latency;
         if(now > status_last_trust_actual)
            latency = now - status_last_trust_actual;
         else
            latency = 0;
         //ra_latency_ms = ( (ra_latency_ms * 499) + (latency * 1000))/500;
         latency_sum += latency;
         latency_count++;
         if(latency > latency_max) latency_max = latency;
         _log(DEBUG, "Queue timestamp = %s, latency %ld s, sum = %s s", time_text(status_last_trust_actual, false), latency, commas_q(latency_sum));
         
         if(debug)
         {
            _log(DEBUG, "Processing TRUST message %d", i);
         }
         if(!strncmp(message_name, "000", 3) && message_type > 0 && message_type < 9) 
         {
            stats[GoodMessage]++;
            message_count++;
            stats[GoodMessage + message_type]++;
            status_last_trust_processed = now;

            switch(message_type)
            {
            case 1: process_trust_0001(body, tokens, index); break;
            case 2: process_trust_0002(body, tokens, index); break;
            case 3: process_trust_0003(body, tokens, index); break;
            case 5: process_trust_0005(body, tokens, index); break;
            case 6: process_trust_0006(body, tokens, index); break;
            case 7: process_trust_0007(body, tokens, index); break;
            case 8: process_trust_0008(body, tokens, index); break;
            default:
               _log(MINOR, "Message type \"%s\" discarded.", message_name);
               break;
            }
         }
         else
         {
            _log(MINOR, "Unrecognised message type \"%s\".", message_name);
            jsmn_dump_tokens(body, tokens, index);
            stats[NotRecog]++;
         }
         
         size_t message_ends = tokens[index].end;
         do  index++; 
         while ( tokens[index].start < message_ends && tokens[index].start >= 0 && index < NUM_TOKENS);

      }
   }
   elapsed = time_ms() - elapsed;
   if(debug || elapsed > 2500)
   {
      _log(MINOR, "Frame took %s ms to process.", commas_q(elapsed));
   }
   sprintf(query, "UPDATE status SET last_trust_processed = %ld, last_trust_actual = %ld", status_last_trust_processed, status_last_trust_actual);
   db_query(query);
}

static void process_trust_0001(const char * const string, const jsmntok_t * const tokens, const int index)
{
   char zs[128], zs1[128], report[1024];
   char train_id[64], train_uid[64], tsc[64];
   char query[4096];
   dword cif_schedule_id = 0;
   MYSQL_RES * result0;
   MYSQL_ROW row0;
   
   sprintf(report, "Activation message:");

   jsmn_find_extract_token(string, tokens, index, "train_id", train_id, sizeof(train_id));
   sprintf(zs1, " train_id=\"%s\"", train_id);
   strcat(report, zs1);

   jsmn_find_extract_token(string, tokens, index, "schedule_start_date", zs, sizeof(zs));
   time_t schedule_start_date_stamp = parse_datestamp(zs);
   sprintf(zs1, " schedule_start_date=%.32s", zs);
   strcat(report, zs1);

   jsmn_find_extract_token(string, tokens, index, "schedule_end_date", zs, sizeof(zs));
   time_t schedule_end_date_stamp   = parse_datestamp(zs);
   sprintf(zs1, " schedule_end_date=%.32s", zs);
   strcat(report, zs1);

   jsmn_find_extract_token(string, tokens, index, "train_uid", train_uid, sizeof(train_uid));
   sprintf(zs1, " train_uid=\"%s\"", train_uid);
   strcat(report, zs1);

   jsmn_find_extract_token(string, tokens, index, "schedule_source", zs, sizeof(zs));
   sprintf(zs1, " schedule_source=\"%.8s\"", zs);
   strcat(report, zs1);

   jsmn_find_extract_token(string, tokens, index, "schedule_wtt_id", zs, sizeof(zs));
   sprintf(zs1, " schedule_wtt_id=\"%.16s\"", zs);
   strcat(report, zs1);

   sprintf(query, "select id, CIF_stp_indicator from cif_schedules where cif_train_uid = '%s' AND schedule_start_date = %ld AND schedule_end_date = %ld AND deleted > %ld ORDER BY LOCATE(CIF_stp_indicator, 'ONPC'), created DESC", train_uid, schedule_start_date_stamp, schedule_end_date_stamp, now);
   if(!db_query(query))
   {
      word cancelled = false;
      result0 = db_store_result();
      word num_rows = mysql_num_rows(result0);
      if(num_rows < 1) 
      {
         mysql_free_result(result0);
         stats[Mess1Miss]++;
         _log(MINOR, report);
         _log(MINOR, "   No schedules found.  Deferring activation.");
         defer_activation(train_uid, schedule_start_date_stamp, schedule_end_date_stamp, train_id);
      }
      else
      {
         row0 = mysql_fetch_row(result0);
         cif_schedule_id = atol(row0[0]);
         if(row0[1][0] == 'C')
         {
            // Activation matches cancelled schedule.  This is BAU so don't log it
            _log(DEBUG, report);
            _log(DEBUG, "   Matches cancelled schedule %ld.", cif_schedule_id);
            stats[Mess1Cape]++;
            cancelled = true;
         }
         sprintf(query, "INSERT INTO trust_activation VALUES(%ld, '%s', %u, 0)", now, train_id, cif_schedule_id);
         db_query(query);
         mysql_free_result(result0);

         // Process "extra" data
         sprintf(query, "INSERT INTO trust_activation_extra VALUES(%ld, '%s', '", now, train_id);

         jsmn_find_extract_token(string, tokens, index, "schedule_source", zs, sizeof(zs));
         strcat(query, zs);
         strcat(query, "', '");
         
         jsmn_find_extract_token(string, tokens, index, "train_file_address", zs, sizeof(zs));
         strcat(query, zs);
         strcat(query, "', ");
             
         jsmn_find_extract_token(string, tokens, index, "schedule_end_date", zs, sizeof(zs));
         sprintf(zs1, "%lu", parse_datestamp(zs));
         strcat(query, zs1);
         strcat(query, ", ");
                             
         jsmn_find_extract_token(string, tokens, index, "tp_origin_timestamp", zs, sizeof(zs));
         sprintf(zs1, "%lu", parse_datestamp(zs));
         strcat(query, zs1);
         strcat(query, ", ");
              
         jsmn_find_extract_token(string, tokens, index, "creation_timestamp", zs, sizeof(zs));
         zs[10] = '\0';
         sprintf(zs1, "%lu", correct_trust_timestamp(atol(zs)));
         strcat(query, zs1);
         strcat(query, ", '");
               
         jsmn_find_extract_token(string, tokens, index, "tp_origin_stanox", zs, sizeof(zs));
         strcat(query, zs);
         strcat(query, "', ");
                 
         jsmn_find_extract_token(string, tokens, index, "origin_dep_timestamp", zs, sizeof(zs));
         zs[10] = '\0';
         sprintf(zs1, "%lu", correct_trust_timestamp(atol(zs)));
         strcat(query, zs1);
         strcat(query, ", '");
             
         jsmn_find_extract_token(string, tokens, index, "train_service_code", tsc, sizeof(tsc));
         strcat(query, tsc);
         strcat(query, "', '");
               
         jsmn_find_extract_token(string, tokens, index, "toc_id", zs, sizeof(zs));
         strcat(query, zs);
         strcat(query, "', '");
                           
         jsmn_find_extract_token(string, tokens, index, "d1266_record_number", zs, sizeof(zs));
         strcat(query, zs);
         strcat(query, "', '");
              
         jsmn_find_extract_token(string, tokens, index, "train_call_type", zs, sizeof(zs));
         strcat(query, zs);
         strcat(query, "', '");
                  
         jsmn_find_extract_token(string, tokens, index, "train_uid", zs, sizeof(zs));
         strcat(query, zs);
         strcat(query, "', '");
                        
         jsmn_find_extract_token(string, tokens, index, "train_call_mode", zs, sizeof(zs));
         strcat(query, zs);
         strcat(query, "', '");
                  
         jsmn_find_extract_token(string, tokens, index, "schedule_type", zs, sizeof(zs));
         strcat(query, zs);
         strcat(query, "', '");
                    
         jsmn_find_extract_token(string, tokens, index, "sched_origin_stanox", zs, sizeof(zs));
         strcat(query, zs);
         strcat(query, "', '");
              
         jsmn_find_extract_token(string, tokens, index, "schedule_wtt_id", zs, sizeof(zs));
         strcat(query, zs);
         strcat(query, "', ");
                  
         jsmn_find_extract_token(string, tokens, index, "schedule_start_date", zs, sizeof(zs));
         sprintf(zs1, "%lu", parse_datestamp(zs));
         strcat(query, zs1);
         strcat(query, ")");
              
         db_query(query);
      }

      if(cif_schedule_id && !cancelled &&
         train_id[2] >= '0' && train_id[2] <= '9' &&
         (train_id[3] < 'A' || train_id[3] > 'Z'  ||
          train_id[4] < '0' || train_id[4] > '9'  ||
          train_id[5] < '0' || train_id[5] > '9'))
      {
         // This has an obfuscated headcode.
         _log(MINOR, "Activation with obfuscated headcode received, trust ID \"%s\".", train_id);
         char obfus_hc[16], true_hc[8];
         strcpy(obfus_hc, train_id + 2);
         obfus_hc[4] = '\0';
         sprintf(query, "SELECT signalling_id, deduced_headcode, deduced_headcode_status from cif_schedules where id = %u", cif_schedule_id);
         if(!db_query(query))
         {
            result0 = db_store_result();
            if((row0 = mysql_fetch_row(result0)))
            {
               char status[32];
               true_hc[0] = '\0';
               if(row0[0][0])
               {
                  strcpy(true_hc, row0[0]);
                  strcpy(status, "From schedule");
               }
               else if(row0[1][0])
               {
                  strcpy(true_hc, row0[1]);
                  strcpy(status, "Status ");
                  strcat(status, row0[2]);
               }
               if(true_hc[0] == obfus_hc[0]) // Only if class is the same.
               {
                  sprintf(query, "INSERT INTO obfus_lookup (created, true_hc, obfus_hc) VALUES(%ld, '%s', '%s')", now, true_hc, obfus_hc);
                  db_query(query);
                  _log(DEBUG, "Added obfuscated headcode \"%s\", true headcode \"%s\" (%s) to obfuscation lookup table.  TRUST id \"%s\", garner schedule id %u.",obfus_hc, true_hc, status, train_id, cif_schedule_id);
                  sprintf(query, "DELETE FROM obfus_lookup WHERE created < %ld", now - 86400L); // 24 hours.
                  db_query(query);
               }
               else if(true_hc[0])
               {
                  _log(MINOR, "Discarded obfuscated headcode \"%s\", true headcode \"%s\" (%s) as class does not match.  TRUST id \"%s\", garner schedule id %u.",obfus_hc, true_hc, status, train_id, cif_schedule_id);
               }
            }
            mysql_free_result(result0);
         }
      }

      // Deduced headcode
      if(cif_schedule_id && !cancelled &&
         train_id[2] >= '0' && train_id[2] <= '9' &&
         train_id[3] >= 'A' && train_id[3] <= 'Z' &&
         train_id[4] >= '0' && train_id[4] <= '9' &&
         train_id[5] >= '0' && train_id[5] <= '9')
      {
         char act_headcode[16];
         strcpy(act_headcode, train_id + 2);
         act_headcode[4] = '\0';
         sprintf(query, "SELECT signalling_id, deduced_headcode, deduced_headcode_status, CIF_train_service_code from cif_schedules where id = %u", cif_schedule_id);
         if(!db_query(query))
         {
            result0 = db_store_result();
            if((row0 = mysql_fetch_row(result0)))
            {
               _log(DEBUG, "Deduce headcode:  From trust id \"%s\", sig id \"%s\", deduced (before) \"%s\", deduced status \"%s\", garner schedule id %u.",act_headcode, row0[0], row0[1], row0[2], cif_schedule_id);
               if(row0[0][0] && strcmp(act_headcode, row0[0]))
               {
                  _log(MINOR, "Headcode \"%s\" from trust ID does not match schedule headcode \"%s\".", act_headcode, row0[0]);
               }
               else if(!row0[0][0])
               {
                  // Schedule doesn't have a specified headcode.
                  if(!row0[1][0])
                  {
                     // Not previously deduced
                     sprintf(query, "UPDATE cif_schedules SET deduced_headcode = '%s', deduced_headcode_status = 'A' WHERE id = %u", act_headcode, cif_schedule_id);
                     _log(GENERAL, "Deduced headcode \"%s\" for schedule %u.", act_headcode, cif_schedule_id);
                     db_query(query);
                     stats[DeducedHC]++;
                  }
                  else if(strcmp(row0[1], act_headcode) && row0[2][0] == 'A')
                  {
                     // Previously automatically deduced and it doesn't match
                     sprintf(query, "UPDATE cif_schedules SET deduced_headcode = '%s', deduced_headcode_status = 'A' WHERE id = %u", act_headcode, cif_schedule_id);
                     _log(MAJOR, "Previously deduced headcode \"%s\", status \"%s\" replaced by \"%s\" for schedule %u.", row0[1], row0[2], act_headcode, cif_schedule_id);
                     db_query(query);
                     stats[DeducedHCReplaced]++;
                  }
                  else if(strcmp(row0[1], act_headcode))
                  {
                     _log(MINOR, "Previously deduced headcode \"%s\", status \"%s\" not replaced by \"%s\" for schedule %u.", row0[1], row0[2], act_headcode, cif_schedule_id);
                  }
                  else
                  {
                     // Matches previous headcode.
                     _log(DEBUG, "Previously deduced headcode \"%s\", status \"%s\" unchanged for schedule %ld, headcode \"%s\".", row0[1], row0[2], cif_schedule_id, act_headcode);
                  }
               }

               // Deduce TSC
               _log(DEBUG, "TSC: In activation \"%s\", in schedule \"%s\", schedule %ld.", tsc, row0[3], cif_schedule_id);
               if(strcmp(tsc, row0[3]))
               {
                  if((!row0[3][0]) || (row0[3][0] == ' '))
                  {
                     _log(GENERAL, "TSC in activation message \"%s\", schedule %ld has \"%s\".  Schedule updated.", tsc, cif_schedule_id, row0[3]);
                     sprintf(query, "UPDATE cif_schedules SET CIF_train_service_code = '%s' WHERE id = %u", tsc, cif_schedule_id);
                     db_query(query);
                     stats[DeducedTSC]++;
                  }
                  else
                  {
                     _log(DEBUG, "TSC in activation message \"%s\", schedule %ld has \"%s\".  Not updated.", tsc, cif_schedule_id, row0[3]);
                  }
               }            
               
            }
            mysql_free_result(result0);
         }
      }
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

   sprintf(query, "INSERT INTO trust_cancellation VALUES(%ld, '%s', '%s', '%s', '%s', 0)", now, train_id, reason, type, stanox);
   db_query(query);
   
   return;
}

static void process_trust_0003(const char * string, const jsmntok_t * tokens, const int index)
{
  char query[1024], zs[32], zs1[32], train_id[16], loc_stanox[16], event_type[16];
   word flags;
  
   time_t planned_timestamp, actual_timestamp, timestamp;

   flags = 0;
   
   sprintf(query, "INSERT INTO trust_movement VALUES(%ld, '", now);
   jsmn_find_extract_token(string, tokens, index, "train_id", train_id, sizeof(train_id));
   strcat(query, train_id);
   strcat(query, "', '");
   jsmn_find_extract_token(string, tokens, index, "event_type", event_type, sizeof(event_type));
   jsmn_find_extract_token(string, tokens, index, "planned_event_type", zs, sizeof(zs));
   if     (event_type[0] == 'D' && zs[0] == 'D') flags = 0x0001;
   else if(event_type[0] == 'A' && zs[0] == 'A') flags = 0x0002;
   else if(event_type[0] == 'A' && zs[0] == 'D') flags = 0x0003; // ARRIVAL, DESTINATION
   else _log(MAJOR, "TRUST movement:  Unexpected fields event_type \"%s\", planned_event_type \"%s\".", event_type, zs);
   jsmn_find_extract_token(string, tokens, index, "platform", zs, sizeof(zs));
   strcat(query, zs);
   strcat(query, "', '");
   jsmn_find_extract_token(string, tokens, index, "loc_stanox", loc_stanox, sizeof(loc_stanox));
   strcat(query, loc_stanox);
   strcat(query, "', ");
   jsmn_find_extract_token(string, tokens, index, "actual_timestamp", zs, sizeof(zs));
   zs[10] = '\0';
   actual_timestamp = correct_trust_timestamp(atol(zs));
   sprintf(zs, "%ld", actual_timestamp);
   strcat(query, zs);
   strcat(query, ", ");
   //if(actual_timestamp > status_last_trust_actual)
   //{
   //   status_last_trust_actual = actual_timestamp;
   //}
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
   switch(zs[0])
      {
      case 'A': break;
      case 'M': flags += 0x0004; break;
      default: _log(MAJOR, "TRUST movement:  Unexpected event_source field \"%s\".", zs); break;
      }
   jsmn_find_extract_token(string, tokens, index, "offroute_ind", zs, sizeof(zs));
   if(zs[0] == 't') flags += 0x0020;
   jsmn_find_extract_token(string, tokens, index, "train_terminated", zs, sizeof(zs));
   if(zs[0] == 't') flags += 0x0040;
   jsmn_find_extract_token(string, tokens, index, "variation_status", zs, sizeof(zs));
   switch(zs[2])
      {
      case 'R': break; // EARLY
      case ' ': flags += 0x0008; break; // ON TIME
      case 'T': flags += 0x0010; break; // LATE
      case 'F': flags += 0x0018; break; // OFF ROUTE
      default: _log(MAJOR, "TRUST movement:  Unexpected variation_status field \"%s\".", zs); break;
      }
   jsmn_find_extract_token(string, tokens, index, "next_report_stanox", zs, sizeof(zs));
   strcat(query, zs);
   strcat(query, "', ");
   jsmn_find_extract_token(string, tokens, index, "next_report_run_time", zs, sizeof(zs));
   sprintf(zs1, "%d", atoi(zs));
   strcat(query, zs1);
   strcat(query, ", ");
   jsmn_find_extract_token(string, tokens, index, "correction_ind", zs, sizeof(zs));
   if(zs[0] == 't') flags += 0x0080;

   sprintf(zs1, "%d", flags);
   strcat(query, zs1);
   
   strcat(query, ")");

   db_query(query);

   // Old one?
   if(planned_timestamp && now - actual_timestamp > 12*60*60)
   {
      _log(DEBUG, "Late movement message received, actual timestamp %s.", time_text(actual_timestamp, true));
   }

   // NB Don't accept cif_schedule_id==0 ones here as the schedule may have arrived after the activation!
   // This can happen due to a VSTP race, hopefully fixed V505
   // OR due to the service being activated before the daily timetable download.
   sprintf(query, "SELECT * from trust_activation where trust_id = '%s' and created > %ld and cif_schedule_id > 0", train_id, actual_timestamp - (4*24*60*60));
   if(!(*conf[conf_trustdb_no_deduce_act]) && !db_query(query))
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
         // Movement no activation.  Attempt to create the missing activation.
         MYSQL_ROW row0;
         char tiploc[128], reason[128];
         qword elapsed = time_ms();
         char planned[8];

         reason[0] = '\0';
         strcpy(planned, "-----");
         tiploc[0] = '\0';

         _log(MINOR, "Movement message received with no matching activations, TRUST id = \"%s\".", train_id);

         sprintf(query, "SELECT * from trust_activation where trust_id = '%s' and created > %ld AND cif_schedule_id = 0", train_id, actual_timestamp - (4*24*60*60));
         if(!db_query(query))
         {
            MYSQL_RES * result0 = db_store_result();

            word num_rows = mysql_num_rows(result0);
            mysql_free_result(result0);
            if(num_rows > 0)
            {
               _log(MINOR, "   A matching activation with no schedule exists.");
            }
         }
         stats[MovtNoAct]++;

         if(!run)
         {
            strcpy(reason, "Abort received");
         }
         else if(*conf[conf_trustdb_no_deduce_act])
         {
            strcpy(reason, "Deduce disabled by option");            
         }
         else if(planned_timestamp == 0)
         {
            strcpy(reason, "No planned timestamp");
         }
         else if(now - actual_timestamp > (16L*60L*60L))
         {
            strcpy(reason, "Movement is too old");
         }

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
            //sort_time = broken->tm_hour * 4 * 60 + broken->tm_min * 4;
            sprintf(planned, "%02d%02d%s", broken->tm_hour, broken->tm_min, (broken->tm_sec > 29)?"H":"");
            // Select the day
            word day = broken->tm_wday;
            word yest = (day + 6) % 7;
            broken->tm_hour = 12;
            broken->tm_min = 0;
            broken->tm_sec = 0;
            time_t when = timegm(broken);

            // TODO It is this query which takes ages.  Can we accelerate it?
            sprintf(query, "SELECT cif_schedules.id, cif_schedules.CIF_train_uid, signalling_id, CIF_stp_indicator FROM cif_schedules INNER JOIN cif_schedule_locations AS l ON cif_schedules.id = l.cif_schedule_id WHERE l.tiploc_code = '%s'",
                    tiploc);

            if(event_type[0] == 'A')
               sprintf(query1, " AND (l.arrival = '%s' OR l.pass = '%s')", planned, planned);
            else if(event_type[0] == 'D')
               sprintf(query1, " AND (l.departure = '%s' OR l.pass = '%s')", planned, planned);
            else
            {
               sprintf(query1, " AND 0");
               strcpy(reason, "Unrecognised event type");
            }
            strcat(query, query1);

            strcat(query, " AND (cif_schedules.CIF_stp_indicator = 'N' OR cif_schedules.CIF_stp_indicator = 'P' OR cif_schedules.CIF_stp_indicator = 'O')");

            static const char * days_runs[8] = {"runs_su", "runs_mo", "runs_tu", "runs_we", "runs_th", "runs_fr", "runs_sa", "runs_su"};

            //
            sprintf(query1, " AND (((%s) AND (schedule_start_date <= %ld) AND (schedule_end_date >= %ld) AND (NOT next_day))",   days_runs[day],  when + 12*60*60, when - 12*60*60);
            strcat(query, query1);
            sprintf(query1, " OR   ((%s) AND (schedule_start_date <= %ld) AND (schedule_end_date >= %ld) AND (    next_day)))",  days_runs[yest], when - 12*60*60, when - 36*60*60);
            strcat(query, query1);

            //                                      Exclude buses . . . . . . . . . . . . . . .
            sprintf(query1, " AND deleted > %ld AND train_status != 'B' AND train_status != '5' ORDER BY LOCATE(CIF_stp_indicator, 'NPO')", planned_timestamp);
            strcat(query, query1);
            if(!db_query(query))
            {
#define ROWS 8
               MYSQL_ROW rows[ROWS];
               word row_count, row, headcode_match;
               char save_uid[16], target_head[16], save_stp;
               save_uid[0] = save_stp = '\0';
               dword cif_schedule_id = 0;
               result0 = db_store_result();
               num_rows = mysql_num_rows(result0);
               if(!num_rows)
               {
                  strcpy(reason, "No schedules found");
               }
               headcode_match = false;
               strcpy(target_head, train_id + 2);
               target_head[4] = '\0';
               for(row_count = 0; row_count < ROWS && (rows[row_count] = mysql_fetch_row(result0)); row_count++)
               {
                  _log(MINOR, "   Potential match:%8s (%s %s) %4s", rows[row_count][0], rows[row_count][1], rows[row_count][3], rows[row_count][2]);
                  if(!strcmp(rows[row_count][2], target_head))
                  {
                     headcode_match = true;
                     _log(DEBUG,"   Headcode match \"%s\".", target_head);
                  }
               }
               for(row = 0; row < row_count && !reason[0]; row++)
               {
                  // If we have a headcode match, only consider matching rows.  (If none match, consider all rows.)
                  if((!headcode_match) || (!strcmp(rows[row][2], target_head)))
                  {
                     if(save_uid[0] && strcmp(save_uid, rows[row][1]))  strcpy(reason, "Multiple matching schedule UIDs");
                     else if (save_stp == 'O' && rows[row][3][0] =='O') strcpy(reason, "Multiple matching overlay schedules");
                     else
                     {
                        cif_schedule_id = atol(rows[row][0]);
                        strcpy(save_uid, rows[row][1]);
                        save_stp = rows[row][3][0];
                     }
                  }
               }
               mysql_free_result(result0);

               if(!reason[0])
               {
                  sprintf(query, "SELECT * from trust_activation where created > %ld and cif_schedule_id = %u", planned_timestamp - (8*60*60), cif_schedule_id);
                  db_query(query);
                  result0 = db_store_result();
                  num_rows = mysql_num_rows(result0);
                  if(num_rows > 0)
                  {
                     //sprintf(reason, "Deduced schedule %u already has an activation recorded", cif_schedule_id);
                     _log(MINOR, "   Deduced schedule %u already has an activation recorded.  Another activation added.", cif_schedule_id);
                  }
                  mysql_free_result(result0);
               }
               if(!reason[0])
               {
                  sprintf(query, "INSERT INTO trust_activation VALUES(%ld, '%s', %u, 1)", now, train_id, cif_schedule_id);
                  db_query(query);
                  elapsed = time_ms() - elapsed;
                  _log(MINOR, "   Successfully deduced schedule %u.  Elapsed time %s ms.", cif_schedule_id, commas_q(elapsed));

                  if(train_id[2] >= '0' && train_id[2] <= '9' &&
                     (train_id[3] < 'A' || train_id[3] > 'Z'  ||
                      train_id[4] < '0' || train_id[4] > '9'  ||
                      train_id[5] < '0' || train_id[5] > '9'))
                  {
                     // This has an obfuscated headcode.  Do we know the real one?
                     char obfus_hc[16], true_hc[8];
                     strcpy(obfus_hc, train_id + 2);
                     obfus_hc[4] = '\0';
                     sprintf(query, "SELECT signalling_id, deduced_headcode, deduced_headcode_status from cif_schedules where id = %u", cif_schedule_id);
                     if(!db_query(query))
                     {
                        result0 = db_store_result();
                        if((row0 = mysql_fetch_row(result0)))
                        {
                           char status[32];
                           true_hc[0] = '\0';
                           if(row0[0][0])
                           {
                              strcpy(true_hc, row0[0]);
                              strcpy(status, "From schedule");
                           }
                           else if(row0[1][0])
                           {
                              strcpy(true_hc, row0[1]);
                              strcpy(status, "Status ");
                              strcat(status, row0[2]);
                           }
                           if(true_hc[0] == obfus_hc[0]) // Only if class is the same.
                           {
                              sprintf(query, "INSERT INTO obfus_lookup VALUES(%ld, '%s', '%s')", now, true_hc, obfus_hc);
                              db_query(query);
                              _log(DEBUG, "   Added obfuscated \"%s\", true \"%s\" (%s) to headcode obfuscation table.  TRUST id \"%s\", garner schedule id %u.  [Deduced activation]", obfus_hc, true_hc, status, train_id, cif_schedule_id);
                              sprintf(query, "DELETE FROM obfus_lookup WHERE created < %ld", now - 86400L); // 24 hours.
                              db_query(query);
                           }
                           else if(true_hc[0])
                           {
                              _log(MINOR, "Discarded obfuscated headcode \"%s\", true headcode \"%s\" (%s) as class does not match.  TRUST id \"%s\", garner schedule id %u.",obfus_hc, true_hc, status, train_id, cif_schedule_id);
                           }
                        }
                        mysql_free_result(result0);
                     }
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
            elapsed = time_ms() - elapsed;
            _log(MINOR, "   Failed to deduce an activation.  Reason:  %s.   Elapsed time %s ms.", reason, commas_q(elapsed));
            _log(MINOR, "      stanox = %s, tiploc = \"%s\", planned_timestamp \"%s\"", loc_stanox, tiploc, planned);
            _log(MINOR, "      actual_timestamp %s.", time_text(actual_timestamp, true));
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

   sprintf(query, "INSERT INTO trust_changeorigin VALUES(%ld, '%s', '%s', '%s')", now, train_id, reason, stanox);
   db_query(query);
   
   return;
}

static void process_trust_0007(const char * const string, const jsmntok_t * const tokens, const int index)
{
   char query[1024];
   char train_id[128], new_id[128];
   MYSQL_RES * result;
   MYSQL_ROW row;
   dword cif_schedule_id = 0;
   
   jsmn_find_extract_token(string, tokens, index, "train_id", train_id, sizeof(train_id));
   jsmn_find_extract_token(string, tokens, index, "revised_train_id", new_id, sizeof(new_id));

   sprintf(query, "INSERT INTO trust_changeid VALUES(%ld, '%s', '%s')", now, train_id, new_id);
   db_query(query);
   
   sprintf(query, "SELECT cif_schedule_id FROM trust_activation WHERE created > %lu AND trust_id = '%s' ORDER BY created DESC",
           now - 20*24*60*60, train_id);
   if(!db_query(query))
   {
      result = db_store_result();
      if((row = mysql_fetch_row(result)))
      {
         cif_schedule_id = atol(row[0]);
      }
      mysql_free_result(result);
   }

   if(cif_schedule_id && 
      new_id[2] >= '0' && new_id[2] <= '9' &&
      (new_id[3] < 'A' || new_id[3] > 'Z'  ||
       new_id[4] < '0' || new_id[4] > '9'  ||
       new_id[5] < '0' || new_id[5] > '9'))
   {
      // This has an obfuscated headcode.
      char obfus_hc[16], true_hc[8];
      strcpy(obfus_hc, new_id + 2);
      obfus_hc[4] = '\0';
      sprintf(query, "SELECT signalling_id, deduced_headcode, deduced_headcode_status from cif_schedules where id = %u", cif_schedule_id);
      if(!db_query(query))
      {
         result = db_store_result();
         if((row = mysql_fetch_row(result)))
         {
            char status[32];
            true_hc[0] = '\0';
            if(row[0][0])
            {
               strcpy(true_hc, row[0]);
               strcpy(status, "From schedule");
            }
            else if(row[1][0])
            {
               strcpy(true_hc, row[1]);
               strcpy(status, "Status ");
               strcat(status, row[2]);
            }
            if(true_hc[0] == obfus_hc[0])
            {
               sprintf(query, "INSERT INTO obfus_lookup (created, true_hc, obfus_hc) VALUES(%ld, '%s', '%s')", now, true_hc, obfus_hc);
               db_query(query);
               _log(GENERAL, "Change id message:  Added obfuscated headcode \"%s\", true headcode \"%s\" (%s) to obfuscation lookup table.  TRUST id \"%s\", was \"%s\", garner schedule id %u.", obfus_hc, true_hc, status, new_id, train_id, cif_schedule_id);
            }
            else if(true_hc[0])
            {
               _log(MINOR, "Change id message:  Discarded obfuscated headcode \"%s\", true headcode \"%s\" (%s) as class does not match.  TRUST id \"%s\", was \"%s\", garner schedule id %u.",obfus_hc, true_hc, status, new_id, train_id, cif_schedule_id);
            }
         }
         mysql_free_result(result);
      }
   }
       
   return;
}

static void process_trust_0008(const char * const string, const jsmntok_t * const tokens, const int index)
{
   char query[1024];
   char train_id[128], original_stanox[128], stanox[128];
   
   jsmn_find_extract_token(string, tokens, index, "train_id", train_id, sizeof(train_id));
   jsmn_find_extract_token(string, tokens, index, "original_loc_stanox", original_stanox, sizeof(original_stanox));
   jsmn_find_extract_token(string, tokens, index, "loc_stanox", stanox, sizeof(stanox));

   sprintf(query, "INSERT INTO trust_changelocation (created, trust_id, original_stanox, stanox) VALUES(%ld, '%s', '%s', '%s')", now, train_id, original_stanox, stanox);
   db_query(query);
   
   return;
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

   sprintf(zs, "%25s: %-12s %ld days", "Run time", "", (now - start_time)/(24*60*60));
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

static time_t correct_trust_timestamp(const time_t in)
{
   struct tm * broken = localtime(&in);

   if(in && broken->tm_isdst) return in - 60*60;
   return in;
}

// Deferred activation engine
#define DEFERRED_ACTIVATIONS 16
static struct deferred_activation_detail
{
   char uid[8], trust_id[16];
   time_t due,schedule_start_date, schedule_end_date;
   word active;
}
   deferred_activations[DEFERRED_ACTIVATIONS];

static void init_deferred_activations(void)
{
   word i;
   for(i = 0; i < DEFERRED_ACTIVATIONS; i++)
      deferred_activations[i].active = false;
}

static void defer_activation(const char * const uid, const time_t schedule_start_date, const time_t schedule_end_date, const char * const trust_id)
{
   _log(PROC, "defer_activation(\"%s\", %ld, %ld, \"%s\")", uid, schedule_start_date, schedule_end_date, trust_id);
   if(strlen(uid) > 7 || strlen(trust_id) > 15)
   {
      _log(CRITICAL, "defer_activation() Overlong uid \"%s\" or trust_id \"%s\".  Activation discarded.", uid, trust_id);
      return;
   }
   
   
   word i;
   for(i = 0; i < DEFERRED_ACTIVATIONS && deferred_activations[i].active; i++);

   if(i < DEFERRED_ACTIVATIONS)
   {
      strcpy(deferred_activations[i].uid, uid);
      strcpy(deferred_activations[i].trust_id, trust_id);
      deferred_activations[i].due = now + 32;
      deferred_activations[i].schedule_start_date = schedule_start_date;
      deferred_activations[i].schedule_end_date = schedule_end_date;
      deferred_activations[i].active = true;
   }
   else
   {
      _log(MINOR, "      Activation discarded - Defer queue full.");
   }
}

static void process_deferred_activations(void)
{
   word i;
   now = time(NULL);
   MYSQL_RES * db_result;
   MYSQL_ROW db_row;

   for(i = 0; i < DEFERRED_ACTIVATIONS; i++)
   {
      if(deferred_activations[i].active && deferred_activations[i].due < now)
      {
         char query[1024];
         sprintf(query, "select id from cif_schedules where cif_train_uid = '%s' AND schedule_start_date = %ld AND schedule_end_date = %ld AND deleted > %ld AND CIF_stp_indicator != 'C' ORDER BY LOCATE(CIF_stp_indicator, 'OCNP')", deferred_activations[i].uid, deferred_activations[i].schedule_start_date, deferred_activations[i].schedule_end_date, now);
         if(!db_query(query))
         {
            db_result = db_store_result();
            word num_rows = mysql_num_rows(db_result);
            if(num_rows < 1) 
            {
               _log(MINOR, "No schedules found for deferred activation \"%s\".  Activation recorded without schedule.", deferred_activations[i].trust_id);

               sprintf(query, "INSERT INTO trust_activation VALUES(%ld, '%s', %ld, 0)", now, deferred_activations[i].trust_id, 0L);
               db_query(query);
            }
            else
            {
               db_row = mysql_fetch_row(db_result);
               dword cif_schedule_id = atol(db_row[0]);
               _log(MINOR, "Found schedule %ld for deferred activation \"%s\".", cif_schedule_id, deferred_activations[i].trust_id);
               stats[Mess1MissHit]++;
               sprintf(query, "INSERT INTO trust_activation VALUES(%ld, '%s', %u, 0)", now, deferred_activations[i].trust_id, cif_schedule_id);
               db_query(query);
               // TODO:  We should do the 'deduced headcode' processing here.
            }
            mysql_free_result(db_result);
         }
         deferred_activations[i].active = false;
      }
   }
}               

static word count_deferred_activations(void)
{
   word i, result;
   result = 0;

   for(i = 0; i < DEFERRED_ACTIVATIONS; i++)
   {
      if(deferred_activations[i].active) result++;
   }

   return result;
}

static void check_timeout(void)
{
   char report[1024];

   // Daily report
   now = time(NULL);
   struct tm * broken = localtime(&now);
   if(broken->tm_wday != last_report_day && broken->tm_hour >= REPORT_HOUR && broken->tm_min >= REPORT_MINUTE)
   {
      last_report_day = broken->tm_wday;
      report_stats();
      {
         char query[256];
         sprintf(query, "DELETE FROM message_count WHERE time < %ld", now - (24*60*60));
         db_query(query);
      }
   }
   
   // Message counts
   if(now > message_count_report_due)
   {
      char query[256];
      sprintf(query, "INSERT INTO message_count VALUES('trustdb', %ld, %d)", now, message_count);
      if(!db_query(query))
      {
         _log(DEBUG, "Message count of %d recorded.", message_count);
         message_count = 0;
         message_count_report_due = now + MESSAGE_COUNT_REPORT_INTERVAL;
      }
   }

   // Latency report.
   if(now > latency_check_due)
   {
      if(latency_count > 0)
      {
         qword mean_latency;
         mean_latency = (latency_sum * 1000) / latency_count;
         mean_latency = (mean_latency + 500) / 1000;
         char peak[32];
         strcpy(peak, commas_q(latency_max));
         if(mean_latency > LATENCY_ALARM_THRESHOLD)
         {
            _log(MINOR, "Average message latency %s s, peak %s s.", commas_q(mean_latency), peak);
            if(!latency_alarm_raised && now - start_time > 256)
            {
               sprintf(report, "Average message latency %s s, peak %s s.", commas_q(mean_latency), peak);
               email_alert(NAME, BUILD, "Message Latency Alarm", report);
               latency_alarm_raised = true;
            }
         }
         else
         {
            if(latency_alarm_raised)
            {
               _log(MINOR, "Average message latency %s s, peak %s s.", commas_q(mean_latency), peak);
               sprintf(report, "Average message latency %s s, peak %s s.", commas_q(mean_latency), peak);
               email_alert(NAME, BUILD, "Message Latency Alarm Cleared", report);
               latency_alarm_raised = false;
            }
         }

         latency_sum = latency_count = latency_max = 0;
      }
      latency_check_due = now + LATENCY_CHECK_INTERVAL;
   } 
}
