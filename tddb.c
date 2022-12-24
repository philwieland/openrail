/*
    Copyright (C) 2014, 2015, 2016, 2017, 2018, 2019, 2022 Phil Wieland

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
#include <stdarg.h>

#include "jsmn.h"
#include "misc.h"
#include "db.h"
#include "database.h"
#include "build.h"

#define NAME  "tddb"

#ifndef RELEASE_BUILD
#define BUILD "3316p"
#else
#define BUILD RELEASE_BUILD
#endif

static void perform(void);
static void process_frame(const char * const body);
static void process_message(const word describer, const char * const body, const size_t index);
static void signalling_update(const char * const message_name, const word describer, const time_t t, const word a, const dword d);
static void update_database_berth(const word describer, const char * const k, const char * const v);
static void update_database(const word type, const word describer, const char * const k, const char * const v);
static const char * const query_berth(const word describer, const char * const k); 
static void create_database(void);

static void jsmn_dump_tokens(const char * const string, const jsmntok_t * const tokens, const word object_index);
static void report_stats(void);
static const char * show_signalling_state(const word describer);
static void log_detail(const time_t stamp, const char * text, ...);
static void check_timeout(void);
static void control_mode_change(const word d, const word n);
static void reload_describers(void);

static word debug, run, interrupt, holdoff;
static char zs[4096];

#define FRAME_SIZE 64000
static char body[FRAME_SIZE];

#define NUM_TOKENS 8192
static jsmntok_t tokens[NUM_TOKENS];

// stompy port for TD stream
#define STOMPY_PORT 55842

// Time in hours (local) when daily statistical report is produced.
// (Set > 23 to disable daily report.)
#define REPORT_HOUR 4
#define REPORT_MINUTE 3

static time_t start_time;

// Describers 
#define DESCRIBERS 512
struct {
   char   id[4];
   byte   control_mode;
   byte   timeout_reported;
   word   no_sig_address;
   byte   process_mode;
   time_t last_td_processed,status_last_td_actual;
   char   description[256];
} describers[DESCRIBERS];
word no_describers;
word search_helper[128];

// Status
static time_t status_last_td_processed;
static word no_feed;

// Timers
#define RELOAD_DESCRIBERS_CHECK_INTERVAL 32
static time_t last_reload_describers_check;
#define CHECK_DESCRIBERS_FLOW_INTERVAL 64
static time_t check_describers_flow_due;

// After loss of feed or other events, skip individual describer stream checks
// for n passes of CHECK_DESCRIBERS_FLOW_INTERVAL
#define NO_FEED_LOCKOUT 4
// Threshold in seconds when checking for loss of individual stream.
#define DESCRIBER_FEED_ALARM_THRESHOLD 360

enum data_types {Berth, Signal};

// Stats
enum stats_categories {ConnectAttempt, GoodMessage, RelMessage, CA, CB, CC, CT, SF, SG, SH, NewDesc, NewKey, NotRecog, HandleWrap, MAXstats};
static qword stats[MAXstats];
static qword grand_stats[MAXstats];
static const char * stats_category[MAXstats] = 
   {
      "Stompy connect attempt", "Good message", 
      "Relevant message", "CA message", "CB message", "CC message", "CT message", "SF message", "SG message", "SH message", "New describer", "New key", "Unrecognised message", "Handle wrap",
   };

// Signalling
#define SIG_BYTES 256
static word signalling[DESCRIBERS][SIG_BYTES];

// Update handle.  
#define MAX_HANDLE 0x19a000
static dword handle;

// Message count
word message_count, message_count_rel;
time_t last_message_count_report;
#define MESSAGE_COUNT_REPORT_INTERVAL 64

// Smart berths
#define BERTH_CMP(a, b) (a[3]==b[3]&&a[2]==b[2]&&a[1]==b[1]&&a[0]==b[0])  
#define BERTH_CMP_C(a, b, c, d, e) (a[3]==e&&a[2]==d&&a[1]==c&&a[0]==b)
#define SMART_ORS 4
static struct smart_or_s
{
   const char * bertha;
   char berthav[8];
   word berthb_m1;
   const char * berthb;
   char berthbv[8];
   const char * smart_berth;
} smart_or[SMART_ORS] = {{"E045","",0,"EH45","","ZZ01"},
                         {"E043","",0,"EH43","","ZZ02"},
                         {"ZEHC","",1,"E036","","ZZ03"},
                         {"E049","",0,"EH49","","ZZ04"},
};
word describer_M0, describer_M1;

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
   _log_init(debug?"/tmp/tddb.log":"/var/log/garner/tddb.log", debug?1:0);

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
      
      if((lfp = open("/var/run/tddb.pid", O_RDWR|O_CREAT, 0640)) < 0)
      {
         _log(CRITICAL, "Unable to open pid file \"/var/run/tddb.pid\".  Aborting.");
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

   // Startup delay
   {
      struct sysinfo info;
      word logged = false;
      word i;
      while(run && !debug && !sysinfo(&info) && info.uptime < (512 + 64))
      {
         if(!logged) _log(GENERAL, "Startup delay...");
         logged = true;
         for(i = 0; i < 8 && run; i++) sleep(1);
      }
   }

   if(run) perform();

   if(lfp) close(lfp);

   if(interrupt)
   {
      _log(CRITICAL, "Terminated due to interrupt.");
   }
   return 0;
}

static void perform(void)
{
   word last_report_day;
   word stompy_timeout = true;

   // Initialise database connection
   while(db_init(conf[conf_db_server], conf[conf_db_user], conf[conf_db_password], conf[conf_db_name], DB_MODE_FOUND_ROWS) && run) 
   {
      _log(CRITICAL, "Failed to initialise database connection.  Will retry...");
      word i;
      for(i = 0; i < 64 && run; i++) sleep(1);
   }

   create_database();

   handle = MAX_HANDLE;

   {
      time_t now = time(NULL);
      struct tm * broken = localtime(&now);
      last_report_day = broken->tm_wday;
      last_message_count_report = now;
      message_count = message_count_rel = 0;
      last_reload_describers_check = now;
      check_describers_flow_due = 0;
   }

   // Describers
   no_describers = 0;
   status_last_td_processed = 0;
   no_feed = NO_FEED_LOCKOUT;
   reload_describers();

   // Signalling
   {
      word i,j;
      for(i = 0; i < DESCRIBERS; i++)
      {
         for(j = 0; j < SIG_BYTES; j++)
         {
            signalling[i][j] = 0xffff;
         }
      }
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
            if(now - last_message_count_report > MESSAGE_COUNT_REPORT_INTERVAL)
            {
               char query[256];
               sprintf(query, "INSERT INTO message_count VALUES('tddb', %ld, %d)", now, message_count);
               if(!db_query(query))
               {
                  message_count = 0;
                  last_message_count_report = now;
               }
               sprintf(query, "INSERT INTO message_count VALUES('tddbrel', %ld, %d)", now, message_count_rel);
               if(!db_query(query))
               {
                  message_count_rel = 0;
               }
            }
         }

         int r = read_stompy(body, FRAME_SIZE, 64);
         _log(DEBUG, "read_stompy() returned %d.", r);
         if(!r && run && run_receive)
         {
            if(stompy_timeout)
            {
               _log(MINOR, "TD message stream - Receive OK.");
               stompy_timeout = false;
            }
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
               // DB error.
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
               if(!stompy_timeout) _log(MINOR, "TD message stream - Receive timeout."); 
               no_feed = NO_FEED_LOCKOUT;
               stompy_timeout = true;
            }
         }

         if(!run_receive) no_feed = NO_FEED_LOCKOUT;
         if(run) check_timeout();
      } // while(run_receive && run)

      close_stompy();
      if(run) check_timeout();
      {      
         word i;
         if(holdoff < 256) holdoff += 34;
         else holdoff = 256;
         for(i = 0; i < holdoff + 64 && run; i++) sleep(1);
      }
   }    
   if(interrupt)
   {
      _log(CRITICAL, "Terminating due to interrupt.");
   }

   db_disconnect();
   report_stats();
}

static void process_frame(const char * const body)
{
   jsmn_parser parser;
   qword elapsed = time_ms();
   
   jsmn_init(&parser);
   int r = jsmn_parse(&parser, body, tokens, NUM_TOKENS);
   if(r != 0) 
   {
      _log(MAJOR, "Parser result %d.  Message discarded.", r);
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
         _log(DEBUG, "STOMP message is array of %d TD messages.", messages);
      }
      else
      {
         messages = 1;
         index = 0;
         _log(DEBUG, "STOMP message contains a single TD message.");
      }

      for(i=0; i < messages && run; i++)
      {
         char area_id[4];
         word describer, hit;
         jsmn_find_extract_token(body, tokens, index, "area_id", area_id, sizeof(area_id));

         stats[GoodMessage]++;
         message_count++;

         hit = false;
         for(describer = search_helper[area_id[0] & 0x7f]; describer < no_describers; describer++)
         {
            if(area_id[0] == describers[describer].id[0] && area_id[1] == describers[describer].id[1])
            {
               // HIT
               if(describers[describer].process_mode)
               {
                  process_message(describer, body, index);
                  stats[RelMessage]++;
                  message_count_rel++;
               }
               describer = DESCRIBERS;
               hit = true;
            }
         }

         if(!hit)
         {
            char q[1024];
            // New describer
            // Note:  For ease of coding, this first message is not processed.
            sprintf(q, "Received message from new describer \"%s\".  Added to database.", area_id);
            _log(MINOR, q);
            if(*conf[conf_tddb_report_new]) email_alert(NAME, BUILD, "New Describer Alert", q);
            sprintf(q, "INSERT INTO describers (id, last_timestamp, control_mode_cmd, control_mode, no_sig_address, process_mode, description) VALUES ('%s', 0, 0, 0, %d, 2, 'New %s')", area_id, SIG_BYTES, time_text(time(NULL), false));
            db_query(q);
            reload_describers();
            stats[NewDesc]++;
         }
         
         size_t message_ends = tokens[index].end;
         do index++; 
         while(tokens[index].start < message_ends && tokens[index].start >= 0 && index < NUM_TOKENS);
      }
   }
   elapsed = time_ms() - elapsed;
   if(debug || elapsed > 2500)
   {
      _log(MINOR, "Frame took %s ms to process.", commas_q(elapsed));
   }
}

static void process_message(const word describer, const char * const body, const size_t index)
{
   char message_type[8];
   char times[16];
   time_t timestamp;
   char from[16], to[16], descr[16], wasf[16], wast[16];

   jsmn_find_extract_token(body, tokens, index, "msg_type", message_type, sizeof(message_type));
   _log(DEBUG, "Message name = \"%s\".", message_type);
   jsmn_find_extract_token(body, tokens, index, "time", times, sizeof(times));
   times[10] = '\0';
   timestamp = atol(times);

   time_t now = time(NULL);
   // Handle Network Rail's midnight bug
   if(timestamp > now + (23*60*60))
   {
      _log(MINOR, "Midnight bug detected:  Describer %s (%s) received timestamp %s.", describers[describer].id, describers[describer].description, time_text(timestamp, false));
      timestamp -= (24*60*60);
      _log(MINOR, "   Corrected timestamp to %s.", time_text(timestamp, false));
   }
   if(timestamp < now - (23*60*60))
   {
      _log(MINOR, "Midnight bug detected:  Describer %s (%s) received timestamp %s.", describers[describer].id, describers[describer].description, time_text(timestamp, false));
      timestamp += (24*60*60);
      _log(MINOR, "   Corrected timestamp to %s.", time_text(timestamp, false));
   }

   describers[describer].last_td_processed = now;

   if(((describers[describer].status_last_td_actual + 8) < timestamp) || ((status_last_td_processed + 8) < now))
   {
       describers[describer].status_last_td_actual = timestamp;
       status_last_td_processed = now;
       char query[256];
       sprintf(query, "update status SET last_td_processed = %ld", now);
       db_query(query);
       sprintf(query, "update describers set last_timestamp = %ld WHERE id = '%s'", timestamp, describers[describer].id);
       db_query(query);
   }

   if(!strcasecmp(message_type, "CA"))
   {
      jsmn_find_extract_token(body, tokens, index, "from", from, sizeof(from));
      jsmn_find_extract_token(body, tokens, index, "to", to, sizeof(to));
      jsmn_find_extract_token(body, tokens, index, "descr", descr, sizeof(descr));
      if(describers[describer].process_mode == 2) 
      {
         strcpy(wasf, query_berth(describer, from));
         strcpy(wast, query_berth(describer, to));
         _log(DEBUG, "%s CA:               Berth step (%s) Description \"%s\" from berth \"%s\" to berth \"%s\"", describers[describer].id, time_text(timestamp, true), descr, from, to);
         log_detail(timestamp, "%s CA: %s from %s to %s %s", describers[describer].id, descr, from, to, show_signalling_state(describer));
         if(strcmp(wasf, descr) || strcmp(wast, ""))
         {
            _log(DEBUG, "Message %s CA: Step \"%s\" from \"%s\" to \"%s\" found \"%s\" in \"%s\" and \"%s\" in \"%s\".", describers[describer].id, descr, from, to, wasf, from, wast, to);
         }
      }
      update_database_berth(describer, from, "");
      update_database_berth(describer, to, descr);
      // Smart berth
      if(describer == describer_M0 && BERTH_CMP_C(from,'E','2','9','9') && BERTH_CMP_C(to,'C','O','U','T'))
      {
         update_database_berth(describer, "ZZ51", descr);
      }
      else if(describer == describer_M0 && BERTH_CMP_C(from,'S','T','I','N') && BERTH_CMP_C(to,'E','0','3','9'))
      {
         update_database_berth(describer, "ZZ51", "");
      }
      stats[CA]++;
   }
   else if(!strcasecmp(message_type, "CB"))
   {
      jsmn_find_extract_token(body, tokens, index, "from", from, sizeof(from));
      if(describers[describer].process_mode == 2) 
      {
         jsmn_find_extract_token(body, tokens, index, "descr", descr, sizeof(descr));
         strcpy(wasf, query_berth(describer, from));
         _log(DEBUG, "%s CB:             Berth cancel (%s) Description \"%s\" from berth \"%s\"", describers[describer].id, time_text(timestamp, true), descr, from);
         log_detail(timestamp, "%s CB: %s from %s         %s", describers[describer].id, descr, from, show_signalling_state(describer));
         if(strcmp(wasf, descr))
         {
            _log(DEBUG, "Message %s CB: Cancel \"%s\" from \"%s\" found \"%s\" in \"%s\".", describers[describer].id, descr, from, wasf, from);
         }
      }
      update_database_berth(describer, from, "");
      stats[CB]++;
   }
   else if(!strcasecmp(message_type, "CC"))
   {
      jsmn_find_extract_token(body, tokens, index, "to", to, sizeof(to));
      jsmn_find_extract_token(body, tokens, index, "descr", descr, sizeof(descr));
      if(describers[describer].process_mode == 2) 
      {
         _log(DEBUG, "%s CC:          Berth interpose (%s) Description \"%s\" to berth \"%s\"", describers[describer].id, time_text(timestamp, true), descr, to);
         log_detail(timestamp, "%s CC: %s           to %s %s", describers[describer].id, descr, to, show_signalling_state(describer));
      }
      update_database_berth(describer, to, descr);
      stats[CC]++;
   }
   else if(!strcasecmp(message_type, "CT"))
   {
      // Heartbeat
      if(describers[describer].process_mode == 2) 
      {
         char report_time[16];
         jsmn_find_extract_token(body, tokens, index, "report_time", report_time, sizeof(report_time));
         _log(DEBUG, "%s CT:                  Heartbeat (%s) Report time = %s", describers[describer].id, time_text(timestamp, true), report_time);
         log_detail(timestamp, "%s CT: Heartbeat, report time %s", describers[describer].id, report_time);
      }
      stats[CT]++;
   }
   else if(!strcasecmp(message_type, "SF"))
   {
      char address[16], data[32];
      jsmn_find_extract_token(body, tokens, index, "address", address, sizeof(address));
      jsmn_find_extract_token(body, tokens, index, "data", data, sizeof(data));
      _log(DEBUG, "%s SF:        Signalling update (%s) Address \"%s\", data \"%s\"", describers[describer].id, time_text(timestamp, true), address, data);

      word  a = strtoul(address, NULL, 16);
      dword d = strtoul(data,    NULL, 16);
      signalling_update("SF", describer, timestamp, a, d);

      stats[SF]++;
   }
   else if(!strcasecmp(message_type, "SG"))
   {
      char address[16], data[32];
      jsmn_find_extract_token(body, tokens, index, "address", address, sizeof(address));
      jsmn_find_extract_token(body, tokens, index, "data", data, sizeof(data));
      _log(DEBUG, "%s SG:       Signalling refresh (%s) Address \"%s\", data \"%s\"", describers[describer].id, time_text(timestamp, true), address, data);
      word  a = strtoul(address, NULL, 16);
      dword d = strtoul(data,    NULL, 16);
      _log(DEBUG, "a = %04x, d = %08x", a, d);
      signalling_update("SG", describer, timestamp, a     , 0xff & (d >> 24));
      signalling_update("SG", describer, timestamp, a + 1 , 0xff & (d >> 16));
      signalling_update("SG", describer, timestamp, a + 2 , 0xff & (d >> 8 ));
      signalling_update("SG", describer, timestamp, a + 3 , 0xff & (d      ));

      stats[SG]++;
   }
   else if(!strcasecmp(message_type, "SH"))
   {
      char address[16], data[32];
      jsmn_find_extract_token(body, tokens, index, "address", address, sizeof(address));
      jsmn_find_extract_token(body, tokens, index, "data", data, sizeof(data));
      _log(DEBUG, "%s SH: Signalling refresh final (%s) Address \"%s\", data \"%s\"", describers[describer].id, time_text(timestamp, true), address, data);
      word  a = strtoul(address, NULL, 16);
      dword d = strtoul(data,    NULL, 16);
      _log(DEBUG, "a = %04x, d = %08x", a, d);
      signalling_update("SH", describer, timestamp, a     , 0xff & (d >> 24));
      signalling_update("SH", describer, timestamp, a + 1 , 0xff & (d >> 16));
      signalling_update("SH", describer, timestamp, a + 2 , 0xff & (d >> 8 ));
      signalling_update("SH", describer, timestamp, a + 3 , 0xff & (d      ));
      if(debug) _log(DEBUG, show_signalling_state(describer));
      
      stats[SH]++;
   }
   else
   {
      _log(GENERAL, "Unrecognised message type \"%s\":", message_type);
      jsmn_dump_tokens(body, tokens, index);
   }
}

static void signalling_update(const char * const message_name, const word describer, const time_t t, const word a, const dword d)
{
   char detail[256], detail1[256];

   _log(PROC, "signalling_update(\"%s\", %d, %02x, %08x)", message_name, describer, a, d);

   detail[0] = '\0';
   if(a < SIG_BYTES)
   {
      dword o = signalling[describer][a];
      signalling[describer][a] = d;
      char signal_key[8], signal_value[8];
      sprintf(signal_key, "%02x", a);
      sprintf(signal_value, "%d", d);
      update_database(Signal, describer, signal_key, signal_value);

      word b, changed;
      changed = false;
      for(b=0; describers[describer].process_mode == 2 && b<8; b++)
      {
         if((((o>>b)&0x01) != ((d>>b)&0x01)) || o > 0xff)
         {
            sprintf(detail1, " Bit %02x %d = %u", a, b, ((d>>b)&0x01));
            strcat(detail, detail1);
            changed = true;
         }
      }
      if(!changed) strcat(detail, "  No change");
      if(!(a < describers[describer].no_sig_address))
      {
         _log(MINOR, "Signalling address %04x out of expected range in %s message from describer %s (%s).  Data %02x.", a, message_name, describers[describer].id, describers[describer].description, d);
      }
   }
   else
   {
      _log(MAJOR, "Signalling address %04x invalid in %s message from describer %s (%s).  Data %02x.", a, message_name, describers[describer].id, describers[describer].description, d);
   }
   if(debug) _log(DEBUG, show_signalling_state(describer));
   if(describers[describer].process_mode == 2) log_detail(t, "%s %s: %02x = %02x                %s%s", describers[describer].id, message_name, a, d, show_signalling_state(describer), detail);
}

static void update_database_berth(const word describer, const char * const k, const char * const v)
{
   update_database(Berth, describer, k, v);

   if(describer != describer_M0 && describer != describer_M1) return;

   // Process OR smart berths
   {
      word i;
      for (i=0; i < SMART_ORS; i++)
      {
         if(BERTH_CMP(k, smart_or[i].bertha) && describer == describer_M0)
         {
            strcpy(smart_or[i].berthav, v);
            if(smart_or[i].berthav[0])
            {
               update_database(Berth, describer, smart_or[i].smart_berth, smart_or[i].berthav);
            }
            else
            {
               update_database(Berth, describer, smart_or[i].smart_berth, smart_or[i].berthbv);
            }
         }
         else if(BERTH_CMP(k, smart_or[i].berthb) && describer == (smart_or[i].berthb_m1?describer_M1:describer_M0))
         {
            strcpy(smart_or[i].berthbv, v);
            if(smart_or[i].berthbv[0]) 
            {
               update_database(Berth, describer_M0, smart_or[i].smart_berth, smart_or[i].berthbv);
            }
            else
            {
               update_database(Berth, describer_M0, smart_or[i].smart_berth, smart_or[i].berthav);
            }
         }
      }
   }
}

static void update_database(const word type, const word describer, const char * const b, const char * const v)
{
   char query[512], typec, vv[8];
   MYSQL_RES * result;
   MYSQL_ROW row;
   time_t now = time(NULL);

   _log(PROC, "update_database(%d, %d, \"%s\", \"%s\")", type, describer, b, v);
   if(strlen(v) > 6)
   {
      _log(MAJOR, "Database update discarded.  Overlong value \"%s\".", v);
      return;
   }
   strcpy(vv, v);
   if(type == Berth) 
   {
      typec = 'b';
      if(v[0])
      {
         sprintf(query, "SELECT true_hc FROM obfus_lookup where obfus_hc = '%s' ORDER BY created DESC LIMIT 1", v);
         if(!db_query(query))
         {
            result = db_store_result();
            if((row = mysql_fetch_row(result))) 
            {
               strcpy(vv, row[0]);
               _log(DEBUG, "De-obfuscating \"%s\" to \"%s\".", v, vv);
            }
            mysql_free_result(result);
         }
      }
   }
   else
   {
      typec = 's';
   }

   if(describers[describer].control_mode != 2)
   {
      if(++handle > MAX_HANDLE)
      {
         handle = 1;
         // n.b. Do not use TRUNCATE TABLE as that cannot be rolled back.
         db_query("delete from td_updates");
         stats[HandleWrap]++;
      }
      else
      {
         sprintf(query, "delete from td_updates where k = '%s%c%s'", describers[describer].id, typec, b);
         db_query(query);
      }

      sprintf(query, "INSERT INTO td_updates values(%ld, %d, '%s%c%s', '%s')", now, handle, describers[describer].id, typec, b, vv);
      db_query(query);
   }

   if(describers[describer].control_mode == 2) typec++;

   sprintf(query, "UPDATE td_states SET updated = %ld, v = '%s' where k = '%s%c%s'", now, vv, describers[describer].id, typec, b);
   if(!db_query(query))
   {
      if(!db_affected_rows())
      {
         sprintf(query, "INSERT INTO td_states VALUES(%ld, '%s%c%s', '%s')", now, describers[describer].id, typec, b, vv);
         db_query(query);
         if(describers[describer].control_mode != 2)
         {
            char report[1024];
            sprintf(report, "Added new %s \"%s\", value \"%s\", on describer %s (%s) to database.", ((typec == 'b')?"berth":"S address"), b, v, describers[describer].id, describers[describer].description);
            _log(MINOR, report);
            if(*conf[conf_tddb_report_new]) email_alert(NAME, BUILD, "New Key Alert", report);
            stats[NewKey]++;
         }
      }
   }
}

static const char * const query_berth(const word describer, const char * const b)
{
   char query[512];
   MYSQL_RES * result;
   MYSQL_ROW row;
   static char reply[32];

   strcpy(reply, "");

   sprintf(query, "SELECT * FROM td_states where k = '%sb%s'", describers[describer].id, b);
   if(!db_query(query))
   {
      result = db_store_result();
      if((row = mysql_fetch_row(result))) 
      {
         strcpy(reply,row[2]);
      }
      mysql_free_result(result);
   }
   return reply;
}

static void create_database(void)
{
   _log(PROC, "create_database()");
   
   word e;
   if((e = database_upgrade(tddb)))
   {
      _log(CRITICAL, "Error %d in database_upgrade().  Aborting.", e);
      exit(1);
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

static void report_stats(void)
{
   char zs[512];
   word i;
   char report[8192];

   _log(GENERAL, "");
   sprintf(zs, "%25s: %-12s Total", "", "Day");
   _log(GENERAL, zs);
   strcpy(report, zs);
   strcat(report, "\n");

   sprintf(zs, "%25s: %-12s %ld days", "Run time", "", (time(NULL) - start_time)/(24*60*60));
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
   {
      word monitored = 0;
      for(i=0; i < no_describers; i++)
      {
         if(describers[i].id[0] && describers[i].process_mode) monitored++;
      }
      sprintf(zs, "Currently processing data from %d of the %d describers listed.", monitored, no_describers);
      _log(GENERAL, zs);
      strcat(report, "\n");
      strcat(report, zs);
      strcat(report, "\n");

      monitored = 0;
      for(i=0; i < no_describers; i++)
      {
         if(describers[i].id[0] && describers[i].process_mode && describers[i].timeout_reported && strlen(report) < sizeof(report) - 512)
         {
            if(!monitored)
            {
               monitored++;
               sprintf(zs, "Data feed timeout has been reported for the following describers:"); 
               _log(GENERAL, zs);
               strcat(report, zs);
               strcat(report, "\n");
            }
            sprintf(zs, "   %s %s", describers[i].id, describers[i].description);
            _log(GENERAL, zs);
            strcat(report, zs);
            strcat(report, "\n");
         }
      }
   }

   email_alert(NAME, BUILD, "Statistics Report", report);
   _log(GENERAL, "");
}

static const char * show_signalling_state(const word describer)
{
   word i;
   char s[8];
   static char  state[128];

   if(describer[describers].no_sig_address > 64)
   {
      state[0] = '\0';
      return state;
   }
   strcpy(state, "[");
   for(i = 0; i < describer[describers].no_sig_address; i++)
   {
      if(i) strcat(state, " ");
      if(signalling[describer][i] > 0xff)
      {
         strcat(state, "..");
      }
      else
      {
         sprintf(s, "%02x", signalling[describer][i]);
         strcat(state, s);
      }
   }
   strcat(state, "]");
   return state;
}

static void log_detail(const time_t stamp, const char * text, ...)
{
   FILE * fp;

   va_list vargs;
   va_start(vargs, text);

   time_t now = time(NULL);
      
   if((fp = fopen(debug?"/tmp/tddb-detail.log":"/var/log/garner/tddb-detail.log", "a")))
   {
      fprintf(fp, "%s ] ", time_text(now, false));
      fprintf(fp, "(Timestamp %s) ", time_text(stamp, false));
      vfprintf(fp, text, vargs);
      fprintf(fp, "\n");
      fclose(fp);
   }

   va_end(vargs);
   return;
}

static void check_timeout(void)
{
   word describer;
   MYSQL_RES * result;
   MYSQL_ROW row;
   char report[512];

   time_t now = time(NULL);
   if(now > check_describers_flow_due)
   {   
      check_describers_flow_due = now + CHECK_DESCRIBERS_FLOW_INTERVAL;
      if(no_feed) 
      {
         no_feed--;
      }
      else
      {
         _log(DEBUG, "Checking describer message streams...");
         for(describer = 0; describer < no_describers; describer++)
         {
            if(describers[describer].process_mode && describers[describer].last_td_processed)
            {
               if(now - describers[describer].last_td_processed > DESCRIBER_FEED_ALARM_THRESHOLD)
               {
                  // Timeout
                  if(!describers[describer].timeout_reported)
                  {
                     sprintf(report, "Describer %s (%s) message stream - Receive timeout.", describers[describer].id, describers[describer].description);
                     _log(MAJOR, report);
                     email_alert(NAME, BUILD, "Describer Stream Alarm", report);
                     describers[describer].timeout_reported = true;
                  }
               }
               else
               {
                  if(describers[describer].timeout_reported)
                  {
                     describers[describer].timeout_reported = false;
                     sprintf(report, "Describer %s (%s) message stream - Receive OK.", describers[describer].id, describers[describer].description);
                     _log(MINOR, report);
                     email_alert(NAME, BUILD, "Describer Stream Alarm Cleared", report);
                  }
               }
            }
         }
      }
   }

   if(now > last_reload_describers_check + RELOAD_DESCRIBERS_CHECK_INTERVAL)
   {
      last_reload_describers_check = now;
      if(!db_query("SELECT control_mode_cmd FROM describers WHERE id = ''"))
      {
         if((result = db_store_result()))
         {
            if((row = mysql_fetch_row(result))) 
            {
               if(row[0][0] != '0')
               {
                  reload_describers();
               }
            }
            mysql_free_result(result);
         }
      }
   }
}

static void control_mode_change(const word d, const word n)
{
   MYSQL_RES * result;
   MYSQL_ROW row;
   char q[512];
   char other_k[16];
   word j;

   _log(GENERAL, "   Mode change from %d to %d on describer %s (%s).", describers[d].control_mode, n, describers[d].id,  describers[d].description);
   if(describers[d].control_mode == 2)
   {
      _log(GENERAL, "      Turning off blank mode and restoring states.");
      // Turn off blank mode
      // 1. Set mode so that update_database points to the live records.
      describers[d].control_mode = n;
      // 2. Retrieve hidden records and make them live
      sprintf(q, "SELECT updated, k, v FROM td_states WHERE substring(k,1,2) = '%s' AND v != '' AND (substring(k,3,1) = 'c' OR substring(k,3,1) = 't')", describers[d].id);
      if(!db_query(q))
      {
         result = db_store_result();
         while((row = mysql_fetch_row(result))) 
         {
            strcpy(other_k, row[1]);
            other_k[2]--;
            if(other_k[2] == 'b')
            {
               update_database_berth(d, other_k+3, row[2]);
            }
            else if(other_k[2] == 's')
            {
               update_database(Signal, d, other_k+3, row[2]);
            }
         }
         mysql_free_result(result);
      }
      // 3. Delete all hidden records
      sprintf(q, "DELETE FROM td_states WHERE (substring(k,3,1) = 'c' OR substring(k,3,1) = 't') AND substring(k,1,2) = '%s'", describers[d].id);
      db_query(q);
   }
   switch(n)
   {
   case 0:
      describers[d].control_mode = 0;
      sprintf(q, "UPDATE describers SET control_mode_cmd = 0, control_mode = 0 WHERE id = '%s'", describers[d].id);
      db_query(q);
      break;

   case 1:
      // Clear all berths
      _log(GENERAL, "      Clearing all berth data and reverting to mode 0.");
      sprintf(q, "SELECT k FROM td_states WHERE v != '' AND substring(k,1,2) = '%s'", describers[d].id);
      if(!db_query(q))
      {
         result = db_store_result();
         while((row = mysql_fetch_row(result))) 
         {
            if(row[0][2] == 'b')
            {
               update_database_berth(d, row[0]+3, "");
            }
            else if(row[0][2] == 's')
            {
               update_database(Signal, d, row[0]+3, "");
            }
         }
         mysql_free_result(result);
      }

      // Clear locally stored signal states
      for(j = 0; j < SIG_BYTES; j++)
      {
         signalling[d][j] = 0xffff;
      }
      
      // Turn it off.
      describers[d].control_mode = 0;
      sprintf(q, "UPDATE describers SET control_mode_cmd = 0, control_mode = 0 WHERE id = '%s'", describers[d].id);
      db_query(q);
      break;

   case 2:
      // Turn on blank mode
      _log(GENERAL, "      Turning on blank mode, retaining states in database.");
      // 1. Delete all hidden records.
      sprintf(q, "DELETE FROM td_states WHERE (substring(k,3,1) = 'c' OR substring(k,3,1) = 't') AND substring(k,1,2) = '%s'", describers[d].id);
      // 2. Copy live records to hidden and blank live ones.
      sprintf(q, "SELECT updated, k, v FROM td_states WHERE substring(k,1,2) = '%s' AND (substring(k,3,1) = 'b' OR substring(k,3,1) = 's')", describers[d].id);
      if(!db_query(q))
      {
         result = db_store_result();
         while((row = mysql_fetch_row(result))) 
         {
            strcpy(other_k, row[1]);
            other_k[2]++;
            sprintf(q, "INSERT INTO td_states (updated, k, v) VALUES(%s, '%s', '%s')", row[0], other_k, row[2]);
            db_query(q);
            if(row[2][0])
            {
               if(row[1][2] == 'b')
               {
                  update_database_berth(d, row[1]+3, "");
               }
               else if(row[1][2] == 's')
               {
                  update_database(Signal, d, row[1]+3, "");
               }
            }
         }
         mysql_free_result(result);
      }
      // Set mode so that future updates will go to the hidden records.
      describers[d].control_mode = 2;
      sprintf(q, "UPDATE describers SET control_mode_cmd = 2, control_mode = 2 WHERE id = '%s'", describers[d].id);
      db_query(q);
      break;
   }
}

static void reload_describers(void)
{
   MYSQL_RES * result;
   MYSQL_ROW row;
   word new_describers;
   word list_changed = false;
   word new_control_mode, i, j;

   _log(GENERAL, "Loading describer data ...");

   for(new_describers = 0; new_describers < 128; new_describers++) search_helper[new_describers] = DESCRIBERS;

   db_start_transaction();
   new_describers = 0;
   //                   0   1               2                 3             4               5             6
   if(!db_query("SELECT id, last_timestamp, control_mode_cmd, control_mode, no_sig_address, process_mode, description FROM describers ORDER BY id"))
   {
      if((result = db_store_result()))
      {
         while((row = mysql_fetch_row(result))) 
         {
            _log(DEBUG, "   id = \"%s\", new_describers = %d", row[0], new_describers);
            if(new_describers >= DESCRIBERS)
            {
               _log(MAJOR, "   Describer %s discarded.  Table is full.", row[0]);
            }
            else if(row[0][0])
            {
               if(new_describers >= no_describers || strcmp(row[0], describers[new_describers].id))
               {
                  list_changed = true;
               }
               strcpy(describers[new_describers].id, row[0]);
               if(search_helper[row[0][0] & 0x7f] > new_describers) search_helper[row[0][0] & 0x7f] = new_describers;
               describers[new_describers].control_mode = atoi(row[3]);
               new_control_mode = atoi(row[2]);
               if(!list_changed && describers[new_describers].control_mode != new_control_mode) control_mode_change(new_describers, new_control_mode);
               describers[new_describers].no_sig_address = atoi(row[4]);
               if(describers[new_describers].no_sig_address > SIG_BYTES) describers[new_describers].no_sig_address=SIG_BYTES;
               describers[new_describers].process_mode   = atoi(row[5]);
               strcpy(describers[new_describers].description, row[6]);
               if(!strcmp("M0", row[0])) describer_M0 = new_describers;
               if(!strcmp("M1", row[0])) describer_M1 = new_describers;
               new_describers++;
            }
         }
      }
      mysql_free_result(result);
      no_describers = new_describers;
      _log(GENERAL, "   Loaded %d describers.", no_describers);
      if(list_changed)
      {
         _log(GENERAL, "   Describer list has changed.  Resetting data.");
         for(i = 0; i < no_describers; i++)
         {
            describers[i].timeout_reported = 0;
            describers[i].last_td_processed = 0;
            describers[i].status_last_td_actual = 0;
            for(j = 0; j < SIG_BYTES; j++)
            {
               signalling[i][j] = 0xffff;
            }
         }
      }
      else
      {
         // If we got a new describer we may not have processed all control mode commands, so leave them for next time.
         db_query("UPDATE describers SET control_mode_cmd = 0 WHERE id = ''");
      }
      db_commit_transaction();
   }

   // Give any added describers time to get some messages before complaining
   no_feed = NO_FEED_LOCKOUT;
}
