/*
    Copyright (C) 2014, 2015, 2016 Phil Wieland

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

#define NAME  "tddb"
#define BUILD "X328"

static void perform(void);
static void process_frame(const char * const body);
static void process_message(const word describer, const char * const body, const size_t index);
static void signalling_update(const char * const message_name, const word describer, const time_t t, const word a, const dword d);
static void update_database(const word type, const word describer, const char * const k, const char * const v);
static const char * const query_berth(const word describer, const char * const k); 
static void create_database(void);

static void jsmn_dump_tokens(const char * const string, const jsmntok_t * const tokens, const word object_index);
static void report_stats(void);
static const char * show_signalling_state(const word describer);
static void log_detail(const time_t stamp, const char * text, ...);
static void check_timeout(void);

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

static time_t start_time;

// Describers 
#define DESCRIBERS 3
static const char * describers[DESCRIBERS] = {"M1", "XZ", "WA"};

// Status
static time_t status_last_td_processed;
static word timeout_reported[DESCRIBERS];
static time_t last_td_processed[DESCRIBERS], status_last_td_actual[DESCRIBERS];

enum data_types {Berth, Signal};

// Stats
enum stats_categories {ConnectAttempt, GoodMessage, RelMessage, CA, CB, CC, CT, SF, SG, SH, NewBerth, NotRecog, HandleWrap, MAXstats};
static qword stats[MAXstats];
static qword grand_stats[MAXstats];
static const char * stats_category[MAXstats] = 
   {
      "Stompy connect attempt", "Good message", 
      "Relevant message", "CA message", "CB message", "CC message", "CT message", "SF message", "SG message", "SH message", "New berth", "Unrecognised message", "Handle wrap",
   };

// Signalling
#define SIG_BYTES 256
static word no_of_sig_address[DESCRIBERS] = {8, 0, 15};
static word signalling[DESCRIBERS][SIG_BYTES];

// Update handle.  MAX_HANDLE must be < 32767
#define MAX_HANDLE 9990
static word handle;

// Message count
word message_count, message_count_rel;
time_t last_message_count_report;
#define MESSAGE_COUNT_REPORT_INTERVAL 64

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
      while(run && !debug && (sysinfo(&info) || info.uptime < (512 + 64)))
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
   word last_report_day = 9;
   word stompy_timeout = true;

   // Initialise database connection
   while(db_init(conf[conf_db_server], conf[conf_db_user], conf[conf_db_password], conf[conf_db_name]) && run) 
   {
      _log(CRITICAL, "Failed to initialise database connection.  Will retry...");
      word i;
      for(i = 0; i < 64 && run; i++) sleep(1);
   }

   create_database();

   handle = 0xfff0;

   {
      time_t now = time(NULL);
      struct tm * broken = localtime(&now);
      if(broken->tm_hour >= REPORT_HOUR)
      {
         last_report_day = broken->tm_wday;
      }
      last_message_count_report = now;
      message_count = message_count_rel = 0;
   }

   // Status
   status_last_td_processed = 0;
   {
      word describer;
      for(describer = 0; describer < DESCRIBERS; describer++)
      {
         status_last_td_actual[describer] = last_td_processed[describer] = 0;
         timeout_reported[describer] = false;
      }
   }

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
            if(broken->tm_hour >= REPORT_HOUR && broken->tm_wday != last_report_day)
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
               stompy_timeout = true;
            }
         }
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
         word describer;
         jsmn_find_extract_token(body, tokens, index, "area_id", area_id, sizeof(area_id));

         stats[GoodMessage]++;
         message_count++;

         for(describer = 0; describer < DESCRIBERS; describer++)
         {
            if(!strcasecmp(area_id, describers[describer]))
            {
               process_message(describer, body, index);
               stats[RelMessage]++;
               message_count_rel++;
               describer = DESCRIBERS;
            }
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

   last_td_processed[describer] = now;

   if(((status_last_td_actual[describer] + 8) < timestamp) || ((status_last_td_processed + 8) < now))
   {
       status_last_td_actual[describer] = timestamp;
       status_last_td_processed = now;
       char query[256];
       sprintf(query, "update status SET last_td_processed = %ld", now);
       db_query(query);
       sprintf(query, "update td_status set last_timestamp = %ld WHERE d = '%s'", timestamp, describers[describer]);
       db_query(query);
   }

   if(!strcasecmp(message_type, "CA"))
   {
      jsmn_find_extract_token(body, tokens, index, "from", from, sizeof(from));
      jsmn_find_extract_token(body, tokens, index, "to", to, sizeof(to));
      jsmn_find_extract_token(body, tokens, index, "descr", descr, sizeof(descr));
      strcpy(wasf, query_berth(describer, from));
      strcpy(wast, query_berth(describer, to));
      _log(DEBUG, "%s CA:               Berth step (%s) Description \"%s\" from berth \"%s\" to berth \"%s\"", describers[describer], time_text(timestamp, true), descr, from, to);
      log_detail(timestamp, "%s CA: %s from %s to %s", describers[describer], descr, from, to);
      if(strcmp(wasf, descr) || strcmp(wast, ""))
      {
         _log(DEBUG, "Message %s CA: Step \"%s\" from \"%s\" to \"%s\" found \"%s\" in \"%s\" and \"%s\" in \"%s\".", describers[describer], descr, from, to, wasf, from, wast, to);
      }
      update_database(Berth, describer, from, "");
      update_database(Berth, describer, to, descr);
      stats[CA]++;
   }
   else if(!strcasecmp(message_type, "CB"))
   {
      jsmn_find_extract_token(body, tokens, index, "from", from, sizeof(from));
      jsmn_find_extract_token(body, tokens, index, "descr", descr, sizeof(descr));
      strcpy(wasf, query_berth(describer, from));
      _log(DEBUG, "%s CB:             Berth cancel (%s) Description \"%s\" from berth \"%s\"", describers[describer], time_text(timestamp, true), descr, from);
      log_detail(timestamp, "%s CB: %s from %s", describers[describer], descr, from);
      if(strcmp(wasf, descr))
      {
         _log(DEBUG, "Message %s CB: Cancel \"%s\" from \"%s\" found \"%s\" in \"%s\".", describers[describer], descr, from, wasf, from);
      }
      update_database(Berth, describer, from, "");
      stats[CB]++;
   }
   else if(!strcasecmp(message_type, "CC"))
   {
      jsmn_find_extract_token(body, tokens, index, "to", to, sizeof(to));
      jsmn_find_extract_token(body, tokens, index, "descr", descr, sizeof(descr));
      _log(DEBUG, "%s CC:          Berth interpose (%s) Description \"%s\" to berth \"%s\"", describers[describer], time_text(timestamp, true), descr, to);
      log_detail(timestamp, "%s CC: %s           to %s", describers[describer], descr, to);
      update_database(Berth, describer, to, descr);
      stats[CC]++;
   }
   else if(!strcasecmp(message_type, "CT"))
   {
      // Heartbeat
      char report_time[16];
   
      jsmn_find_extract_token(body, tokens, index, "report_time", report_time, sizeof(report_time));
      _log(DEBUG, "%s CT:                  Heartbeat (%s) Report time = %s", describers[describer], time_text(timestamp, true), report_time);
      stats[CT]++;
   }
   else if(!strcasecmp(message_type, "SF"))
   {
      char address[16], data[32];
      jsmn_find_extract_token(body, tokens, index, "address", address, sizeof(address));
      jsmn_find_extract_token(body, tokens, index, "data", data, sizeof(data));
      _log(DEBUG, "%s SF:        Signalling update (%s) Address \"%s\", data \"%s\"", describers[describer], time_text(timestamp, true), address, data);

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
      _log(DEBUG, "%s SG:       Signalling refresh (%s) Address \"%s\", data \"%s\"", describers[describer], time_text(timestamp, true), address, data);
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
      _log(DEBUG, "%s SH: Signalling refresh final (%s) Address \"%s\", data \"%s\"", describers[describer], time_text(timestamp, true), address, data);
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
   if(a < SIG_BYTES && a < no_of_sig_address[describer])
   {
      dword o = signalling[describer][a];
      signalling[describer][a] = d;

      word b;
      for(b=0; b<8; b++)
      {
         if((((o>>b)&0x01) != ((d>>b)&0x01)) || o > 0xff)
         {
            sprintf(detail1, "  Bit %02x %d = %ld", a, b, ((d>>b)&0x01));
            strcat(detail, detail1);
            char signal_key[8];
            sprintf(signal_key, "%02x%d", a, b);
            update_database(Signal, describer, signal_key, ((d>>b)&0x01)?"1":"0");
         }
      }
   }
   else
   {
      _log(MINOR, "Signalling address %04x out of range in %s message from describer %s.  Data %08x", a, message_name, describers[describer], d);
   }
   if(debug) _log(DEBUG, show_signalling_state(describer));
   log_detail(t, "%s %s: %02x = %02x %s%s", describers[describer], message_name, a, d, show_signalling_state(describer), detail);
}

static void update_database(const word type, const word describer, const char * const b, const char * const v)
{
   char query[512], typec, vv[8];
   MYSQL_RES * result;
   MYSQL_ROW row;
   time_t now = time(NULL);

   _log(PROC, "update_database(%d, %d, \"%s\", \"%s\")", type, describer, b, v);
   if(strlen(v) > 7)
   {
      _log(MAJOR, "Database update discarded.  Overlong value \"%s\".", v);
      return;
   }
   strcpy(vv, v);
   if(type == Berth) 
   {
      typec = 'b';
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
   else
   {
      typec = 's';
   }

   if(++handle > MAX_HANDLE)
   {
      handle = 1;
      db_query("delete from td_updates");
      stats[HandleWrap]++;
   }
   else
   {
      sprintf(query, "delete from td_updates where k = '%s%c%s'", describers[describer], typec, b);
      db_query(query);
   }

   sprintf(query, "INSERT INTO td_updates values(%ld, %d, '%s%c%s', '%s')", now, handle, describers[describer], typec, b, vv);
   db_query(query);

   sprintf(query, "SELECT * FROM td_states where k = '%s%c%s'", describers[describer], typec, b);
   if(!db_query(query))
   {
      result = db_store_result();
      if((row = mysql_fetch_row(result))) 
      {
         sprintf(query, "UPDATE td_states SET updated = %ld, v = '%s' where k = '%s%c%s'", now, vv, describers[describer], typec, b);
      }
      else
      {
         sprintf(query, "INSERT INTO td_states VALUES(%ld, '%s%c%s', '%s')", now, describers[describer], typec, b, vv);
         if(type == Berth)
         {
            _log(MINOR, "Added new berth \"%s\" on describer %s to database.", b, describers[describer]);
            stats[NewBerth]++;
         }
      }
      mysql_free_result(result);
      db_query(query);
   }
}

static const char * const query_berth(const word describer, const char * const b)
{
   char query[512];
   MYSQL_RES * result;
   MYSQL_ROW row;
   static char reply[32];

   strcpy(reply, "");

   sprintf(query, "SELECT * FROM td_states where k = '%sb%s'", describers[describer], b);
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
   MYSQL_RES * result0;
   char query[1024];

   _log(PROC, "create_database()");

   database_upgrade(tddb);

   // Ensure the rows are all present in td_status
   word d;
   for(d=0; d < DESCRIBERS; d++)
   {
      sprintf(query, "SELECT * FROM td_status WHERE d = '%s'", describers[d]);
      if(!db_query(query))
      {
         result0 = db_store_result();
         if(!mysql_num_rows(result0))
         {
            sprintf(query, "INSERT INTO td_status VALUES('%s', 0)", describers[d]);
            db_query(query);
            _log(GENERAL, "Created td_status row for describer \"%s\".", describers[d]);
         }
         mysql_free_result(result0);
      }
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
   char zs[128];
   word i;
   char report[2048];

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
   email_alert(NAME, BUILD, "Statistics Report", report);
}

static const char * show_signalling_state(const word describer)
{
   word i;
   char s[8];
   static char  state[128];

   if(no_of_sig_address[describer] > 16)
   {
      state[0] = '\0';
      return state;
   }
   strcpy(state, "[");
   for(i = 0; i < no_of_sig_address[describer]; i++)
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

   for(describer = 0; describer < DESCRIBERS; describer++)
   {
      if(last_td_processed[describer] && (time(NULL) - last_td_processed[describer] > 333))
      {
         // Timeout
         if(!timeout_reported[describer])
         {
#if 1
            _log(MAJOR, "Describer %s message stream - Receive timeout.", describers[describer]);
            timeout_reported[describer] = true;
#else
            MYSQL_RES * result;
            MYSQL_ROW row;
            char q[256];
            _log(MAJOR, "Describer %s message stream - Receive timeout - Clearing database.", describers[describer]);
            timeout_reported[describer] = true;
         
            // Blank out the database
            sprintf(q, "SELECT k FROM td_states where k like '%s%%'", describers[describer]);
            if(!db_query(q))
            {
               result = db_store_result();
               while((row = mysql_fetch_row(result))) 
               {
                  update_database((row[0][2] == 'b')?Berth:Signal, describer, &row[0][3], "");
               }
               mysql_free_result(result);
            }
         
            word j;
            for(j = 0; j < SIG_BYTES; j++)
            {
               signalling[describer][j] = 0xffff;
            }
            // To spread database load, drop out after one timeout, remainder will be spotted next time.
            describer = DESCRIBERS;
#endif
         }
      }
      else
      {
         if(timeout_reported[describer])
         {
            timeout_reported[describer] = false;
            _log(MINOR, "Describer %s message stream - Receive OK.", describers[describer]);
         }
      }
   }
}
