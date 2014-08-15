/*
    Copyright (C) 2014 Phil Wieland

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

#define NAME  "tddb"
#define BUILD "V810"

static void perform(void);
static void process_frame(const char * const body);
static void process_message(const char * const body, const size_t index);
static void signalling_update(const char * const message_name, const time_t t, const word a, const dword d);
static void update_database(const word type, const char * const k, const char * const v);
static const char * const query_berth(const char * const k); 
static void create_database(void);

static void jsmn_dump_tokens(const char * const string, const jsmntok_t * const tokens, const word object_index);
static void report_stats(void);
static void log_message(const char * const message);
static const char * show_signalling_state(void);
static void log_detail(const time_t stamp, const char * text, ...);

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

// Status
static time_t status_last_td_processed, status_last_td_actual;
static time_t timeout_detect;
#define TIMEOUT_PERIOD 64

enum data_types {Berth, Signal};

// Stats
enum stats_categories {ConnectAttempt, GoodMessage, M1, CA, CB, CC, CT, SF, SG, SH, NotRecog, MAXstats};
static qword stats[MAXstats];
static qword grand_stats[MAXstats];
static const char * stats_category[MAXstats] = 
   {
      "Stompy connect attempt", "Good message", 
      "M1 message", "CA message", "CB message", "CC message", "CT message", "SF message", "SG message", "SH message", "Unrecognised message",
   };

// Signalling
#define SIG_BYTES 8
static word signalling[SIG_BYTES];

// Update handle
#define MAX_HANDLE 4095
static word handle;

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

   if(load_config(config_file_path))
   {
      printf("Failed to read config file \"%s\".\n", config_file_path);
      usage = true;
   }

   if(usage)
   {
      printf("\tUsage: %s [-c /path/to/config/file.conf]\n\n", argv[0] );
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

   // Set up log
   _log_init(debug?"/tmp/tddb.log":"/var/log/garner/tddb.log", debug?1:0);

   if(debug)
   {
      _log(DEBUG, "db_server = \"%s\"", conf.db_server);
      _log(DEBUG, "db_name = \"%s\"", conf.db_name);
      _log(DEBUG, "db_user = \"%s\"", conf.db_user);
      _log(DEBUG, "db_pass = \"%s\"", conf.db_pass);
      _log(DEBUG, "nr_user = \"%s\"", conf.nr_user);
      _log(DEBUG, "nr_pass = \"%s\"", conf.nr_pass);
      _log(DEBUG, "debug = \"%s\"", conf.debug);
   }

   // Enable core dumps
   struct rlimit limit;
   if(!getrlimit(RLIMIT_CORE, &limit))
   {
      limit.rlim_cur = RLIM_INFINITY;
      setrlimit(RLIMIT_CORE, &limit);
   }

   start_time = time(NULL);

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
   if(!debug)
   {
      _log(GENERAL, "Startup delay...");
      word i;
      for(i = 0; i < 256 && run; i++) sleep(1);
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
   struct sockaddr_in serv_addr;
   struct hostent *server;
   int rc, stompy_socket;
   word last_report_day = 9;

   // Initialise database
   while(db_init(conf.db_server, conf.db_user, conf.db_pass, conf.db_name) && run) 
   {
      _log(CRITICAL, "Failed to initialise database connection.  Will retry...");
      word i;
      for(i = 0; i < 64 && run; i++) sleep(1);
   }

   log_message("");
   log_message("");
   log_message("tddb started.");

   create_database();

   handle = 0xfff0;

   {
      time_t now = time(NULL);
      struct tm * broken = localtime(&now);
      if(broken->tm_hour >= REPORT_HOUR)
      {
         last_report_day = broken->tm_wday;
      }
   }

   // Status
   timeout_detect = status_last_td_processed = status_last_td_actual = 0;

   // Signalling
   {
      word i;
      for(i = 0; i < SIG_BYTES; i++)
      {
         signalling[i] = 0xffff;
      }
   }

   while(run)
   {   
      stats[ConnectAttempt]++;
      _log(GENERAL, "Connecting socket to stompy...");
      stompy_socket = socket(AF_INET, SOCK_STREAM, 0);
      if (stompy_socket < 0) 
      {
         _log(CRITICAL, "Failed to create client socket.  Error %d %s", errno, strerror(errno));
      }
      server = gethostbyname("localhost");
      if (server == NULL) 
      {
         _log(CRITICAL, "Failed to resolve localhost\".");
         return;
      }

      bzero((char *) &serv_addr, sizeof(serv_addr));
      serv_addr.sin_family = AF_INET;
      bcopy((char *)server->h_addr, 
            (char *)&serv_addr.sin_addr.s_addr,
            server->h_length);
      serv_addr.sin_port = htons(STOMPY_PORT);

      /* Now connect to the server */
      rc = connect(stompy_socket, &serv_addr, sizeof(serv_addr));
      if(rc)
      {
         sprintf(zs,"Failed to connect.  Error %d %s", errno, strerror(errno));
         _log(CRITICAL, zs);
      }
      else
      {
         _log(GENERAL, "Connected.  Waiting for messages...");
         holdoff = 0;
         timeout_detect = time(NULL) + TIMEOUT_PERIOD;

         int run_receive = true;
         while(run && run_receive)
         {
            {
               time_t now = time(NULL);
               struct tm * broken = localtime(&now);
               if(broken->tm_hour >= REPORT_HOUR && broken->tm_wday != last_report_day)
               {
                  last_report_day = broken->tm_wday;
                  report_stats();
               }
            }
            size_t  length = 0;
            ssize_t l = read_all(stompy_socket, &length, sizeof(size_t));
            if(l < sizeof(size_t))
            {
               if(l) _log(CRITICAL, "Failed to read message size.  Error %d %s.", errno, strerror(errno));
               else  _log(CRITICAL, "Failed to read message size.  End of file.");
               run_receive = false;
            }
            if(run && run_receive && (length < 10 || length > FRAME_SIZE))
            {
               _log(MAJOR, "Received invalid frame size 0x%08lx.", length);
               run_receive = false;
            }
            if(run && run_receive)
            {
               _log(DEBUG, "RX frame size = %ld", length);
               l = read_all(stompy_socket, body, length);
               if(l < 1)
               {
                  if(l) _log(CRITICAL, "Failed to read message body.  Error %d %s.", errno, strerror(errno));
                  else  _log(CRITICAL, "Failed to read message body.  End of file.");
                  run_receive = false;
               }
               else if(l < length)
               {
                  // Impossible?
                  _log(CRITICAL, "Only received %ld of %ld bytes of message body.", l, length);
                  run_receive = false;
               }
            }

            // Process the message
            if(run && run_receive)
            {
               process_frame(body);

               // Send ACK
               l = write(stompy_socket, "A", 1);
               if(l < 0)
               {
                  _log(CRITICAL, "Failed to write message ack.  Error %d %s", errno, strerror(errno));
                  run_receive = false;
               }

               // Check for timeout
               if(timeout_detect && time(NULL) > timeout_detect)
               {
                  // N.B.  This wont trigger if the message stream stops completely.
                  // Only if there are no M1 messages amongst a flow of other TDs.
                  // Message will repeat every ~128 seconds until an M1 message is received.
                  _log(CRITICAL, "Timeout detected.");
                  timeout_detect = time(NULL) + TIMEOUT_PERIOD;
               }
            }
         } // while(run && run_receive)
      }

      _log(GENERAL, "Disconnecting socket.");
      close(stompy_socket);
      stompy_socket = -1;
      {      
         word i;
         if(holdoff < 128) holdoff += 15;
         else holdoff = 128;
         for(i = 0; i < holdoff && run; i++) sleep(1);
      }
   }  // while(run)

   db_disconnect();

   report_stats();
}

static void process_frame(const char * const body)
{
   jsmn_parser parser;
   qword elapsed = time_ms();
   
   log_message(body);

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
         _log(DEBUG, "STOMP message is array of %d TRUST messages.", messages);
      }
      else
      {
         messages = 1;
         index = 0;
         _log(DEBUG, "STOMP message contains a single TRUST message.");
      }

      for(i=0; i < messages && run; i++)
      {
         char area_id[4];
         jsmn_find_extract_token(body, tokens, index, "area_id", area_id, sizeof(area_id));

         stats[GoodMessage]++;
         if(!strcasecmp(area_id, "M1"))
         {
            process_message(body, index);
            stats[M1]++;
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

static void process_message(const char * const body, const size_t index)
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
   timeout_detect = now + TIMEOUT_PERIOD;

   if(((status_last_td_actual + 8) < timestamp) || ((status_last_td_processed + 8) < now))
   {
       status_last_td_actual = timestamp;
       status_last_td_processed = now;
       char query[256];
       sprintf(query, "update status SET last_td_actual = %ld, last_td_processed = %ld", timestamp, now);
       db_query(query);
   }

   if(!strcasecmp(message_type, "CA"))
   {
      jsmn_find_extract_token(body, tokens, index, "from", from, sizeof(from));
      jsmn_find_extract_token(body, tokens, index, "to", to, sizeof(to));
      jsmn_find_extract_token(body, tokens, index, "descr", descr, sizeof(descr));
      strcpy(wasf, query_berth(from));
      strcpy(wast, query_berth(to));
      _log(DEBUG, "CA:               Berth step (%s) Description \"%s\" from berth \"%s\" to berth \"%s\"", time_text(timestamp, true), descr, from, to);
      log_detail(timestamp, "CA: %s from %s to %s", descr, from, to);
      if((strcmp(wasf, descr) && strcmp(from, "STIN")) || 
         (strcmp(wast, "")    && strcmp(to, "COUT")))
      {
         _log(MINOR, "Message CA: Step \"%s\" from \"%s\" to \"%s\" found \"%s\" in \"%s\" and \"%s\" in \"%s\".", descr, from, to, wasf, from, wast, to);
      }
      update_database(Berth, from, "");
      update_database(Berth, to, descr);
      stats[CA]++;
   }
   else if(!strcasecmp(message_type, "CB"))
   {
      jsmn_find_extract_token(body, tokens, index, "from", from, sizeof(from));
      jsmn_find_extract_token(body, tokens, index, "descr", descr, sizeof(descr));
      strcpy(wasf, query_berth(from));
      _log(DEBUG, "CB:             Berth cancel (%s) Description \"%s\" from berth \"%s\"", time_text(timestamp, true), descr, from);
      log_detail(timestamp, "CB: %s from %s", descr, from);
      if(strcmp(wasf, descr))
      {
         _log(MINOR, "Message CB: Cancel \"%s\" from \"%s\" found \"%s\" in \"%s\".", descr, from, wasf, from);
      }
      update_database(Berth, from, "");
      stats[CB]++;
   }
   else if(!strcasecmp(message_type, "CC"))
   {
      jsmn_find_extract_token(body, tokens, index, "to", to, sizeof(to));
      jsmn_find_extract_token(body, tokens, index, "descr", descr, sizeof(descr));
      _log(DEBUG, "CC:          Berth interpose (%s) Description \"%s\" to berth \"%s\"", time_text(timestamp, true), descr, to);
      log_detail(timestamp, "CC: %s           to %s", descr, to);
      update_database(Berth, to, descr);
      stats[CC]++;
   }
   else if(!strcasecmp(message_type, "CT"))
   {
      // Heartbeat
      char report_time[16];
   
      jsmn_find_extract_token(body, tokens, index, "report_time", report_time, sizeof(report_time));
      _log(DEBUG, "CT:                  Heartbeat (%s) Report time = %s", time_text(timestamp, true), report_time);
      stats[CT]++;
   }
   else if(!strcasecmp(message_type, "SF"))
   {
      char address[16], data[32];
      jsmn_find_extract_token(body, tokens, index, "address", address, sizeof(address));
      jsmn_find_extract_token(body, tokens, index, "data", data, sizeof(data));
      _log(DEBUG, "SF:        Signalling update (%s) Address \"%s\", data \"%s\"", time_text(timestamp, true), address, data);

      word a = atoi(address);
      dword d = strtoul(data, NULL, 16);
      signalling_update("SF", timestamp, a, d);

      stats[SF]++;
   }
   else if(!strcasecmp(message_type, "SG"))
   {
      char address[16], data[32];
      jsmn_find_extract_token(body, tokens, index, "address", address, sizeof(address));
      jsmn_find_extract_token(body, tokens, index, "data", data, sizeof(data));
      _log(DEBUG, "SG:       Signalling refresh (%s) Address \"%s\", data \"%s\"", time_text(timestamp, true), address, data);
      word  a = atoi(address);
      dword d = strtoul(data, NULL, 16);
      _log(DEBUG, "a = %04x, d = %08x", a, d);
         signalling_update("SG", timestamp, a     , 0xff & (d >> 24));
         signalling_update("SG", timestamp, a + 1 , 0xff & (d >> 16));
         signalling_update("SG", timestamp, a + 2 , 0xff & (d >> 8 ));
         signalling_update("SG", timestamp, a + 3 , 0xff & (d      ));

      stats[SG]++;
   }
   else if(!strcasecmp(message_type, "SH"))
   {
      char address[16], data[32];
      jsmn_find_extract_token(body, tokens, index, "address", address, sizeof(address));
      jsmn_find_extract_token(body, tokens, index, "data", data, sizeof(data));
      _log(DEBUG, "SH: Signalling refresh final (%s) Address \"%s\", data \"%s\"", time_text(timestamp, true), address, data);
      word  a = atoi(address);
      dword d = strtoul(data, NULL, 16);
      _log(DEBUG, "a = %04x, d = %08x", a, d);
      signalling_update("SH", timestamp, a     , 0xff & (d >> 24));
      signalling_update("SH", timestamp, a + 1 , 0xff & (d >> 16));
      signalling_update("SH", timestamp, a + 2 , 0xff & (d >> 8 ));
      signalling_update("SH", timestamp, a + 3 , 0xff & (d      ));
      if(debug) _log(DEBUG, show_signalling_state());
      
      stats[SH]++;
   }
   else
   {
      _log(GENERAL, "Unrecognised message type \"%s\":", message_type);
      jsmn_dump_tokens(body, tokens, index);
   }
}

static void signalling_update(const char * const message_name, const time_t t, const word a, const dword d)
{
   char detail[256], detail1[256];

   _log(PROC, "signalling_update(\"%s\", %02x, %08x)", message_name, a, d);

   detail[0] = '\0';
   if(a < SIG_BYTES)
   {
      dword o = signalling[a];
      signalling[a] = d;

      word b;
      for(b=0; b<8; b++)
      {
         if((((o>>b)&0x01) != ((d>>b)&0x01)) || o > 0xff)
         {
            sprintf(detail1, "  Bit %d%d = %ld", a, b, ((d>>b)&0x01));
            strcat(detail, detail1);
            char signal_key[8];
            sprintf(signal_key, "%d%d", a, b);
            update_database(Signal, signal_key, ((d>>b)&0x01)?"1":"0");
         }
      }
   }
   else
   {
      _log(MINOR, "Signalling address %04x out of range in %s message.  Data %08x", a, message_name, d);
   }
   if(debug) _log(DEBUG, show_signalling_state());
   log_detail(t, "%s: %02x = %02x [%s]%s", message_name, a, d, show_signalling_state(), detail);
}

static void update_database(const word type, const char * const b, const char * const v)
{
   char query[512], typec;
   MYSQL_RES * result;
   MYSQL_ROW row;
   time_t now = time(NULL);

   _log(PROC, "update_database(%d, \"%s\", \"%s\")", type, b, v);

   if(type == Berth) 
   {
      typec = 'b';
   }
   else
   {
      typec = 's';
   }

   if(++handle > MAX_HANDLE)
   {
      handle = 1;
      db_query("delete from td_updates");
   }
   else
   {
      sprintf(query, "delete from td_updates where k = '%c%s'", typec, b);
      db_query(query);
   }

   sprintf(query, "INSERT INTO td_updates values(%ld, %d, '%c%s', '%s')", now, handle, typec, b, v);
   db_query(query);

   sprintf(query, "SELECT * FROM td_states where k = '%c%s'", typec, b);
   if(!db_query(query))
   {
      result = db_store_result();
      if((row = mysql_fetch_row(result))) 
      {
         sprintf(query, "UPDATE td_states SET updated = %ld, v = '%s' where k = '%c%s'", now, v, typec, b);
      }
      else
      {
         sprintf(query, "INSERT INTO td_states VALUES(%ld, '%c%s', '%s')", now, typec, b, v);
      }
      mysql_free_result(result);
      db_query(query);
   }
}

static const char * const query_berth(const char * const b)
{
   char query[512];
   MYSQL_RES * result;
   MYSQL_ROW row;
   static char reply[32];

   strcpy(reply, "");

   sprintf(query, "SELECT * FROM td_states where k = 'b%s'", b);
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
   MYSQL_ROW row0;
   word create_td_updates, create_td_states, create_friendly_names_20;

   _log(PROC, "create_database()");

   create_td_updates = create_td_states = create_friendly_names_20 = true;

   if(db_query("show tables")) return;
   
   result0 = db_store_result();
   while((row0 = mysql_fetch_row(result0))) 
   {
      if(!strcasecmp(row0[0], "td_updates"))      create_td_updates      = false;
      if(!strcasecmp(row0[0], "td_states"))       create_td_states       = false;
      if(!strcasecmp(row0[0], "friendly_names_20"))  create_friendly_names_20  = false;
   }
   mysql_free_result(result0);

   if(create_td_updates)
   {
      db_query(
"CREATE TABLE td_updates "
"(created INT UNSIGNED NOT NULL, "
"handle   INT UNSIGNED NOT NULL, "
"k        CHAR(8) NOT NULL, "
"v        CHAR(8) NOT NULL "
") ENGINE = InnoDB"
               );
      _log(GENERAL, "Created database table td_updates.");
   }
   if(create_td_states)
   {
      db_query(
"CREATE TABLE td_states "
"(updated INT UNSIGNED NOT NULL, "
"k        CHAR(8) NOT NULL, "
"v        CHAR(8) NOT NULL "
") ENGINE = InnoDB"
               );
      _log(GENERAL, "Created database table td_states.");
   }
   if(create_friendly_names_20)
   {
      db_query(
"CREATE TABLE friendly_names_20 "
"(tiploc CHAR(8) NOT NULL, "
"name    VARCHAR(32), "
"PRIMARY KEY (tiploc) "
") ENGINE = InnoDB"
               );
      _log(GENERAL, "Created database table friendly_names_20.");
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

static void log_message(const char * const message)
{
   FILE * fp;
   char filename[128];

   time_t now = time(NULL);
   struct tm * broken = gmtime(&now);
   
   sprintf(filename, "/tmp/tddb-messages-%04d-%02d-%02d.log", broken->tm_year + 1900, broken->tm_mon + 1, broken->tm_mday);
   
   if((fp = fopen(filename, "a")))
   {
      fprintf(fp, "%s\n%s\n", time_text(now, false), message);
      fclose(fp);
   }
}

static const char * show_signalling_state(void)
{
   word i;
   char s[8];
   static char  state[128];

   state[0] = '\0';
   for(i = 0; i < SIG_BYTES; i++)
   {
      if(i) strcat(state, " ");
      if(signalling[i] > 0xff)
      {
         strcat(state, "..");
      }
      else
      {
         sprintf(s, "%02x", signalling[i]);
         strcat(state, s);
      }
   }
   return state;
}

static void log_detail(const time_t stamp, const char * text, ...)
{
   FILE * fp;
   char filename[128];

   va_list vargs;
   va_start(vargs, text);

   time_t now = time(NULL);
   struct tm * broken = gmtime(&now);
      
   sprintf(filename, "/tmp/tddb-results-%04d-%02d-%02d.log", broken->tm_year + 1900, broken->tm_mon + 1, broken->tm_mday);

   if((fp = fopen(filename, "a")))
   {
      fprintf(fp, "%s ] ", time_text(stamp, false));
      vfprintf(fp, text, vargs);
      fprintf(fp, "\n");
      fclose(fp);
   }

   va_end(vargs);
   return;
}
