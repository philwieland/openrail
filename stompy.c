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
#include <sys/sysinfo.h>
#include <netinet/in.h>
#include <errno.h>
#include <netdb.h>
#include <dirent.h>

#include "misc.h"

#define NAME  "stompy"
#define BUILD "X328"

static void perform(void);
static void set_up_server_sockets(void);
static void stomp_write(void);
static void stomp_read(void);
static void client_write(const int s);
static void client_read(const int s);
static void client_accept(const int s);
static void user_command(void);
static void send_subscribes(void);
static void handle_shutdown(word report);
static void full_shutdown(void);
enum stomp_manager_event {SM_START, SM_ERROR, SM_FAIL, SM_TIMEOUT, SM_TX_DONE, SM_RX_DONE, SM_SHUTDOWN, SM_EVENTS};
static void stomp_manager(const enum stomp_manager_event event, const char * const headers);
static void stomp_queue_tx(const char * const d, const ssize_t l);
static void report_stats(void);
static void report_alarms(void);
static void report_rates(const char * const m);
static void next_stats(void);
static void next_alarms(void);
static void report_status(void);
static void dump_headers(const char * const h);

static void init_buffers_queues(void);
static struct frame_buffer * new_buffer(void);
static void free_buffer(struct frame_buffer * const b);
static void enqueue(const word s, struct frame_buffer * const b);
static struct frame_buffer * dequeue(const word s);
static struct frame_buffer * queue_front(const word s);
static word queue_length(const word s);

static void dump_buffer_to_disc(const word s, struct frame_buffer * const b);
static word dump_queue_to_disc(const word s);
static int is_a_buffer(const struct dirent *d);
static word load_queue_from_disc(const word s);
static int disc_queue_length(const word s);
static qword disc_queue_oldest(const word s);
static void log_message(const word s);
static word count_messages(const struct frame_buffer * const b);
static void heartbeat_tx(void);

static word debug, run, interrupt, sigusr1, controlled_shutdown;
static char zs[4096];

// STOMP manager
static enum {SM_AWAIT_CONNECTED, SM_RUN, SM_SEND_DISCO, SM_HOLD, SM_STATES} stomp_manager_state;

// STOMP RX
static enum {STOMP_IDLE, STOMP_HEADER, STOMP_BODY, STOMP_FAIL} stomp_read_state;
static struct frame_buffer * stomp_read_buffer;

// STOMP TX Queue
#define STOMP_TX_QUEUE_SIZE 32768
static char stomp_tx_queue[STOMP_TX_QUEUE_SIZE];
static ssize_t stomp_tx_queue_on, stomp_tx_queue_off;

#define BASE_PORT 55840

#define STOMP_HOST "datafeeds.networkrail.co.uk"
#define STOMP_PORT 61618

// Select timeout period in seconds
#define SELECT_TIMEOUT 2

// Sockets
static fd_set read_sockets, write_sockets, active_read_sockets, active_write_sockets;
static int s_stomp;
static byte s_stream[FD_SETSIZE];
#define STREAMS 4
#define STOMP STREAMS
static byte s_type[FD_SETSIZE];
enum s_types {CLIENT, SERVER, TYPES};
static int s_number[STREAMS][TYPES];

// Stream modes
static enum {STREAM_DISC, STREAM_RUN, STREAM_LOCK} stream_state[STREAMS];

static char * stomp_topics[STREAMS];
static char * stomp_topic_names[STREAMS];
static word stomp_topic_log[STREAMS];
static char topics[1024];

// Stats
static time_t start_time;
enum stats_categories {StompBytes, ConnectAttempt, StompMessage, BaseStreamFrameSent, 
                       DiscWrite = BaseStreamFrameSent + STREAMS, DiscRead, MAXstats
};
static qword stats[MAXstats];
static qword grand_stats[MAXstats];
static const char * stats_category[MAXstats] = 
   {
      "STOMP Bytes", "STOMP Connect Attempt", "STOMP Message", "", "", "", "",
      "Frame Disc Write", "Frame Disc Read",
   };
static qword stats_longest;
static qword grand_stats_longest;

#define RATES_AVERAGE_OVER 16
static word rates_count[STREAMS][RATES_AVERAGE_OVER];
static word rates_index, rates_start;

// Timers
static time_t now;
// Time in hours (local) when daily statistical report is produced.
#define REPORT_HOUR 4
// STOMP timeout in seconds
#define STOMP_TIMEOUT 64
// server socket retry time in seconds
#define SERVER_SOCKET_RETRY 32
// Heartbeat TX interval in seconds
#define HEARTBEAT_TX 16
static time_t stats_due, alarms_due, server_sockets_due, stomp_timeout, rates_due, heartbeat_tx_due;

// STOMP controls
static word stomp_holdoff;

// Disc spool
// This and sub-directories will be created if required.
#define STOMPY_SPOOL "/var/spool/stompy"

// Command file
#define COMMAND_FILE "/tmp/stompy.cmd"

// Message rates log file
#define RATES_FILE "/var/log/garner/stompy.rates"

// Message log
#define MESSAGE_LOG_FILEPATH "/var/log/garner/stompy.messagelog"

// Frame buffers and queues thereof
#define BUFFERS 32
#define FRAME_SIZE 64000
static struct frame_buffer
{
   char frame[FRAME_SIZE];
   qword stamp;
   struct frame_buffer * next;
} buffers[BUFFERS];
static struct frame_buffer * empty_list, * stream_q_on[STREAMS], * stream_q_off[STREAMS];
 
// Client interface
static ssize_t client_length[STREAMS], client_index[STREAMS];
static struct frame_buffer * client_buffer[STREAMS];
static enum { CLIENT_IDLE, CLIENT_AWAIT_ACK, CLIENT_RUN} client_state[STREAMS];

// Signal handling
void termination_handler(int signum)
{
   if(signum == SIGUSR1)
   {
      sigusr1 = true;
   }
   else if(signum != SIGHUP)
   {
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

   {
      // Parse subscriptions from config
      strcpy(topics, conf[conf_stomp_topics]);
      char * p = topics;
      char * q;
      char * e = p + strlen(p);
      int i;
      for(i = 0; i < STREAMS; i++)
      {
         q = strchr(p, ';');
         if(q)
         {
            stomp_topics[i] = p;
            *q = '\0';
            p = q + 1;
         }
         else
         {
            stomp_topics[i] = p;
            p = e;
         }
         if(!strcasecmp(stomp_topics[i], "void")) stomp_topics[i][0] = '\0';
      }
      p = e + 1;
      strcpy(p, conf[conf_stomp_topic_names]);
      e = p + strlen(p);
      for(i = 0; i < STREAMS; i++)
      {
         q = strchr(p, ';');
         if(q)
         {
            stomp_topic_names[i] = p;
            *q = '\0';
            p = q + 1;
         }
         else
         {
            stomp_topic_names[i] = p;
            p = e;
         }
      }
      for(i = 0; i < STREAMS; i++)
      {
         stomp_topic_log[i] = false;
      }
      {
         char temp[2048];
         strcpy(temp, conf[conf_stomp_topic_log]);
         p = temp;
         for(i = 0; i < STREAMS; i++)
         {
            q = strchr(p, ';');
            if(q)
            {
               *q = '\0';
               stomp_topic_log[i] = !strcasecmp(p, "true");
               p = q + 1;
            }
         }
      }
   }

   int lfp = 0;

   now = start_time = time(NULL);
   // Set up log
   _log_init(debug?"/tmp/stompy.log":"/var/log/garner/stompy.log", debug?1:0);

   _log(GENERAL, "");
   _log(GENERAL, "%s %s", NAME, BUILD);

   // Enable core dumps
   struct rlimit limit;
   if(!getrlimit(RLIMIT_CORE, &limit))
   {
      limit.rlim_cur = RLIM_INFINITY;
      setrlimit(RLIMIT_CORE, &limit);
   }

   // Create spool directories
   {
      word s;
      struct stat b;
      char d[256];
      if(stat(STOMPY_SPOOL, &b))
      {
         if(mkdir(STOMPY_SPOOL, 0777))
         {
            _log(CRITICAL, "Failed to create spool directory \"%s\".  Error %d %s", STOMPY_SPOOL, errno, strerror(errno));
         }
         else
         {
            _log(GENERAL, "Created spool directory \"%s\".", STOMPY_SPOOL);
         }
         if(chmod(STOMPY_SPOOL, 0777))
         {
            _log(CRITICAL, "Failed to chmod spool directory \"%s\".  Error %d %s", STOMPY_SPOOL, errno, strerror(errno));
         }
      }
      if(stat(STOMPY_SPOOL, &b))
      {
         _log(CRITICAL, "Spool directory \"%s\" missing.  Error %d %s.  Fatal.", STOMPY_SPOOL, errno, strerror(errno));
         exit(1);
      }

      for(s = 0; s < STREAMS; s++)
      {
         sprintf(d, "%s/%s%d", STOMPY_SPOOL, debug?"d-":"", s);
         if(stat(d, &b))
         {
            if(mkdir(d, 0777))
            {
               _log(CRITICAL, "Failed to create spool directory \"%s\".  Error %d %s", d, errno, strerror(errno));
            }
            else
            {
               _log(GENERAL, "Created spool directory \"%s\".", d);
            }
            if(chmod(d, 0777))
            {
               _log(CRITICAL, "Failed to chmod spool directory \"%s\".  Error %d %s", d, errno, strerror(errno));
            }
         }
         if(disc_queue_length(s) < 0)
         {
            _log(CRITICAL, "Spool directory missing for stream %d.  Fatal.", s);
            exit(1);
         }
      }
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

      umask(022); // Created files will be rw for owner, r for all others

      i = chdir("/var/run/");  
      if(i < 0)
      {
         /* chdir error */
         _log(CRITICAL, "chdir() error.  Aborting.");
         exit(1);
      }
        
      if((lfp = open("/var/run/stompy.pid", O_RDWR|O_CREAT, 0640)) < 0)
      {
         _log(CRITICAL, "Unable to open pid file \"/var/run/stompy.pid\".  Aborting.");
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
      
      _log(GENERAL, "Running as daemon.");
   }
   else
   {
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
   if(signal(SIGUSR1, termination_handler) == SIG_IGN) signal(SIGUSR1, SIG_IGN);
   signal(SIGPIPE, SIG_IGN);
   if(!debug) signal(SIGCHLD, SIG_IGN); /* ignore child */
   if(!debug) signal(SIGTSTP, SIG_IGN); /* ignore tty signals */
   if(!debug) signal(SIGTTOU, SIG_IGN);
   if(!debug) signal(SIGTTIN, SIG_IGN);

   // Zero the stats
   {
      word i;
      for(i=0; i < MAXstats; i++) 
      {
         stats[i] = 0;
         grand_stats[i] = 0;
      }
   }
   stats_longest = grand_stats_longest = 0;

   // Remove any user commands lying around.
   unlink(COMMAND_FILE);

   // Report subscriptions
   {
      word stream;
      _log(GENERAL, "Configured STOMP topics:");
      for(stream = 0; stream < STREAMS; stream++)
      if(stomp_topics[stream][0])
      {
         _log(GENERAL, "   %d: \"%s\" (%s) Logging %s.", stream, stomp_topics[stream], stomp_topic_names[stream], stomp_topic_log[stream]?"enabled":"disabled");
      }
      else
      {
         _log(GENERAL, "   %d: Not used.", stream);
      }
      if(*conf[conf_stompy_bin]) _log(CRITICAL, "All received data will be discarded due to stompy_bin option.");
   }

   // Startup delay.  Only applied immediately after system boot
   {
      struct sysinfo info;
      word logged = false;
      word i;
      while(run && !debug && (sysinfo(&info) || info.uptime < 32))
      {
         if(!logged) _log(GENERAL, "Startup delay...");
         logged = true;
         for(i = 0; i < 8 && run; i++) sleep(1);
      }
   }

   perform();

   if(lfp) close(lfp);

   return 0;
}

static void perform(void)
{
   int s;
   word stream, type;

   struct timeval wait_time;

   // Initialise queues
   stomp_tx_queue_on = stomp_tx_queue_off = 0;
   init_buffers_queues();

   // Initialise all socket settings.
   FD_ZERO (&read_sockets);
   FD_ZERO (&write_sockets);
   s_stomp = -1;
   for(stream = 0; stream < STREAMS; stream++)
   {
      for(type = 0; type < TYPES; type++)
      {
         s_number[stream][type] = -1;
      }
      client_state[stream] = CLIENT_IDLE;
      client_buffer[stream] = NULL;
      stream_state[stream] = STREAM_DISC;
   }

   run = true;
   interrupt = false;
   sigusr1 = false;
   controlled_shutdown = false;
   report_rates(NULL);

   // Set up timers
   next_stats();
   next_alarms();
   stomp_timeout = stomp_holdoff = server_sockets_due = rates_due = heartbeat_tx_due = 0;

   // Set up STOMP interface
   stomp_read_state = STOMP_IDLE;
   stomp_read_buffer = NULL;
   stomp_manager(SM_START, NULL);

   while(run)
   {
      now = time(NULL);

      // Check each timer for expiry
      if(now > heartbeat_tx_due) heartbeat_tx();
      if(now > server_sockets_due && !controlled_shutdown) set_up_server_sockets();
      if(now > stats_due)     report_stats();
      if(now > alarms_due)    report_alarms();
      if(now > rates_due)     report_rates("");
      if(now > stomp_timeout) stomp_manager(SM_TIMEOUT, NULL);

      if(controlled_shutdown)
      {
         wait_time.tv_sec  = 0;
         wait_time.tv_usec = 0x40000L;
      }
      else
      {
         wait_time.tv_sec  = SELECT_TIMEOUT;
         wait_time.tv_usec = 0;
      }
      active_read_sockets = read_sockets;
      active_write_sockets = write_sockets;
      int result = select(FD_SETSIZE, &active_read_sockets, &active_write_sockets, NULL, &wait_time);
      if(result < 0)
      {
         // Error
         if(errno == EINTR)
         {
         }
         else
         {
            _log(CRITICAL, "Select returns error %d %s.  Fatal.", errno, strerror(errno));
            run = false;
         }
      }
      else if (result == 0)
      {
         // Select has timed out
         if(controlled_shutdown) handle_shutdown(true);
      }
      else
      {
         // Got some activity.
         for(s = 0; s < FD_SETSIZE; s++)
         {
            if(FD_ISSET(s, &active_write_sockets))
            {
               if(s_stream[s] == STOMP) stomp_write();
               else client_write(s);
            }
            if(FD_ISSET(s, &active_read_sockets))
            {
               if(s_type[s] == SERVER) client_accept(s);
               else if(s_stream[s] == STOMP) stomp_read();
               else client_read(s); 
            }
         }
      }
      if(sigusr1) user_command();
      sigusr1 = false;
      if(interrupt) full_shutdown();
      interrupt = false;
   }

   report_status();
   report_stats();
}

static void set_up_server_sockets(void)
{
   //  
   int s;
   word stream;
   struct sockaddr_in server_addr;

   _log(PROC, "set_up_server_sockets()");

   server_sockets_due = 0x7fffffff;
   if(controlled_shutdown) return;

   for(stream = 0; stream < STREAMS; stream++)
   {
      if(stomp_topics[stream][0] && s_number[stream][SERVER] < 0)
      {
         s = socket(AF_INET, SOCK_STREAM, 0);
         if (s < 0)
         {
            _log(CRITICAL, "Failed to create server socket for stream %d.  Error %d %s.  Fatal.", stream, errno, strerror(errno));
            exit(1);
         }
   
         memset((void *) &server_addr, 0, sizeof(server_addr));

         server_addr.sin_family = AF_INET;
         server_addr.sin_addr.s_addr = INADDR_ANY;
         server_addr.sin_port = htons(BASE_PORT + stream);
   
         if(bind(s, (struct sockaddr *) &server_addr, sizeof(server_addr)) < 0) 
         {
            _log(MAJOR, "Failed to bind to socket %d.  Error %d %s.", BASE_PORT + stream, errno, strerror(errno));
            server_sockets_due = now + SERVER_SOCKET_RETRY;
            close(s);
            return;
         }
   
         // Make it non-blocking
         int oldflags = fcntl(s, F_GETFL, 0);
         oldflags |= O_NONBLOCK;
         fcntl(s, F_SETFL, oldflags);

         if (listen (s, 1) < 0)
         {
            _log(CRITICAL, "Failed to listen on socket %d, port %d, error %d %s.  Fatal.", s, BASE_PORT + stream, errno, strerror(errno));
            exit(1);
         }
      
         FD_SET (s, &read_sockets);
         s_type[s] = SERVER;
         s_stream[s] = stream;
         s_number[stream][SERVER] = s;
         _log(DEBUG, "Server socket %d for stream %d set up.", s, stream);
      }
   }
   _log(GENERAL, "All server sockets have been set up.");
}

static void stomp_write(void)
{
   // Send anything in the STOMP TX queue.
   // Note - This is NOT a proper circular buffer.
   _log(PROC, "stomp_write()");

   if(stomp_tx_queue_on != stomp_tx_queue_off)
   {
      ssize_t sent = write(s_stomp, stomp_tx_queue + stomp_tx_queue_off, stomp_tx_queue_on - stomp_tx_queue_off);
      if(sent < 0)
      {
         _log(MAJOR, "Failed to write to STOMP server.  Error %d %s", errno, strerror(errno));
         stomp_manager(SM_FAIL, NULL);
      }
      stomp_tx_queue_off += sent;
      _log(DEBUG, "stomp_write():  Sent %d bytes.  Last byte 0x%02x.", sent, stomp_tx_queue[stomp_tx_queue_off-1]);
   }

   if(stomp_tx_queue_on == stomp_tx_queue_off)
   {
      stomp_tx_queue_on = stomp_tx_queue_off = 0;

      FD_CLR(s_stomp, &write_sockets);
      _log(DEBUG, "stomp_write():  Queue now empty.");
      if(controlled_shutdown && stomp_read_state == STOMP_IDLE)
      {
         stomp_manager(SM_FAIL, NULL);
      }
      else
      {
         stomp_manager(SM_TX_DONE, NULL);
      }
   }
}

static void stomp_read(void)
{
#define MAX_HEADER 1024
   static char headers[MAX_HEADER];
   static ssize_t stomp_read_h_i, stomp_read_b_i;
   static char * mid, * s, * body;
   static word stream;

   _log(PROC, "stomp_read()");

   // N.B. Handling of stomp_read_buffer
   // If a receive is interrupted by an error, stomp_read_buffer remains pointing to a buffer (which contains 
   // garbage).  This will be re-used for the next frame.
   char d[1024];
   ssize_t l = read(s_stomp, d, 1024);
   if(l < 0)
   {
      _log(MAJOR, "STOMP read error %d %s", errno, strerror(errno));
      stomp_manager(SM_FAIL, NULL);
      return;
   }

   if(!l)
   {
      _log(MAJOR, "STOMP read error.  End of file.");
      stomp_manager(SM_FAIL, NULL);
      return;
   }

   _log(DEBUG, "stomp_read():  Received %d characters.", l);
   stats[StompBytes] += l;

   ssize_t i;

   for(i=0; i < l; i++)
   {
      switch(stomp_read_state)
      {
      case STOMP_IDLE: // Start
         stomp_read_h_i = stomp_read_b_i = 0;

         // First character - could be a heartbeat
         if(d[i] == '\n')
         {
            // Heartbeat
            _log(DEBUG, "STOMP heartbeat received.");
            stomp_read_state = STOMP_IDLE;
            stomp_manager(SM_RX_DONE, NULL);
         }
         else
         {
            headers[stomp_read_h_i++] = d[i];
            stomp_read_state = STOMP_HEADER;
         }               
         break;

      case STOMP_HEADER: // Header
         if(stomp_read_h_i >= MAX_HEADER)
         {
            _log(MAJOR, "Received STOMP headers too long.  Frame discarded.");
            stomp_read_state = STOMP_FAIL;
         }
         else
         {
            if(d[i] == '\n' && headers[stomp_read_h_i - 1] == '\n')
            {
               // End of header
               headers[stomp_read_h_i++] = '\0';
               if(debug)
               {
                  _log(DEBUG, "Headers received:");
                  dump_headers(headers);
               }
               stomp_read_state = STOMP_BODY;
               stomp_read_b_i = 0;
            }
            else
            {
               if(stomp_read_h_i < MAX_HEADER - 2)
               {
                  headers[stomp_read_h_i++] = d[i];
               }
               else
               {
                  _log(MAJOR, "Received STOMP headers too long.  Frame discarded.");
                  stomp_read_state = STOMP_FAIL;
               }
            }
         }
         break;

      case STOMP_BODY: // Body 
         if(stomp_manager_state == SM_AWAIT_CONNECTED)
         {
            // In this case we don't care about the body contents
            // Discard until we get the terminating NULL
            if(d[i] == '\0')
            {
               stomp_read_state = STOMP_IDLE;
               stomp_manager(SM_RX_DONE, headers);
            }
         }
         else
         {
            if(stomp_read_b_i == 0 && d[i] == '\0')
            {
               _log(MAJOR, "STOMP frame received has an empty body.  Discarded.  Headers:");
               dump_headers(headers);
               stomp_read_state = STOMP_IDLE;
               stomp_manager(SM_RX_DONE, headers);
            }
            else if(stomp_read_b_i == 0)
            {
               if(!strstr(headers, "MESSAGE"))
               {
                  _log(MAJOR, "STOMP frame received is not a MESSAGE.  Discarded.  Headers:");
                  dump_headers(headers);
                  stomp_read_state = STOMP_FAIL;
               }
               else
               {
                  // Find which stream
                  s = strstr(headers, "subscription:");
                  if(s)
                  {
                     s += 13;
                     _log(DEBUG, "Stream id %c.", *s);
                  }
                  else
                  {
                     _log(MAJOR, "STOMP MESSAGE received with no subscription value.  Discarded.  Headers:");
                     dump_headers(headers);
                     stomp_read_state = STOMP_FAIL;
                  }
               }
               if(stomp_read_state == STOMP_BODY)
               {
                  mid = strstr(headers, "message-id:");
                  if(mid)
                  {
                     mid += 11;
                  }
                  else
                  {
                     _log(MAJOR, "STOMP MESSAGE received with no message id.  Discarded.  Headers:");
                     dump_headers(headers);
                     stomp_read_state = STOMP_FAIL;
                  }
               }
               // Set up where it's going, and get a queue entry if appropriate
               if(stomp_read_state == STOMP_BODY)
               {
                  switch(*s)
                  {
                  case '0':  case '1': case '2': case '3':
                     stream = *s - '0';
                     // Note the following code allows one stream to hog all the buffers.  Is that a good idea?
                     if(!stomp_read_buffer) stomp_read_buffer = new_buffer();
                     if(stomp_read_buffer)
                     {
                        body = stomp_read_buffer->frame;
                     }
                     else
                     {
                        // Handle run out of buffers:  Dump queue to disc, switch to disc mode ask for a buffer again.
                        _log(GENERAL, "No buffers available for stream %d (%s).  Saving to disc.", stream, stomp_topic_names[stream]);
                        if(!dump_queue_to_disc(stream))
                        {
                           // None in our queue, find some elsewhere
                           word s;
                           for(s = 0; s < STREAMS; s++) 
                           {
                              if(dump_queue_to_disc(s)) 
                              {
                                 // Found some!
                                 if(stream_state[s] == STREAM_RUN) stream_state[s] = STREAM_DISC;
                                 _log(GENERAL, "Dumped queue to disc and switched to disc mode on stream %d (%s) to free space.", s, stomp_topic_names[s]);
                                 s = STREAMS;
                              }
                           }
                        }
                        if(stream_state[stream] == STREAM_RUN) stream_state[stream] = STREAM_DISC;
                        stomp_read_buffer = new_buffer();
                        if(!stomp_read_buffer)
                        {
                           _log(CRITICAL, "Failed to find a free buffer.  STOMP MESSAGE discarded.");
                           stomp_read_state = STOMP_FAIL;
                           body = NULL;
                        }
                        else
                           body = stomp_read_buffer->frame;
                     }
                     break;
                     
                  default: _log(MAJOR, "STOMP MESSAGE received with unrecognised subscription value \"%c\".", *s);
                     stomp_read_state = STOMP_FAIL;
                     body = NULL;
                     break;
                  }
               }
            }

            if(stomp_read_state == STOMP_BODY) 
            {
               // Stick character in buffer
               stomp_read_b_i++;
               if(body) *body++ = d[i];

               if(body && !d[i])
               {
                  // Deal with frame
                  _log(DEBUG, "Got end of message frame.  Processing...");
                  if(stomp_read_b_i > stats_longest) stats_longest = stomp_read_b_i;
                  stomp_read_buffer->stamp = time_us();
                  _log(DEBUG, "Stamp is %lld.", stomp_read_buffer->stamp);
                  rates_count[stream][rates_index] += count_messages(stomp_read_buffer);
                  if(!(*conf[conf_stompy_bin])) 
                  {
                     if(stream_state[stream] == STREAM_RUN)
                     {
                        enqueue(stream, stomp_read_buffer);
                     }
                     else
                     {
                        // write buffer to disc and free it
                        dump_buffer_to_disc(stream, stomp_read_buffer);
                     }
                     stomp_read_buffer = NULL; // Buffer has been emptied or recorded in the queue, so we no longer own it.
                  }
                  stats[StompMessage]++;
                  
                  if(s_number[stream][CLIENT] >= 0 && client_state[stream] != CLIENT_AWAIT_ACK)
                  {
                     FD_SET(s_number[stream][CLIENT], &write_sockets);
                  }

                  // Send ACK
                  {
                     char ack_h[1024];
                     sprintf(ack_h, "ACK\nsubscription:%c\nmessage-id:", *s);
                     ssize_t i = strlen(ack_h);
                     while((ack_h[i++] = *mid++) != '\n');
                     ack_h[i++] = '\n';
                     ack_h[i++] = '\0';
                     if(debug)
                     {
                        _log(DEBUG, "Ack message headers:");
                        dump_headers(ack_h);
                     }
                     stomp_queue_tx(ack_h, i);
                  }
                  
                  // All done
                  stomp_read_state = STOMP_IDLE;
                  stomp_manager(SM_RX_DONE, NULL);
               }
            }
         }
         break;
      
      case STOMP_FAIL: // Just bin data until the end of the STOMP frame.
         if(d[i] == '\0')
         {
            stomp_read_state = STOMP_IDLE;
            stomp_manager(SM_RX_DONE, NULL);
         }
         break;
      }
   }

   if(controlled_shutdown && stomp_read_state == STOMP_IDLE && stomp_tx_queue_on == 0)
   {
      stomp_manager(SM_FAIL, NULL);
   }
}

static void client_write(const int s)
{
   word stream = s_stream[s];
   _log(PROC, "client_write(%d):  Stream %d, client state %d", s, stream, client_state[stream]);

   ssize_t l;

   if(client_state[stream] == CLIENT_AWAIT_ACK)
   {
      // Shouldn't happen
      _log(CRITICAL, "client_write() Unexpected state CLIENT_AWAIT_ACK on stream %d", stream);
      FD_CLR(s, &write_sockets);
      return;
   }

   if(client_state[stream] == CLIENT_IDLE && controlled_shutdown)
   {
      dump_queue_to_disc(stream);
      stream_state[stream] = STREAM_LOCK;
      close(s);
      FD_CLR(s, &write_sockets);
      FD_CLR(s, &read_sockets);
      s_number[s][CLIENT] = -1;
      return;
   }
   
   if(client_state[stream] == CLIENT_IDLE)
   {
      if(client_buffer[stream]) _log(CRITICAL, "Unexpected client buffer!");
      client_buffer[stream] = queue_front(stream);
      if(!client_buffer[stream])
      {
         // Queue is empty
         if(stream_state[stream] == STREAM_RUN || stream_state[stream] == STREAM_LOCK)
         {
            // Nothing more to do
            FD_CLR(s, &write_sockets);
            return;
         }

         // stream_state is _DISC, queue is empty.  Read some more from disc.
         if(disc_queue_length(stream))
         {
            if(queue_length(STREAMS))
            {
               load_queue_from_disc(stream);
               client_buffer[stream] = queue_front(stream);
            }
            else 
            {
               // There is stuff on the disc, but we have no room to read it.
               // This is a problem.  If we just leave the stream in this state, it will hog the select.
               _log(GENERAL, "Unable to load stream %d (%s) messages from disc.  No buffers available.", stream, stomp_topic_names[stream]);
               word st;
               for(st = 0; st < STREAMS; st++)
               {
                  if(dump_queue_to_disc(st))
                  {
                     if(stream_state[st] == STREAM_RUN) stream_state[st] = STREAM_DISC;
                     _log(GENERAL, "Dumped queue for stream %d (%s) to disc to free space.", st, stomp_topic_names[st]);
                     st = STREAMS;
                  }
               }
               load_queue_from_disc(stream);
               client_buffer[stream] = queue_front(stream);
            }
         }
         else
         {
            // We have emptied the disc
            _log(GENERAL, "Stream %d (%s) disc queue empty.", stream, stomp_topic_names[stream]);
            stream_state[stream] = STREAM_RUN;
            FD_CLR(s, &write_sockets);
            return;
         }
      }

      client_length[stream] = strlen(client_buffer[stream]->frame) + 1; // INCLUDING the terminating \0
      _log(DEBUG, "Frame length is %ld.", client_length[stream]);
      client_index[stream] = 0;
      l = write(s,  &client_length[stream], sizeof(ssize_t));
      if(l != sizeof(ssize_t))
      {
         // Handle error
         _log(MAJOR, "Error sending buffer size to client.  l = %ld, error %d %s.", l, errno, strerror(errno));
         client_state[stream] = CLIENT_IDLE;
         client_buffer[stream] = NULL;
         close(s);
         s_number[stream][CLIENT] = -1;
         FD_CLR(s, &write_sockets);
         FD_CLR(s, &read_sockets);
         _log(GENERAL, "Client disconnected from stream %d (%s).", stream, stomp_topic_names[stream]);
         // Could switch to disc mode here?  Or wait until queue fills.
      }
      else
      {
         _log(DEBUG, "Wrote length OK");
         client_state[stream] = CLIENT_RUN;
      }
   }

   if(client_state[stream] == CLIENT_RUN)
   {
      _log(DEBUG, "About to write %ld bytes of body.",   client_length[stream] - client_index[stream]);
      l = write(s, &client_buffer[stream]->frame[client_index[stream]], client_length[stream] - client_index[stream]);
      if(l < 0)
      {
         // Handle error
         _log(MAJOR, "Error writing message to client buffer.  Error %d %s", errno, strerror(errno));
         client_state[stream] = CLIENT_IDLE;
         client_buffer[stream] = NULL; // Buffer is still on queue
         close(s);
         s_number[stream][CLIENT] = -1;
         FD_CLR(s, &write_sockets);
         FD_CLR(s, &read_sockets);
         _log(GENERAL, "Client disconnected from stream %d (%s).", stream, stomp_topic_names[stream]);
         // Could switch to disc mode here?  Or wait until queue fills.
      }
      else
      {
         _log(DEBUG, "client_write() sent %ld bytes of frame", l);
         client_index[stream] += l;
         if(client_index[stream] >= client_length[stream])
         {
            // Finished
            client_state[stream] = CLIENT_AWAIT_ACK;
            FD_CLR(s, &write_sockets);
            FD_SET(s, &read_sockets);
         }
      }
   }
   //   _log(DEBUG, "client_write():  Returns with client state = %d.", client_state[stream]);
}

static void client_read(const int s)
{
   word stream = s_stream[s];
   _log(PROC, "client_read(%d) stream %d", s, stream);
   
   ssize_t l;
   static char buffer[16];

   if(client_state[stream] != CLIENT_AWAIT_ACK)
   {
      _log(CRITICAL, "Unexpected client receive on socket %d stream %d", s, stream);
      FD_CLR(s, &read_sockets);
      l = read(s, buffer, 16);
      return;
   }
   l = read(s, buffer, 16);
   if(l < 0)
   {
      _log(MAJOR, "Error reading ACK from client on stream %d (%s).  Error %d %s.", stream, stomp_topic_names[stream], errno, strerror(errno));
      client_state[stream] = CLIENT_IDLE;
      client_buffer[stream] = NULL;
      close(s);
      s_number[stream][CLIENT] = -1;
      FD_CLR(s, &write_sockets);
      FD_CLR(s, &read_sockets);
      _log(GENERAL, "Client disconnected from stream %d (%s).", stream, stomp_topic_names[stream]);
      return;
   }
   else if(!l)
   {
      _log(MAJOR, "EOF reading ACK from client on stream %d (%s).", stream, stomp_topic_names[stream]);
      client_state[stream] = CLIENT_IDLE;
      client_buffer[stream] = NULL;
      close(s);
      s_number[stream][CLIENT] = -1;
      FD_CLR(s, &write_sockets);
      FD_CLR(s, &read_sockets);
      _log(GENERAL, "Client disconnected from stream %d (%s).", stream, stomp_topic_names[stream]);
      return;
   }
   FD_CLR(s, &read_sockets);
   client_state[stream] = CLIENT_IDLE;

   // Log message
   if(stomp_topic_log[stream]) log_message(stream);

   if(client_buffer[stream] != dequeue(stream))
   {
      _log(CRITICAL, "Queue end mismatch detected in client_read() on stream %d (%s).  Fatal.", stream, stomp_topic_names[stream]);
      run = false;
      return;
   }
   free_buffer(client_buffer[stream]);
   client_buffer[stream] = NULL;
   stats[BaseStreamFrameSent + stream]++;
   if(controlled_shutdown)
   {
      dump_queue_to_disc(stream);
      stream_state[stream] = STREAM_LOCK;
      close(s);
      FD_CLR(s, &write_sockets);
      FD_CLR(s, &read_sockets);
      s_number[stream][CLIENT] = -1;
      return;
   }
   FD_SET(s, &write_sockets);
   if(stream_state[stream] == STREAM_LOCK)
   {
      dump_queue_to_disc(stream);
   }
}

static void client_accept(const int s)
{
   _log(PROC, "client_accept(%d)", s);
   word stream = s_stream[s];
   int new_socket = accept(s, NULL, NULL);
   if(new_socket < 0)
   {
      _log(CRITICAL, "accept() on stream %d failed.  Error %d %s", stream, errno, strerror(errno));
   }
   else
   {
      // Handle the error condition where the socket is already open... Just close it quietly.
      if(s_number[stream][CLIENT] >= 0)
      {
         _log(MAJOR, "Client connect for stream %d, socket %d when socket %d already in use.", stream, new_socket, s_number[stream][CLIENT]);
         // Already open.  Close the old one.
         close(s_number[stream][CLIENT]);
         FD_CLR(s_number[stream][CLIENT], &write_sockets);
         FD_CLR(s_number[stream][CLIENT], &read_sockets);
         s_number[stream][CLIENT] = -1;
         if(client_state[stream] != CLIENT_IDLE)
         {
            // Deal with an unacknowledged client write buffer.
            // The buffer is still in the queue so in fact we don't have to do anything.
            client_state[stream] = CLIENT_IDLE;
         }
      }
      // Make it non-blocking
      int oldflags = fcntl(new_socket, F_GETFL, 0);
      /* Set just the flag we want to set:  Non-blocking. */
      oldflags |= O_NONBLOCK;
      fcntl(new_socket, F_SETFL, oldflags);

      s_stream[new_socket] = stream;
      s_type[new_socket] = CLIENT;
      s_number[stream][CLIENT] = new_socket;
      FD_SET(new_socket, &write_sockets);
      _log(GENERAL, "Client connected to stream %d (%s).", stream, stomp_topic_names[stream]);
   }
}

static void user_command(void)
{
   _log(PROC, "user_command()");
   char b[128];
   ssize_t i, l;

   FILE *cmd;
   if((cmd = fopen(COMMAND_FILE, "r")))
   {
      l = fread(b, 1, 128, cmd);
      fclose(cmd);
      if(unlink(COMMAND_FILE))
      {
         _log(MAJOR, "Failed to delete command file \"%s\":  Error %d %s", COMMAND_FILE, errno, strerror(errno));
      }
      for(i = 0; i < l; i++)
      {
         switch(b[i])
         {
         case 'v':
            _log(GENERAL, "Command v - Stream 0 (VSTP) hold.");
            stream_state[0] = STREAM_LOCK;
            break;
         case 't':
            _log(GENERAL, "Command t - Stream 1 (TRUST) hold.");
            stream_state[1] = STREAM_LOCK;
            break;
         case 'd':
            _log(GENERAL, "Command d - Stream 2 (TD) hold.");
            stream_state[2] = STREAM_LOCK;
            break;
         case 'V':
            _log(GENERAL, "Command V - Stream 0 (VSTP) run.");
            // We set the stream to DISC.  If the disc is empty it'll drop to RUN on the first client_write
            if(!controlled_shutdown)
            {
               stream_state[0] = STREAM_DISC;
               if(s_number[0][CLIENT] >= 0) FD_SET(s_number[0][CLIENT], &write_sockets);
            }
            break;
         case 'T':
            _log(GENERAL, "Command T - Stream 1 (TRUST) run.");
            if(!controlled_shutdown)
            {
               stream_state[1] = STREAM_DISC;
               if(s_number[1][CLIENT] >= 0) FD_SET(s_number[1][CLIENT], &write_sockets);
            }
            break;
         case 'D':
            _log(GENERAL, "Command D - Stream 2 (TD) run.");
            if(!controlled_shutdown)
            {
               stream_state[2] = STREAM_DISC;
               if(s_number[2][CLIENT] >= 0) FD_SET(s_number[2][CLIENT], &write_sockets);
            }
            break;
         case 's':
            _log(GENERAL, "Command s - Commence controlled shutdown.");
            controlled_shutdown = true;
            stomp_manager(SM_SHUTDOWN, NULL);
            handle_shutdown(false);
            break;

         case 'q': case 'Q':
            report_status();
            break;
         }
      }
   }
   
}

static void send_subscribes(void)
{
   char headers[1024];
   int stream;
   _log(PROC, "send_subscribes()");

   for(stream = 0; stream < STREAMS; stream++)
   {
      if(stomp_topics[stream][0])
      {
         strcpy(headers, "SUBSCRIBE\n");
         strcat(headers, "destination:/topic/");
         strcat(headers, stomp_topics[stream]);
         strcat(headers, "\n");
         if(debug)
         {
            sprintf(zs, "activemq.subscriptionName:%s-stompy-%d-debug\n", conf[conf_nr_user], stream);
            strcat(headers, zs);
         }
         else
         {
            sprintf(zs, "activemq.subscriptionName:%s-stompy-%d-%s\n", conf[conf_nr_user], stream, abbreviated_host_id());
            strcat(headers, zs);
         }
         sprintf(zs, "id:%d\n", stream);
         strcat(headers, zs);      
         strcat(headers, "ack:client\n");   
         strcat(headers, "\n");
         stomp_queue_tx(headers, strlen(headers) + 1);
      }
   }
}

static void handle_shutdown(word report)
{
   _log(PROC, "handle_shutdown()");

   word stream, complete;
   char reason[128];

   complete = true;

   if(s_stomp >= 0) 
   {
      complete = false;
      strcpy(reason, "STOMP socket open");
   }

   for(stream = 0; stream < STREAMS; stream++)
   {
      if(s_number[stream][SERVER] >= 0)
      {
         complete = false;
         sprintf(reason, "Stream %d (%s) server socket still open", stream, stomp_topic_names[stream]);
         //close(s_number[stream][SERVER]);
         shutdown(s_number[stream][SERVER], 2);
         FD_CLR(s_number[stream][SERVER], &read_sockets);
         FD_CLR(s_number[stream][SERVER], &write_sockets);
         s_number[stream][SERVER] = -1;
      }
      if(s_number[stream][CLIENT] >= 0)
      {
         complete = false;
         sprintf(reason, "Stream %d (%s) client connection still active", stream, stomp_topic_names[stream]);
         if(client_state[stream] == CLIENT_IDLE)
         {
            dump_queue_to_disc(stream);
            close(s_number[stream][CLIENT]);
            FD_CLR(s_number[stream][CLIENT], &read_sockets);
            FD_CLR(s_number[stream][CLIENT], &write_sockets);
            s_number[stream][CLIENT] = -1;
         }
      }
      stream_state[stream] = STREAM_LOCK;
   }  

   if(complete) 
   {
      _log(GENERAL, "Controlled shutdown complete.");
      full_shutdown();
   }
   else if(report)
   {
      _log(GENERAL, "Controlled shutdown waiting - %s.", reason);
   }
}

static void full_shutdown(void)
{
   word stream, type;
   _log(PROC, "full_shutdown()");

   if(interrupt) _log(GENERAL, "Shutting down due to interrupt.");

   // Close all sockets
   if(s_stomp >= 0)
   {
      close(s_stomp);
      s_stomp = -1;
   }
   for(stream = 0; stream < STREAMS; stream++)
   {
      dump_queue_to_disc(stream);
      for(type = 0; type < TYPES; type++)
      {
         if(s_number[stream][type] >= 0) 
         {
            close(s_number[stream][type]);
         }
      }
   }

   run = false;
}


///////// STOMP Manager //////////
#define SET_TIMER_HOLDOFF   {if(++stomp_holdoff > 16) stomp_holdoff = 16; stomp_timeout = now + stomp_holdoff * 8;}
#define SET_TIMER_RUNNING   {stomp_timeout = now + STOMP_TIMEOUT;}
#define SET_TIMER_NEVER     {stomp_timeout = 0x7fffffff;}
#define SET_TIMER_IMMEDIATE {stomp_timeout = now;}

static word sm_action[SM_STATES][SM_EVENTS] = {
   //                    0 SM_START, SM_ERROR, SM_FAIL, SM_TIMEOUT, SM_TX_DONE, SM_RX_DONE, SM_SHUTDOWN,
   /* 0 SM_AWAIT_CONNECTED */ {1,         2,       2,        2,          0,         4,           2,},
   /* 1 SM_RUN             */ {1,         3,       2,        3,          0,         5,           3,},
   /* 2 SM_SEND_DISCO      */ {1,         2,       2,        2,          2,         0,           0,},
   /* 3 SM_HOLD            */ {1,         0,       0,        1,          0,         0,           0,},
};
static void stomp_manager(const enum stomp_manager_event event, const char * const param)
{
   struct sockaddr_in serv_addr;
   struct hostent *server;
   static word alarm_sent;
   static time_t outage_start;

   _log(PROC, "stomp_manager(%d, \"%s\") in state %d:  Action %d.", event, param?param:"NULL", stomp_manager_state, sm_action[stomp_manager_state][event]);

   // Initialise
   if(event == SM_START) 
   {
      stomp_manager_state = SM_HOLD;
      outage_start = 0;
      alarm_sent = false;
   }

   if(stomp_manager_state >= SM_STATES || event >= SM_EVENTS)
   {
      _log(CRITICAL, "stomp_manager(%d, \"%s\") in state %d:  Action %d INVALID.", event, param?param:"NULL", stomp_manager_state, sm_action[stomp_manager_state][event]);
      return;
   }

   if(event == SM_TIMEOUT && stomp_manager_state == SM_RUN)
   {
      _log(CRITICAL, "STOMP timeout.");
   }

   switch(sm_action[stomp_manager_state][event])
   {
   case 0: // Do nothing
      break;

   case 1: // Start a connection.  Assume all is disconnected.
      if(controlled_shutdown)
      {
         SET_TIMER_NEVER;
         return;
      }
      if(outage_start && !alarm_sent)
      {
         char report[256];
         sprintf(report, "STOMP connection failed.");
         email_alert(NAME, BUILD, "STOMP Alarm", report);
         alarm_sent = true;
      }
      stats[ConnectAttempt]++;
      _log(GENERAL, "Connecting to STOMP server.");
      report_rates("Connecting to STOMP server.");

      stomp_read_state = STOMP_IDLE;
      // DO NOT stomp_read_buffer = NULL; HERE
      stomp_tx_queue_on = stomp_tx_queue_off = 0;

      s_stomp = socket(AF_INET, SOCK_STREAM, 0);
      if (s_stomp < 0) 
      {
         _log(CRITICAL, "Failed to create STOMP socket.  Error %d %d.", errno, strerror(errno));
         stomp_manager_state = SM_HOLD;
         SET_TIMER_HOLDOFF;
         return;
      }

      server = gethostbyname(STOMP_HOST);
      if (server == NULL) 
      {
         close(s_stomp);
         FD_CLR(s_stomp, &read_sockets);
         FD_CLR(s_stomp, &write_sockets);
         s_stomp = -1;
         _log(CRITICAL, "Failed to resolve STOMP server hostname.");
         stomp_manager_state = SM_HOLD;
         SET_TIMER_HOLDOFF;
         return;
      }

      bzero((char *) &serv_addr, sizeof(serv_addr));
      serv_addr.sin_family = AF_INET;
      bcopy((char *)server->h_addr, 
            (char *)&serv_addr.sin_addr.s_addr,
            server->h_length);
      serv_addr.sin_port = htons(STOMP_PORT);

      // Now connect to the server
      // Really, we should do this in non-blocking mode and handle the successful/unsuccessful connection in the main select.
      // This connect blocks if there's no-one there.
      if (connect(s_stomp, (const struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) 
      {
         _log(MAJOR, "Unable to connect to STOMP server.  Error %d %s.", errno, strerror(errno));
         close(s_stomp);
         FD_CLR(s_stomp, &read_sockets);
         FD_CLR(s_stomp, &write_sockets);
         s_stomp = -1;
         stomp_manager_state = SM_HOLD;
         SET_TIMER_HOLDOFF;
         return;
      }	

      // Make it non-blocking
      int oldflags = fcntl(s_stomp, F_GETFL, 0);
      oldflags |= O_NONBLOCK;
      fcntl(s_stomp, F_SETFL, oldflags);

      _log(GENERAL, "Socket connected to STOMP server.  Sending CONNECT message.");
      
      {
         char headers[1024];
         
         strcpy(headers, "CONNECT\n");
         strcat(headers, "login:");
         strcat(headers, conf[conf_nr_user]);  
         strcat(headers, "\npasscode:");
         strcat(headers, conf[conf_nr_password]);
         strcat(headers, "\n");          
         if(debug)
         {
            sprintf(zs, "client-id:%s-stompy-debug\n", conf[conf_nr_user]);
            strcat(headers, zs);
         }
         else
         {
            sprintf(zs, "client-id:%s-stompy-%s\n", conf[conf_nr_user], abbreviated_host_id());
            strcat(headers, zs);
         }          
         strcat(headers, "heart-beat:20000,20000\n");          
         strcat(headers, "\n");
         
         stomp_queue_tx(headers, strlen(headers) + 1);
      }
      stomp_manager_state = SM_AWAIT_CONNECTED;
      FD_SET(s_stomp, &read_sockets);
      s_stream[s_stomp] = STOMP;
      SET_TIMER_RUNNING;
      break;
      
   case 2: // Hard close
      if(!outage_start) 
      {
         outage_start = now;
         report_rates("STOMP connection failed.");
      }
      if(s_stomp >= 0)
      {
         close(s_stomp);
         FD_CLR(s_stomp, &read_sockets);
         FD_CLR(s_stomp, &write_sockets);
         s_stomp = -1;
         _log(GENERAL, "STOMP socket disconnected.");
      }
      stomp_manager_state = SM_HOLD;
      if(controlled_shutdown) 
      {
         SET_TIMER_NEVER;
      }
      else 
      {
         SET_TIMER_HOLDOFF;
      }
      break;
      
   case 3: // Soft close
      if(!outage_start) 
      {
         outage_start = now;
         report_rates("STOMP connection failed.");
      }
      if(s_stomp >= 0)
      {
         _log(GENERAL, "Sending STOMP DISCONNECT.");
         stomp_queue_tx("DISCONNECT\n\n", 13);
         SET_TIMER_RUNNING;
         stomp_manager_state = SM_SEND_DISCO;
      }
      else
      {
         stomp_manager_state = SM_SEND_DISCO;
         SET_TIMER_IMMEDIATE;
      } 
      break;

   case 4: // RX_DONE while awaiting connect response
      if(param)
      {
         _log(GENERAL, "STOMP response to CONNECT received.  Headers:");
         dump_headers(param);
         if(strstr(param, "CONNECTED"))
         {
            send_subscribes();
            stomp_manager_state = SM_RUN;
            SET_TIMER_RUNNING;
            report_rates("Connected to STOMP server.");
            if(outage_start)
            {
               time_t duration = now - outage_start;
               _log(GENERAL, "STOMP outage lasted %ld seconds.", duration);
               outage_start = 0;
               
               if(alarm_sent)
               {
                  char report[256];
                  sprintf(report, "STOMP connection restored.  Outage duration was %ld seconds.", duration);
                  email_alert(NAME, BUILD, "STOMP Alarm Cleared", report);
                  report_rates(report);
                  alarm_sent = false;
               }
            }
            stomp_holdoff = 0;
         }
         else
         {
            _log(MAJOR, "CONNECT response incorrect.");
            if(s_stomp >= 0)
            {
               FD_CLR(s_stomp, &read_sockets);
               FD_CLR(s_stomp, &write_sockets);
            }
            // This will get an immediate action 2
            stomp_manager_state = SM_SEND_DISCO;
            SET_TIMER_IMMEDIATE;
         }
      }
      else
      {
         // Could be a spurious heartbeat.  Wait a bit longer . . .
         _log(MINOR, "Unexpected NULL headers while awaiting CONNECTED.");
      }
      break;

   case 5: // RX_DONE while running.
   SET_TIMER_RUNNING;
   break;

   default:
   _log(CRITICAL, "Invalid action in STOMP manager.  state = %d, event = %d", stomp_manager_state, event);
   break;
   }
}

static void stomp_queue_tx(const char * const d, const ssize_t l)
{
   if(l > STOMP_TX_QUEUE_SIZE - stomp_tx_queue_on)
   {
      _log(MAJOR, "STOMP TX data lost.  Queue overflow");
      return;
   }
   memcpy(stomp_tx_queue + stomp_tx_queue_on, d, l);
   stomp_tx_queue_on += l;

   FD_SET(s_stomp, &write_sockets);
}

static void report_stats(void)
{
   char zs[128], zs1[128];
   word i;
   char report[2048];

   _log(GENERAL, "");
   sprintf(zs, "%27s: %-14s Total", "", "Day");
   _log(GENERAL, zs);
   strcpy(report, zs);
   strcat(report, "\n");

   sprintf(zs, "%27s: %-14s %ld days", "Run time", "", (now - start_time)/(24*60*60));
   _log(GENERAL, zs);
   strcat(report, zs);
   strcat(report, "\n");
   for(i=0; i<MAXstats; i++)
   {
      grand_stats[i] += stats[i];
      if(i >= BaseStreamFrameSent && i < BaseStreamFrameSent + STREAMS)
      {
         if(stomp_topics[i - BaseStreamFrameSent][0])
         {
            sprintf(zs1, "%s Frame Sent", stomp_topic_names[i - BaseStreamFrameSent]);
            sprintf(zs, "%27s: %-14s ", zs1, commas_q(stats[i]));
            strcat(zs, commas_q(grand_stats[i]));
            _log(GENERAL, zs);
            strcat(report, zs);
            strcat(report, "\n");
         }
      }
      else
      {
         sprintf(zs, "%27s: %-14s ", stats_category[i], commas_q(stats[i]));
         strcat(zs, commas_q(grand_stats[i]));
         _log(GENERAL, zs);
         strcat(report, zs);
         strcat(report, "\n");
      }
      stats[i] = 0;
   }
   if(stats_longest > grand_stats_longest)grand_stats_longest = stats_longest;
   sprintf(zs, "%27s: %-14s ", "Longest STOMP frame", commas_q(stats_longest));
   strcat(zs, commas_q(grand_stats_longest));
   _log(GENERAL, zs);
   strcat(report, zs);
   strcat(report, "\n");
   stats_longest = 0;

   email_alert(NAME, BUILD, "Statistics Report", report);

   next_stats();
}

static void report_alarms(void)
{
   // Survey queues and report any alarms detected.

   char zs[128];
   word head, too_old, stream;
   char report[2048];
   qword oldest = 0;

   head = true;
   report[0] = '\0';

   if(s_stomp < 0)
   {
      if(head)
      {
         head = false;
         _log(GENERAL, "Alarm report:");
         strcat(report, "\nAlarm report:\n");
      }
      _log(GENERAL, "STOMP connection is down.");
      strcat(report, "STOMP connection is down.\n");
   }

   for(stream = 0; stream < STREAMS && !(*conf[conf_stompy_bin]); stream++)
   {
      if(stomp_topics[stream][0])
      {
         too_old = false;
         if(disc_queue_length(stream)) 
         {
            oldest = disc_queue_oldest(stream);
            too_old = time_us() - oldest > 0x80000000LL;  // About 36 minutes.
         }
         if(too_old || stream_state[stream] != STREAM_RUN || s_number[stream][CLIENT] < 0)
         {
            if(head)
            {
               head = false;
               _log(GENERAL, "Active alarms:");
               strcat(report, "\nActive alarms:\n");
            }
         }

         if(s_number[stream][CLIENT] < 0)
         {
            sprintf(zs, "Stream %d (%s) client connection is down.", stream, stomp_topic_names[stream]);
            _log(GENERAL, zs);
            strcat(report, zs);
            strcat(report, "\n");
         }

         if(stream_state[stream] != STREAM_RUN)
         {
            sprintf(zs, "Stream %d (%s) is currently routing messages to disc.  %d messages on disc.", stream, stomp_topic_names[stream], disc_queue_length(stream));
            _log(GENERAL, zs);
            strcat(report, zs);
            strcat(report, "\n");
         }
         if(too_old)
         {
            oldest /= 1000;
            oldest /= 1000; // Gives seconds
            sprintf(zs, "Stream %d (%s) oldest message in disc queue is stamped %s.", stream, stomp_topic_names[stream], time_text(oldest, true));
            _log(GENERAL, zs);
            strcat(report, zs);
            strcat(report, "\n");
         }
      }
   }
         
   strcat(report, "\n");

   if(!head) email_alert(NAME, BUILD, "Alarm Status Report", report);

   next_alarms();
}

static void report_rates(const char * const m)
{
   FILE * rates_fp;
   static word last_hour;
   word i, j;
   dword total;
#define REPORTED_SILENCE_THRESHOLD 2
   static word reported_silence;
   dword mean;

   if(!m)
   {
      // Initialise
      reported_silence = 0;
      last_hour = 99;
      rates_index = 0;
      rates_start = true;
      return;
   }

   time_t now = time(NULL);
   struct tm * broken = gmtime(&now);

   if((rates_fp = fopen(RATES_FILE, "a")))
   {
      if(broken->tm_hour != last_hour)
      {
         last_hour = broken->tm_hour;
         fprintf(rates_fp, "                   ");
         for(i=0; i < STREAMS; i++)
         {
            fprintf(rates_fp, "|  %7s      ", stomp_topic_names[i]);
         }
         fprintf(rates_fp, "\n");
         fprintf(rates_fp, "                   ");
         for(i=0; i < STREAMS; i++)
         {
            fprintf(rates_fp, "|         Mean  ");
         }
         fprintf(rates_fp, "  Means calculated over %d minutes.\n", RATES_AVERAGE_OVER);
      }
      fprintf(rates_fp, "%02d/%02d/%02d %02d:%02d:%02dZ ", 
              broken->tm_mday, 
              broken->tm_mon + 1, 
              broken->tm_year % 100,
              broken->tm_hour,
              broken->tm_min,
              broken->tm_sec);

      if(!m[0])
      {
         total = 0;
         for(i=0; i < STREAMS; i++)
         {
            total += rates_count[i][rates_index];

            mean = 0;
            for(j=0; j < RATES_AVERAGE_OVER; j++)
            {
               mean += rates_count[i][j];
            }
            mean = (mean + (RATES_AVERAGE_OVER/2)) / RATES_AVERAGE_OVER;

            if(rates_start)
            {
               fprintf(rates_fp, "|  %5d     -  ", rates_count[i][rates_index]);
            }
            else
            {
               fprintf(rates_fp, "|  %5d %5ld  ", rates_count[i][rates_index], mean);
            }
         }
         
         rates_index++;
         if(rates_index >= RATES_AVERAGE_OVER) rates_start = false;
         rates_index %= RATES_AVERAGE_OVER;
         for(i=0; i < STREAMS; i++) rates_count[i][rates_index] = 0;

         // Bodge.  Don't do flow checking unless both "busy" streams are enabled.
         if(stomp_topics[1][0] && stomp_topics[2][0])
         {
            char report[256];

            if(!total)
            {
               // No messages this minute
               if(reported_silence < REPORTED_SILENCE_THRESHOLD)
               {
                  reported_silence++;
                  if(reported_silence == REPORTED_SILENCE_THRESHOLD)
                  {
                     // Beginning of silence
                     sprintf(report, "No STOMP frames received in %d minutes.", REPORTED_SILENCE_THRESHOLD);
                     _log(MAJOR, report);
                     fprintf(rates_fp, "  %s", report);
                     email_alert(NAME, BUILD, "STOMP Alarm", report);
                  }
               }
            }
            else if(reported_silence >= REPORTED_SILENCE_THRESHOLD)
            {
               // End of silence.
               reported_silence = 0;
               strcpy(report, "STOMP message flow has resumed.");
               _log(MAJOR, report);
               fprintf(rates_fp, "  %s", report);
               email_alert(NAME, BUILD, "STOMP Alarm Cleared", report);
            }
         }
      }
      else
      {
         fprintf(rates_fp, "%s", m);
      }
      fprintf(rates_fp, "\n");

      fclose(rates_fp);
      rates_due = now - now%60 + 60;
   }
   else
   {
      _log(MINOR, "Failed to open rates file \"%s\".", RATES_FILE);
      rates_due = now - now%60 + 600;
   }
}

static void next_stats(void)
{
   // Set up timer for when next stats report is due.
   time_t when = now + 24*60*60;
   struct tm * broken = localtime(&when);
   broken->tm_hour = REPORT_HOUR;
   broken->tm_min = 0;
   broken->tm_sec = 0;
   stats_due = timelocal(broken);
}

static void next_alarms(void)
{
   // Set up timer for when next alarm survey is due.
   time_t due = now + 3600;
   alarms_due = due - (due % 3600);
}

static void report_status(void)
{
   word stream, dql;
   char * ss[] = {"Disc", "Run", "Lock"};
   char * cs[] = {"Idle", "Await ack", "Run"}; 
   _log(GENERAL, "System status:");
   _log(GENERAL, "Frame buffers free %d.", queue_length(STREAMS));
   _log(GENERAL, "STOMP:  Manager state %d, read state %d, read buffer %sowned, socket %d.", stomp_manager_state, stomp_read_state, stomp_read_buffer?"":"not ", s_stomp);
   for(stream = 0; stream < STREAMS; stream++)
   {
      if(stomp_topics[stream][0])
      {
         dql = disc_queue_length(stream);
         _log(GENERAL, "Stream %d (%s): Server socket %d, client socket %d, frames in queue %d, on disc %d.", stream, stomp_topic_names[stream], s_number[stream][SERVER], s_number[stream][CLIENT], queue_length(stream), dql);
         _log(GENERAL, "   Stream state %d (%s), client state %d (%s), length %ld, index %ld.", stream_state[stream], ss[stream_state[stream]], client_state[stream], cs[client_state[stream]], client_length[stream], client_index[stream]);
         if(dql) 
         {
            qword oldest = disc_queue_oldest(stream);
            oldest /= 1000;
            oldest /= 1000;
            _log(GENERAL, "   Oldest message in disc queue is stamped %s.", time_text(oldest, true));
         }
      }
   }
}

static void dump_headers(const char * const h)
{
   char zs[256];
   ssize_t i,j;

   i=0;
   j=0;
   while(h && h[i] && j<256)
   {
      if(h[i] == '\n')
      {
         zs[j] = '\0';
         if(j) _log(GENERAL, "   %s", zs);
         j = 0;
         i++;
      }
      else
      {
         zs[j++] = h[i++];
      }
   }
}

// Buffer pool handlers
static void init_buffers_queues(void)
{
   word s;
   _log(PROC, "init_buffers_queues()");

   for(s = 0; s < BUFFERS - 1; s++)
   {
      buffers[s].next = &buffers[s + 1];
   }
   buffers[s].next = NULL;
   empty_list = buffers;

   for(s = 0; s < STREAMS; s++)
   {
      stream_q_on[s] = NULL;
      stream_q_off[s] = NULL;
   }
}

static struct frame_buffer * new_buffer(void)
{
   _log(PROC, "new_buffer()");
   struct frame_buffer * result = empty_list;
   if(result) empty_list = result->next;
   if(!result) _log(DEBUG, "new_buffer():  No buffers available.");
   return result;
}

void free_buffer(struct frame_buffer * const b)
{
   _log(PROC, "free_buffer(~)");
   b->next = empty_list;
   empty_list = b;
}

static void enqueue(const word s, struct frame_buffer * const b)
{
   _log(PROC, "enqueue(%d, ~)", s);
   if(stream_q_on[s]) stream_q_on[s]->next = b;
   stream_q_on[s] = b;
   if(!stream_q_off[s]) stream_q_off[s] = b;
   b->next = NULL;
}

static struct frame_buffer * dequeue(const word s)
{
   _log(PROC, "dequeue(%d)", s);
   struct frame_buffer * result = stream_q_off[s];
   if(result)
   {
      stream_q_off[s] = result->next;
      if(stream_q_on[s] == result) stream_q_on[s] = NULL;
   }
   return result;
}

static struct frame_buffer * queue_front(const word s)
{
   // Return buffer at front of queue or NULL
   // DO NOT Remove buffer from queue
   struct frame_buffer * result = stream_q_off[s];
   _log(PROC, "queue_front(%d) returns %s", s, result?"a buffer":"NULL");
   return result;
}

static word queue_length(const word s)
{
   // Return length of queue for specified stream.  If s == STREAMS, return length of free queue
   _log(PROC, "queue_length(%d)", s);
   struct frame_buffer * b;
   word result = 0;
   if(s < STREAMS)
   {
      b = stream_q_off[s];
   }
   else
   {
      b = empty_list;
   }
   while(b && result < 999)
   {
      result++;
      b = b->next;
   }
   return result;
}

static void dump_buffer_to_disc(const word s, struct frame_buffer * const b)
{
   char filename[256];

   sprintf(filename, "%s/%s%d/%lld", STOMPY_SPOOL, debug?"d-":"", s, b->stamp);
   _log(DEBUG, "dump_buffer_to_disc():  Target filename \"%s\".", filename);

   ssize_t length = strlen(b->frame);

   int fd = open(filename, O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
   if(fd < 0)
   {
      _log(CRITICAL, "Writing buffer to disc, failed to open \"%s\".  Error %d %s.", filename, errno, strerror(errno));
   }
   else
   {
      ssize_t l, done;
      done = 0;
      while (done < length)
      {
         l = write(fd, b->frame + done, length - done);
         if(l < 0)
         {
            _log(CRITICAL, "dump_buffer_to_disc()  Failed during write to \"%s\".  Error %d %s.", filename, errno, strerror(errno));
            done = length;
         }
         else
         {
            done += l;
         }
      }
      close(fd);
      stats[DiscWrite]++;
   }

   free_buffer(b);   
}

static word dump_queue_to_disc(const word s)
{
   // Note.  If the front entry is currently being transmitted or is awaiting ack,
   // it must not be dumped or freed, and must be left in the 
   // queue.  It will only be freed when the client acks it.
   _log(PROC, "dump_queue_to_disc(%d)", s);
   struct frame_buffer * b, * requeue = NULL;
   word r = 0;

   if(client_state[s] != CLIENT_IDLE) requeue = dequeue(s);

   while((b = dequeue(s)))
   {
      dump_buffer_to_disc(s, b);
      r++;
   }
   if(client_state[s] != CLIENT_IDLE && requeue) enqueue(s, requeue);

   _log(DEBUG, "dump_queue_to_disc(%d) returns %d", s, r);
   return r;
}
   
static int is_a_buffer(const struct dirent *d)
{
   if(d->d_name[0] >= '0' && d->d_name[0] <= '9')
      return 1;
   return 0;
}

static void load_buffer_from_disc(struct frame_buffer * const b, const word s, const char * const name)
{
   // NB if returned buffer has ->stamp==0 this indicates failure.
   char filepath[256], newpath[256];
   _log(PROC, "load_buffer_from_disc(~, %d, \"%s\")", s, name);

   sprintf(filepath, "%s/%s%d/%s", STOMPY_SPOOL, debug?"d-":"", s, name);
   _log(DEBUG, "load_buffer_from_disc():  Target filename \"%s\".", filepath);

   b->frame[0] = '\0';
   b->stamp = 0;

   int fd = open(filepath, O_RDONLY);
   if(fd < 0)
   {
      _log(CRITICAL, "Reading buffer from disc failed to open \"%s\".  Error %d %s.", filepath, errno, strerror(errno));
   }
   else
   {
      ssize_t l, length;
      length = 0;
      l = -1;
      while(l)
      {
         l = read(fd, b->frame + length, FRAME_SIZE - length - 1);
         if(l < 0)
         {
            _log(CRITICAL, "Error reading buffer \"%s\" from disc.  Error %d %s.", filepath, errno, strerror(errno));
            l = 0;
         }
         else if(l > 0)
         {
            length += l;
         }
         else // l == 0  EOF
         {
            b->stamp = atoll(name);
            b->frame[length] = 0; // Append the \0.
            _log(DEBUG, "load_buffer_from_disc(): Successfully read %ld bytes with stamp %lld.", l, b->stamp);
            stats[DiscRead]++;
         }
      }
      close(fd);

      // Now "delete" the file
      if(debug)
      {
         sprintf(newpath, "/tmp/stompy-%d-%s", s, name);
         rename(filepath, newpath);
      }
      else
      {
         if(unlink(filepath))
            _log(MAJOR, "Failed to delete \"%s\" from disc.  Error %d %s.", errno, strerror(errno));
      }
   }
}

static word load_queue_from_disc(const word s)
{
   // returns number loaded
   _log(PROC, "load_queue_from_disc(%d)", s);
   word result = 0;
   struct dirent **eps;
   struct frame_buffer * b;

   char spool_path[128];
   sprintf(spool_path, "%s/%s%d", STOMPY_SPOOL, debug?"d-":"", s);

   // Get some file names
   int n = scandir (spool_path, &eps, is_a_buffer, alphasort);
   if (n >= 0)
   {
      int cnt;
      for (cnt = 0; cnt < n && cnt < BUFFERS / 2; cnt++)
      {
         if( (b = new_buffer()) )
         {
            _log(DEBUG, "Loading \"%s\".", eps[cnt]->d_name);
            load_buffer_from_disc(b, s, eps[cnt]->d_name);
            if(b->stamp)
            {
               enqueue(s, b);
               result++;
            }
            else
            {
               free_buffer(b);
            }
         }
         free(eps[cnt]);
      }  
      for(;cnt < n; cnt++)
      {
         free(eps[cnt]);
      }         
      free(eps);
   }
   else
   {
      // Couldn't open the directory");
      _log(CRITICAL, "load_queue_from_disc() failed to open directory \"%s\".  Error %d %s.  Fatal", spool_path, errno, strerror(errno));
      run = false;
   }

   _log(DEBUG, "load_queue_from_disc() returns %d", result);
   return result;
}

static int disc_queue_length(const word s)
{
   // returns number found, or -1 for error.
   // _log(PROC, "disc_queue_length(%d)", s);
   struct dirent **eps;
   int result, i;

   char spool_path[128];
   sprintf(spool_path, "%s/%s%d", STOMPY_SPOOL, debug?"d-":"", s);

   // Get some file names
   result = scandir (spool_path, &eps, is_a_buffer, alphasort);
   if(result >=0)
   {
      for(i = 0; i < result; i++)
      {
         free(eps[i]);
      }
      free(eps);
   }

   _log(PROC, "disc_queue_length(%d) return s %d.", s, result);
   return result;
}

static qword disc_queue_oldest(const word s)
{
   // returns timestamp of oldest disc buffer.  Units are microseconds.
   // Or 0 for none found or error.
   struct dirent **eps;
   qword result = 0;
   int n, i;

   char spool_path[128];
   sprintf(spool_path, "%s/%s%d", STOMPY_SPOOL, debug?"d-":"", s);

   // Get some file names
   n = scandir(spool_path, &eps, is_a_buffer, alphasort);

   if(n > 0)
   {
      result = atoll(eps[0]->d_name);
   }
   if(n >= 0)
   {
      for(i = 0; i < n; i++)
      {
         free(eps[i]);
      }
      free(eps);
   }
   return result;
}

static void log_message(const word s)
{
   FILE * fp;
   
   time_t now = time(NULL);
   struct tm * broken = gmtime(&now);
   
   
   if((fp = fopen(MESSAGE_LOG_FILEPATH, "a")))
   {
      fprintf(fp, "%02d/%02d/%02d %02d:%02d:%02dZ %s\n",
              broken->tm_mday, 
              broken->tm_mon + 1, 
              broken->tm_year % 100,
              broken->tm_hour,
              broken->tm_min,
              broken->tm_sec,
              stomp_topic_names[s]);
      fprintf(fp, "%s\n", client_buffer[s]->frame);
      fclose(fp);
   }
}

static word count_messages(const struct frame_buffer * const b)
{
   word i, count, depth;

   if (b->frame[0] != '[') return 1;

   i = 1;
   count = 0;
   depth = 0;

   while(b->frame[i])
   {
      if(b->frame[i] == '{')
      {
         depth++;
      }
      if(b->frame[i] == '}')
      {
         depth--;
         if(!depth) count++;
      }
      i++;
   }
   if(depth) _log(MINOR, "count_messages()  Depth = %d");
   return count;
}

static void heartbeat_tx(void)
{
   _log(PROC, "heartbeat_tx()");
   heartbeat_tx_due = now + HEARTBEAT_TX;

   if(stomp_manager_state == SM_RUN)
   {
      stomp_queue_tx("\n", 1);
      _log(DEBUG, "Queued heartbeat for transmission.");
   }
}
