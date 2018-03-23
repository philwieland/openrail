/*
    Copyright (C) 2016, 2017 Phil Wieland

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
#include <ctype.h>

#include "misc.h"
#include "db.h"
#include "database.h"
#include "build.h"

#define NAME  "limed"

#ifndef RELEASE_BUILD
#define BUILD "Z205p"
#else
#define BUILD RELEASE_BUILD
#endif

typedef enum {LVRPLSH, HUYTON, PAGES} page_t;

static word debug;
static char zs[4096];
static volatile word run;
static time_t now, start_time;

static char berths_lime[32][8];

// Time in hours (local) when daily statistical report is produced.
// (Set > 23 to disable daily report.)
#define REPORT_HOUR 4
#define REPORT_MINUTE 4

#define MAX_PAGE 4096
#define PAGE_LIMIT (MAX_PAGE - 512)
static char page[MAX_PAGE];
static word max_page;

#define MAX_CACHE 64
typedef struct {
   time_t t;
   word k;
   char d[PAGES][64];
   word c;
   char trust_id[16];
} train;
static train trains[MAX_CACHE];

// Map c value to class string
static char * class[3] = { "", " class=\"ecs\"", " class=\"freight\"" };

// ~34 minutes
#define CACHE_HOLD_TIME 2048
// ~8 minutes
#define CACHE_HOLD_TIME_SHORT 512
static word max_cache;

// Update interval in seconds.
#define UPDATE_INTERVAL 8
static const char * const targets[] = { "/var/www/LVRPLSH.html", "/var/www/HUYTON.html"};
//static const char * const targets[] = { "/var/www/friedbread/LVRPLSH.html", "/var/www/friedbread/HUYTON.html"};

static char top[2048];
static const char * const bot = "<button onclick=\"location.reload(true)\">Refresh</button></body></html>\n";

static void perform(void);
static void create_page_lime(void);
static void create_page_huyton(void);
static train * train_from_berth_value(const char * const v, const page_t p);
static train * train_from_hc(const char * const hc, const page_t p);
static train * train_from_time(const char * const hc, const page_t p);
static train * train_from_schedule(const dword id, const word key, const page_t p, const char * const hc);
static char * location_name(const char * const tiploc);
static word cache_next_free(void);
static train * dummy_cache(const char * const d, const word c);
static void write_page(const char * const target);
static word feed_bad(const char * const d);
static char * train_status(const train * const t);

// Signal handling
void termination_handler(int signum)
{
   if(signum != SIGHUP)
   {
      run = false;
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
   _log_init(debug?"/tmp/limed.log":"/var/log/garner/limed.log", debug?1:0);
   _log(GENERAL, "");
   _log(GENERAL, "%s %s", NAME, BUILD);

   // Enable core dumps
   struct rlimit limit;
   if(!getrlimit(RLIMIT_CORE, &limit))
   {
      limit.rlim_cur = RLIM_INFINITY;
      setrlimit(RLIMIT_CORE, &limit);
   }

   run = true;

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

   // DAEMONISE
   if(!debug)
   {
      int i=fork();
      if (i<0)
      {
         /* fork error */
         _log(ABEND, "fork() error.");
         exit(1);
      }
      if (i>0) exit(0); /* parent exits */
      /* child (daemon) continues */
      
      pid_t sid = setsid(); /* obtain a new process group */   
      if(sid < 0)
      {
         /* setsid error */
         _log(ABEND, "setsid() error.");
         exit(1);
      }

      for (i=getdtablesize(); i>=0; --i) close(i); /* close all descriptors */

      if ((i = open("/dev/null",O_RDONLY)) == -1)
      {
         _log(ABEND, "Failed to reopen stdin while daemonising (errno=%d)",errno);
         exit(1);
      }
      _log(GENERAL, "Re-open stdin returned %d", i);
      if ((i = open("/dev/null",O_WRONLY)) == -1) 
      {
         _log(ABEND, "Failed to reopen stdout while daemonising (errno=%d)",errno);
         exit(1);
      }
      _log(GENERAL, "Re-open stdout returned %d", i);
      if ((i = open("/dev/null",O_WRONLY)) == -1) 
      {
         _log(ABEND, "failed to reopen stderr while daemonising (errno=%d)",errno);
         exit(1);
      }
      _log(GENERAL, "Re-open stderr returned %d", i);

      umask(022); // Created files will be rw for root, r for all others

      i = chdir("/var/run/");  
      if(i < 0)
      {
         /* chdir error */
         _log(ABEND, "chdir() error.");
         exit(1);
      }
      
      if((lfp = open("/var/run/limed.pid", O_RDWR|O_CREAT, 0640)) < 0)
      {
         _log(ABEND, "Unable to open pid file \"/var/run/limed.pid\".");
         exit(1); /* can not open */
      }
         
      if (lockf(lfp,F_TLOCK,0)<0)
      {
         _log(ABEND, "Failed to obtain lock.");
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

   // Startup delay
   {
      struct sysinfo info;
      word logged = false;
      word i;
      while(run && !debug && !sysinfo(&info) && info.uptime < (512 + 192))
      {
         if(!logged) _log(GENERAL, "Startup delay...");
         logged = true;
         for(i = 0; i < 8 && run; i++) sleep(1);
      }
   }

   if(run) perform();

   _log(CRITICAL, "Terminated.");
   _log(GENERAL, "                Run time: %d days", (now - start_time)/(24*60*60)); 
   _log(GENERAL, "Highest used cache entry: %d", max_cache);
   _log(GENERAL, "    Largest page written: %d characters", max_page);

   if(lfp) close(lfp);

   return 0;
}

static void perform(void)
{
   time_t update_due = 0;
   word page = 0;
   word last_report_day;

   // Initialise database connection
   while(db_init(conf[conf_db_server], conf[conf_db_user], conf[conf_db_password], conf[conf_db_name]) && run) 
   {
      _log(CRITICAL, "Failed to initialise database connection.  Will retry...");
      word i;
      for(i = 0; i < 64 && run; i++) sleep(1);
   }

   database_upgrade(limed);

   // Initialise cache
   {
      word i;
      for(i = 0; i < MAX_CACHE; i++)
      {
         trains[i].k = 0;
      }
      max_cache = 0;
   }
   max_page = 0;

   {
      now = time(NULL);
      struct tm * broken = localtime(&now);
      last_report_day = broken->tm_wday;
      start_time = now;
   }


   strcpy(top, "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\" \"http://www.w3.org/TR/html4/loose.dtd\">\n"
          // "<html xmlns=\"http://www.w3.org/1999/xhtml\" lang=\"en\" xml:lang=\"en\">\n"
          "<head><meta http-equiv=\"Content-Type\" content=\"text/html;charset=utf-8\" >");
   sprintf(zs, "<title>%s %s</title>", NAME, BUILD);
   strcat(top, zs);
   strcat(top, 
"<link rel=\"stylesheet\" type=\"text/css\" href=\"/auxy/limed.css\">"
          //"<META HTTP-EQUIV=\"refresh\" CONTENT=\"32\">\n"
"</head>"
"<body>"
          );

   while(run)
   {
      while(run && (now = time(NULL)) < update_due)
      {
         sleep(1);
      }

      {
         struct tm * broken = localtime(&now);
         if(broken->tm_wday != last_report_day && broken->tm_hour >= REPORT_HOUR && broken->tm_min >= REPORT_MINUTE)
         {
            last_report_day = broken->tm_wday;
            _log(GENERAL, "");
            _log(GENERAL, "                Run time: %d days", (now - start_time)/(24*60*60)); 
            _log(GENERAL, "Highest used cache entry: %d", max_cache);
            _log(GENERAL, "    Largest page written: %d characters", max_page);
         }
      }

      if(++page >= PAGES) page = 0;
 
      _log(DEBUG, "Updating page %d ...", page);

      qword elapsed = time_ms();
      update_due = now + UPDATE_INTERVAL;

      switch(page)
      {
      case LVRPLSH: create_page_lime(); break;
      case HUYTON: create_page_huyton(); break;
      default: _log(CRITICAL, "ABEND: Invalid page %d", page); run = false; break;
      }

      elapsed = time_ms() - elapsed;
      _log((elapsed > 1999)?GENERAL:DEBUG, "Update of page %d took %s ms, highest used cache entry %d, largest page written:  %d characters.", page, commas_q(elapsed), max_cache, max_page);
   }
}

static void create_page_lime(void)
{
   word i;
   _log(PROC, "create_page_lime()");
   MYSQL_RES * result;
   MYSQL_ROW row;
   train * t;

   if(!db_query("SELECT k,v FROM td_states WHERE k LIKE 'XZb0AP%' OR k LIKE 'XZb0BP%'"))
   {
      result = db_store_result();
      while((row = mysql_fetch_row(result)) && run) 
      {
         word key = 0xffff;
         switch(row[0][4])
         {
         case 'B': key = 0x10 + row[0][6] - '0'; break;
         case 'A': key = 0x00 + row[0][6] - '0'; break;
         }
         if(key > 0x1f)
         {
            _log(CRITICAL, "Invalid berth key %d generated from k = \"%s\".", key, row[0]);
         }
         else
         {
            strcpy(berths_lime[key], row[1]);
         }
      }
      mysql_free_result(result);
   }
   // Header
   strcpy(page, top);
   strcat(page, "<table class=\"table\">");
   if(feed_bad("XZ")) strcat(page, "<tr class=\"table-alert\"><td colspan=\"4\">Data feed degraded.</td></tr>");
   strcat(page, "<tr class=\"table-p-head\"><th></th><th>Buffers</th><th colspan=\"2\">Front</th></tr>");

   for(i = 1; i < 10 && strlen(page) < PAGE_LIMIT; i++)
   {
      if((berths_lime[i][0] || berths_lime[0x10+i][0]) && i != 6)
      {
         t = train_from_berth_value(berths_lime[0x10+i], LVRPLSH);
         sprintf(zs, "<tr class=\"table-p\"><td>P%d</td><td%s>%s</td>", i, class[t->c], t->d[LVRPLSH]);
         strcat(page, zs);
         t = train_from_berth_value(berths_lime[i], LVRPLSH);
         sprintf(zs, "<td%s colspan=\"2\">%s</td></tr>", class[t->c], t->d[LVRPLSH]);
         strcat(page, zs);
      }
   }

   // Arrange a consistent read so that trains don't move between berths while we are reading them.
   if(db_start_transaction()) return;
   {
      char * berths[][3] = { {"XZb0006", "Slow", "Approaching"} ,
                             {"XZb0005", "Fast", "Approaching"} ,
                             {"XZb0003", "Slow", "Passed Edge Hill"} ,
                             {"XZb0001", "Fast", "Passed Edge Hill"} ,
                             {"XZbE053", "Slow", "Edge Hill"} ,
                             {"XZbE049", "Fast", "Edge Hill"} ,
                             {"XZbE045", "Slow", "Bootle BJ"} ,
                             {"XZbE043", "Fast", "Bootle BJ"} ,
                             {"",        "", ""  } };
      word berth_i = 0;
      word trains = 0;
      while(strlen(page) < PAGE_LIMIT && berths[berth_i][0][0] && run)
      {
         sprintf(zs, "SELECT v from td_states WHERE k = '%s' AND v != ''", berths[berth_i][0]);
         if(!db_query(zs))
         {
            result = db_store_result();
            if((row = mysql_fetch_row(result)))
            {
               if(!trains) strcat(page, "<tr class=\"table-a-head\"><th colspan=\"4\">Incoming</th></tr>");
               t = train_from_berth_value(row[0], LVRPLSH);
               sprintf(zs,"<tr class=\"table-a\"><td>%s</td><td %s>%s</td><td>%s</td><td>%s</td></tr>", berths[berth_i][1], class[t->c], t->d[LVRPLSH], berths[berth_i][2], train_status(t));
               strcat(page, zs);
               trains++;
            }
            mysql_free_result(result);
         }
         berth_i++;
      }
   }
   // End the consistent read.
   (void) db_commit_transaction();


   strcat(page, "<tr class=\"table-p\"><td colspan=\"4\"><span class=\"ecs\">&nbsp; E.C.S. &nbsp;</span> &nbsp; &nbsp; <span class=\"freight\">&nbsp; Freight &nbsp;</span> &nbsp; &nbsp; <b>&lt;</b> Arrival from</td></tr>");
   sprintf(zs, "<tr class=\"table-p\"><td colspan=\"4\">Updated %s by %s %s</td></tr>", time_text(now, true), NAME, BUILD);
   strcat(page, zs);
   strcat(page, "</table>");
   // Footer
   strcat(page, bot);

   // Write it
   write_page(targets[LVRPLSH]);
}

#define SHOW_TRAIN t = train_from_berth_value(row[0], HUYTON); \
   sprintf(zs,"<tr class=\"table-p\"><td>%d</td><td%s>%s</td><td>%s</td><td>%s</td></tr>", trains + 1, class[t->c], t->d[HUYTON], berths[berth_i][1], train_status(t)); \
   strcat(page, zs);

static void create_page_huyton(void)
{
   MYSQL_RES * result;
   MYSQL_ROW row;
   char huyton_route[64];
   char roby_route[64];
   train * t;
   _log(PROC, "create_page_huyton()");

   strcpy(page, top);
   strcat(page, "<table class=\"table\">");
   //strcat(page, "<tr class=\"table-p-head\"><th>&nbsp;</th><th>Train</th><th>Location</th></tr>");
   if(feed_bad("M1")) strcat(page, "<tr class=\"table-alert\"><td colspan=\"4\">Data feed degraded.</td></tr>");

   // Arrange a consistent read so that trains don't move between berths while we are reading them.
   if(db_start_transaction()) return;

   {
      char * berths[][2] = { {"M1b3587", "P1"} ,
                             {"M1b5579", "P2"} ,
                             {"M1b9588", "P2 UP"} ,
                             {"M1b3586", "P3"} ,
                             {"M1b9593", "P4 DN"} ,
                             {"M1b5580", "P4"} ,
                             {"",        ""  } };
      word berth_i = 0;
      word trains = 0;
      while(strlen(page) < PAGE_LIMIT && berths[berth_i][0][0] && trains < 4 && run)
      {
         sprintf(zs, "SELECT v from td_states WHERE k = '%s' AND v != ''", berths[berth_i][0]);
         if(!db_query(zs))
         {
            result = db_store_result();
            if((row = mysql_fetch_row(result)))
            {
               if(!trains) strcat(page, "<tr class=\"table-a-head\"><th colspan=\"4\">Huyton Station</th></tr>");
               t = train_from_berth_value(row[0], HUYTON);
               sprintf(zs,"<tr class=\"table-a\"><td>%s</td><td colspan=\"2\"%s>%s</td><td>%s</td></tr>", berths[berth_i][1], class[t->c], t->d[HUYTON], train_status(t));
               strcat(page, zs);
               trains++;
            }
            mysql_free_result(result);
         }
         berth_i++;
      }
   }

   // Up Route at Roby Jn
   strcpy(roby_route, "DB Error");
   if(strlen(page) < PAGE_LIMIT && !db_query("SELECT k,v FROM td_states WHERE k = 'M1s06'"))
   {
      result = db_store_result();
      if(result && (row = mysql_fetch_row(result)) && run) 
      {
         strcpy(roby_route, "None");
         word v = atoi(row[1]);
         if(v & 0x80)      strcpy(roby_route, "To fast line (P3)");
         else if(v & 0x40) strcpy(roby_route, "To slow line (P4)");
      }
      mysql_free_result(result);
   }

   {
      char * berths[][2] = { { "M1b3590", "Roby Fast"           } , 
                             { "M1b5582", "Roby Slow"           } ,
                             { "M1b3592", "App. Roby"           } ,
                             { "M1bE296", "Broad Green"         } ,
                             { "M1bE298", "Passed Olive Mount"  } ,
                             { "M1bE300", "Wavertree"           } ,
                             { "XZbZEHC", "Bootle BJ"           } ,
                             { ""       , ""                    } ,
      };

      word berth_i = 0;
      word trains = 0;
      word route = 0;
      while(strlen(page) < PAGE_LIMIT && berths[berth_i][0][0] && trains < 3 && run)
      {
         sprintf(zs, "SELECT v from td_states WHERE k = '%s' AND v != ''", berths[berth_i][0]);
         if(!db_query(zs))
         {
            result = db_store_result();
            if((row = mysql_fetch_row(result)))
            {
               if(!trains) strcat(page, "<tr class=\"table-p-head\"><th colspan=\"4\">UP from Liverpool</th></tr>");
               if(!route && berth_i > 1)
               {
                  sprintf(zs, "<tr class=\"table-p\"><td colspan=\"4\">Route selected: %s.</td></tr>", roby_route);
                  strcat(page, zs);
                  route++;
               }
               SHOW_TRAIN;
               trains++;
            }
            mysql_free_result(result);
         }
         berth_i++;
      }
   }

   {
      char * berths[][2] = { 
         { "M1b3755", "Approaching"     } ,
         { "M1b3749", "Prescot"         } ,
         { "M1b3745", "Thatto H"        } ,
         { "M1b3739", "Left St Helens"  } ,
         { "M1bS112", "Left St Helens"  } ,
         { "M1bS022", "St Helens"       } ,
         { ""       , ""                } ,
      };

      word berth_i = 0;
      word trains = 0;
      while(strlen(page) < PAGE_LIMIT && berths[berth_i][0][0] && trains < 3 && run)
      {
         sprintf(zs, "SELECT v from td_states WHERE k = '%s' AND v != ''", berths[berth_i][0]);
         if(!db_query(zs))
         {
            result = db_store_result();
            if((row = mysql_fetch_row(result)))
            {
               if(!trains) strcat(page, "<tr class=\"table-p-head\"><th colspan=\"4\">DOWN from St Helens</th></tr>");
               SHOW_TRAIN;
               trains++;
            }
            mysql_free_result(result);
         }
         berth_i++;
      }
   }

   // Down Chat Moss Route
   strcpy(huyton_route, "DB Error");
   if(strlen(page) < PAGE_LIMIT && !db_query("SELECT k,v from td_states WHERE k = 'M1s05'"))
   {
      result = db_store_result();
      if((row = mysql_fetch_row(result)) && run) 
      {
         strcpy(huyton_route, "None");
         word v = atoi(row[1]);
         if(v & 0x04)      strcpy(huyton_route, "To fast line (P1)");
         else if(v & 0x08) strcpy(huyton_route, "To slow line (P2)");
      }
      mysql_free_result(result);
   }

   // Down Chat Moss Trains
   {
      char * berths[][2] = { { "M1b3585", "Approaching"     } ,
                             { "M1b3581", "Passed Whiston"  } ,
                             { "M1b3579", "Whiston"         } ,
                             { "M1b3577", "Passed Rainhill" } ,
                             { "M1b3575", "Rainhill"        } ,
                             { "M1b3573", "Passed Lea Green"} ,
                             { "WAb0588", "Passed Lea Green"} ,
                             { "WAb0586", "Lea Green"       } ,
                             { "WAb0584", "St Helens Jn"    } ,
                             { "WAb0574", "App St Helens Jn"} ,
                             { "WAb0568", "Passed Sankey"   } ,
                             { "WAb0566", "Passed Sankey"   } ,
                             { "WAb0562", "Earlestown Loop" } ,
                             { "WAb0563", "Earlestown"      } ,
                             { ""       , ""                } ,
      };

      word berth_i = 0;
      word trains = 0;
      while(strlen(page) < PAGE_LIMIT && berths[berth_i][0][0] && trains < 3 && run)
      {
         sprintf(zs, "SELECT v from td_states WHERE k = '%s' AND v != ''", berths[berth_i][0]);
         if(!db_query(zs))
         {
            result = db_store_result();
            if((row = mysql_fetch_row(result)))
            {
               if(!trains)
               {
                  strcat(page, "<tr class=\"table-p-head\"><th colspan=\"4\">DOWN from Chat Moss</th></tr>");
                  sprintf(zs, "<tr class=\"table-p\"><td colspan=\"4\">Route selected: %s.</td></tr>", huyton_route);
                  strcat(page, zs);
               }
               SHOW_TRAIN;
               trains++;
            }
            mysql_free_result(result);
         }
         berth_i++;
      }
   }
            
   // End the consistent read.
   (void) db_commit_transaction();

   if(strlen(page) < PAGE_LIMIT)
   {
      strcat(page, "<tr class=\"table-p\"><td colspan=\"4\"><span class=\"ecs\">&nbsp; E.C.S. &nbsp;</span> &nbsp; &nbsp; <span class=\"freight\">&nbsp; Freight &nbsp;</span> &nbsp; &nbsp; <span class=\"pass\">&nbsp; Pass &nbsp;</span> &nbsp; &nbsp;<b>&lt;</b> From</td></tr>");
      sprintf(zs, "<tr class=\"table-p\"><td colspan=\"4\">Updated %s by %s %s</td></tr>", time_text(now, true), NAME, BUILD);
      strcat(page, zs);
   }
   else
   {
      strcat(page, "<tr class=\"table-p\"><td colspan=\"4\">PAGE BUFFER OVERFLOW</td></tr>");
      _log(CRITICAL, "Page buffer overflow.");
   }
   strcat(page, "</table>");
   // Footer
   strcat(page, bot);

   // Write it
   write_page(targets[HUYTON]);
}

static train * train_from_berth_value(const char * const v, const page_t p)
{
   if(isdigit(v[0]) && isupper(v[1]) && isdigit(v[2]) && isdigit(v[3]))
   {
      // Normal reporting number
      return train_from_hc(v, p);
   }
   else if(v[0] >= '0' && v[0] <= '2' && v[1] >= '0' && v[1] <= '9' && v[2] >= '0' && v[2] <= '5' && v[3] >= '0' && v[3]<= '9')
   {
      // Time
      return train_from_time(v, p);
   }
   return dummy_cache(v, 0);
}

static train * train_from_hc(const char * const hc, const page_t p)
{
   _log(PROC, "train_from_hc(\"%s\", %d)", hc, p);

   word i;
   MYSQL_RES * result0, * result1;
   MYSQL_ROW row0;
   dword schedule_id;
   char query[1024];
   char headcode[8];

   word key = 1 + hc[3] - '0' + 10*(hc[2] - '0') + 100*(hc[0] - '0') + 1000*(hc[1] - 'A' + 10);

   for(i = 0; i < MAX_CACHE; i++)
   {
      if(trains[i].k == key && trains[i].t > now) return &trains[i];
   }

   // Not in cache
   _log(DEBUG, "train_from_hc(\"%s\", %d) Cache miss, key %d.", hc, p, key);

   strcpy(headcode, hc);
   // Reverse the de-obfuscation process
   sprintf(query, "SELECT obfus_hc FROM obfus_lookup WHERE true_hc = '%s' ORDER BY created DESC LIMIT 1", hc);
   if(!db_query(query))
   {
      result0 = db_store_result();
      if((row0 = mysql_fetch_row(result0)) && row0[0][0]) 
      {
         _log(DEBUG, "Real headcode \"%s\", obfuscated headcode \"%s\" found in obfuscation lookup table.", headcode, row0[0]);
         strcpy(headcode, row0[0]);
      }
      mysql_free_result(result0);
   }

   sprintf(query, "SELECT cif_schedule_id, trust_id FROM trust_activation WHERE created > %ld AND SUBSTR(trust_id,3,4) = '%s' ORDER BY created DESC", now-(24*60*60), headcode);

   if(db_query(query))
   {
      _log(CRITICAL, "Database error 1.");
      return dummy_cache("Database error 1.", 2);
   }
   result0 = db_store_result();
   while((row0 = mysql_fetch_row(result0))) 
   {
      schedule_id = atol(row0[0]);

      _log(DEBUG, "Try schedule id %ld.", schedule_id);

      switch(p)
      {
      case LVRPLSH: sprintf(query, "SELECT * from cif_schedule_locations WHERE cif_schedule_id = %u AND tiploc_code = 'LVRPLSH'", schedule_id); break;
      case HUYTON:  
      default:      sprintf(query, "SELECT * from cif_schedule_locations WHERE cif_schedule_id = %u AND (tiploc_code = 'HUYTON' OR tiploc_code = 'HUYTJUN')", schedule_id); break;
      }

      if(db_query(query))
      {
         _log(CRITICAL, "Database error 2.");
         mysql_free_result(result0);
         // Need a special cache entry.
         return dummy_cache("Database error 2.", 2);
      }
      
      result1 = db_store_result();
      if(mysql_num_rows(result1))
      {
         // HIT!
         train * t = train_from_schedule(schedule_id, key, p, hc);
         strcpy(t->trust_id, row0[1]);
         mysql_free_result(result1);
         mysql_free_result(result0);
         return t;
      }
      else
      {
         mysql_free_result(result1);
      }
   }
   
   mysql_free_result(result0);

   // Failed to find train.
   word free = cache_next_free();
   page_t j;
   for(j = 0; j < PAGES; j++) sprintf(trains[free].d[j], "%s ?", hc);
   trains[free].k = key;
   trains[free].c = 0;
   trains[free].t = now + CACHE_HOLD_TIME_SHORT;
   if(free > max_cache) max_cache = free;
   _log(DEBUG, "Cache updated, recorded \"%s\", \"%s\" in cache entry %d, key %d.", trains[free].d[0], trains[free].d[1], free, key);
   
   return &trains[free];
}

static train * train_from_time(const char * const hc, const page_t p)
{
   // Only works on page lime
   static const char * days_runs[8] = {"runs_su", "runs_mo", "runs_tu", "runs_we", "runs_th", "runs_fr", "runs_sa", "runs_su"};
   char query[1024];
   word i;
   MYSQL_RES * result0;
   MYSQL_ROW row0;

   _log(PROC, "train_from_time(\"%s\", %d)", hc, p);

   word key = 1 + hc[3] - '0' + 10*(hc[2] - '0') + 100*(hc[0] - '0') + 1000*(hc[1] - '0');

   for(i = 0; i < MAX_CACHE; i++)
   {
      if(trains[i].k == key && trains[i].t > now) return &trains[i];
   }

   // Not in cache
   _log(DEBUG, "train_from_time(\"%s\", %d) Cache miss, key %d.", hc, p, key);

   struct tm broken = *localtime(&now);

   sprintf(query, "SELECT s.id FROM cif_schedules AS s INNER JOIN cif_schedule_locations AS l ON s.id = l.cif_schedule_id WHERE l.tiploc_code = 'LVRPLSH' AND l.departure = '%s' AND s.deleted > %ld AND (s.%s) AND (s.schedule_start_date <= %ld) AND (s.schedule_end_date >= %ld) ORDER BY LOCATE(s.CIF_stp_indicator, 'ONPC')",
           hc, now + (12*60*60), days_runs[broken.tm_wday], now + (12*60*60), now - (12*60*60));

   if(p == LVRPLSH && !db_query(query))
   {
      result0 = db_store_result();
      if((row0 = mysql_fetch_row(result0))) 
      {
         dword schedule_id = atol(row0[0]);
         mysql_free_result(result0);
         return train_from_schedule(schedule_id, key, p, "");
      }
      mysql_free_result(result0);
   }

   word free = cache_next_free();
   page_t j;
   for(j = 0; j < PAGES; j++) sprintf(trains[free].d[j], "%s ?", hc);
   trains[free].k = key;
   trains[free].c = 0;
   trains[free].t = now + CACHE_HOLD_TIME_SHORT;
   if(free > max_cache) max_cache = free;
   _log(DEBUG, "Cache updated, recorded \"%s\" \"%s\" in cache entry %d, key %d.", trains[free].d[0], trains[free].d[1], free, key);
   
   return &trains[free];
}

static train * train_from_schedule(const dword id, const word key, const page_t p, const char * const hc)
{
   _log(PROC, "train_from_schedule(%ld, %d)", id, key);
   word free;
   char query[1024];
   MYSQL_RES * result0;
   MYSQL_ROW row0;
   char time[64];
   word incoming = false;
   word down = false;
   char from[64], to[64], train[64];
   word type = 0;

   free = cache_next_free();

   page_t j;
   for(j = 0; j < PAGES; j++) strcpy(trains[free].d[j], "DB Error");
   strcpy(train, "DB Error");
   strcpy(trains[free].trust_id, "");

   // Type
   switch(hc[0])
   {
   case 0:
   case '1':
   case '2':
   case '9':
      type = 0; // Passenger or unknown
   break;
   case '5':
      type = 1; // ECS
      break;
   default:
      type = 2; // Freight
      break;
   }
   
   // Build lime description

   // Destination
   sprintf(query, "SELECT tiploc_code FROM cif_schedule_locations WHERE record_identity = 'LT' AND cif_schedule_id = %u", id);
   if(!db_query(query))
   {
      result0 = db_store_result();
      if((row0 = mysql_fetch_row(result0)))
      {
         _log(DEBUG, "query() %s", location_name(row0[0]));
         strcpy(to, location_name(row0[0]));
         if(!strcmp(row0[0], "LVRPLSH"))
         {
            incoming = true; 
            down = true;
         }
         else if(!strcmp(row0[0], "ALERTN"))
         {
            down = true;
         }
      }
      mysql_free_result(result0);
   
      // From
      if(incoming || down)
      {
         sprintf(query, "SELECT tiploc_code FROM cif_schedule_locations WHERE record_identity = 'LO' AND cif_schedule_id = %u", id);
         if(!db_query(query))
         {
            result0 = db_store_result();
            if((row0 = mysql_fetch_row(result0)))
            {
               strcpy(from, location_name(row0[0]));
            }
            mysql_free_result(result0);
         }
      }

      // Arrival time
      if(incoming)
      {
         sprintf(query, "SELECT public_arrival, arrival FROM cif_schedule_locations WHERE cif_schedule_id = %u AND tiploc_code = 'LVRPLSH'", id);
         if(!db_query(query))
         {
            result0 = db_store_result();
            if((row0 = mysql_fetch_row(result0)))
            {
               sprintf(train, "%s <b>&lt;</b> %s", show_time_text(row0[0][0]?row0[0]:row0[1]), from);
            }
            mysql_free_result(result0);
         }
      }
      else
         // Departure time
      {
         sprintf(query, "SELECT public_departure, departure FROM cif_schedule_locations WHERE cif_schedule_id = %u AND tiploc_code = 'LVRPLSH'", id);
         if(!db_query(query))
         {
            result0 = db_store_result();
            if((row0 = mysql_fetch_row(result0)))
            {
               sprintf(train, "%s %s", show_time_text(row0[0][0]?row0[0]:row0[1]), to);
            }
            mysql_free_result(result0);
         }
      }

      strcpy(trains[free].d[LVRPLSH], train);

      // Build huyton description
      // from, to, and down already calculated above.  from only set if down is true.

      // find time
      time[0] = '\0';
      sprintf(query, "SELECT public_departure, departure, pass FROM cif_schedule_locations WHERE (tiploc_code = 'HUYTON' OR tiploc_code = 'HUYTJUN') AND cif_schedule_id = %u ORDER BY tiploc_code DESC", id);
      if(!db_query(query))
      {
         result0 = db_store_result();
         if((row0 = mysql_fetch_row(result0)))
         {
            if(row0[0][0]) strcpy(time, show_time_text(row0[0]));
            else if(row0[1][0]) strcpy(time, show_time_text(row0[1]));
            else sprintf(time, "<span class=\"pass\">%s</span>", show_time_text(row0[2]));
         }
         mysql_free_result(result0);
      }

      if(down)
         sprintf(train, "%s <b>&lt;</b> %s",time, from);
      else
         sprintf(train, "%s %s",time, to);

      strcpy(trains[free].d[HUYTON], train);
   }
      
   trains[free].k = key;
   trains[free].c = type;
   trains[free].t = now + CACHE_HOLD_TIME;
   if(free > max_cache) max_cache = free;
   _log(DEBUG, "Cache updated, recorded \"%s\" \"%s\" in cache entry %d, key %d.", trains[free].d[0], trains[free].d[1], free, key);
   
   return &trains[free];
}

static char * location_name(const char * const tiploc)
{
   char r[32];
   static char response[32];
   char query[1024];
   MYSQL_RES * result;
   MYSQL_ROW row;

   // Max length of response is 20 chars (plus NULL)

   response[0] = '\0';
   r[0] = '\0';

   sprintf(query, "SELECT SUBSTR(name, 1, 20) FROM friendly_names_20 WHERE tiploc = '%s'", tiploc);
   if(!db_query(query))
   {
      result = db_store_result();
      if((row = mysql_fetch_row(result)))
      {
         strcpy(r, row[0]);
      }
      else
      {
         sprintf(query, "INSERT INTO friendly_names_20 VALUES('%s', '')", tiploc);
         db_query(query);
      }
      mysql_free_result(result);
   }

   if(!strlen(r))
   {
      sprintf(query, "SELECT SUBSTR(fn, 1, 20) FROM corpus WHERE tiploc = '%s'", tiploc);
      if(!db_query(query))
      {
         result = db_store_result();
         if((row = mysql_fetch_row(result)) && row[0][0])
         {
            strcpy(r, row[0]);
         }
         mysql_free_result(result);
      }
   }

   if(!strlen(r))
   {
      strcpy(response, tiploc);
   }
   else
   {
      // Abbreviate further
      word i,j;
      word mode = true;
      i = j = 0;
      while(r[i])
      {
         if(mode || isupper(r[i])) response[j++] = r[i];
         if(r[i++] == ' ') mode = false;
      }
      response[j] = '\0';
   }
   return response;
}

static word cache_next_free(void)
{
   word free = MAX_CACHE;
   time_t oldest = now + CACHE_HOLD_TIME;
   word i;

   for(i = 0; i < MAX_CACHE; i++)
   {
      if(!trains[i].k || trains[i].t < now) 
      {
         free = i;
         i = MAX_CACHE;
      }
      else if(trains[i].t < oldest)
      {
         free = i;
         oldest = trains[i].t;
      }
   }

   if(free < MAX_CACHE) return free;
   _log(ABEND, "cache_next_free() result = %d.", free);
   run = false;
   return 0; 
}

static train * dummy_cache(const char * const d, const word c)
{
   page_t j;
   word free = cache_next_free();
   trains[free].k = 0;
   trains[free].c = c;
   trains[free].trust_id[0] = '\0';
   for(j = 0; j < PAGES; j++) strcpy(trains[free].d[j], d);
   return &trains[free];
}

static void write_page(const char * const target)
{
   // Write it
   int fildes = open(target, O_WRONLY | O_CREAT | O_TRUNC, 0644);
   if(fildes > -1)
   {
      ssize_t r,sent;
      sent = 0;
      size_t length = strlen(page);
      if(length > max_page) max_page = length;

      while(length > sent)
      {
         r = write(fildes, page + sent, length - sent);
         if(r < 0)
         {
            _log(MAJOR, "File \"%s\" write failed, error %d.", target, errno);
            sent = length;
         }
         else
         {
            sent += r;
         }
      }
      close(fildes);
   }
   else
   {
      _log(MAJOR, "Failed to open file \"%s\" for writing.  Error %d", target, errno);
   }
}

static word feed_bad(const char * const d)
{
   char query[1024];
   MYSQL_RES * result;
   MYSQL_ROW row;
   time_t last_actual = 0;

   sprintf(query, "SELECT last_timestamp FROM describers WHERE id = '%s'", d);
   if(!db_query(query))
   {
      result = db_store_result();
      if((row = mysql_fetch_row(result)))
      {
         last_actual = atol(row[0]);
      }
      mysql_free_result(result);
   }
   return ((now - last_actual) > 96);
}

static char * train_status(const train * const t)
{
   static char reply[8];
   char query[1024];
   MYSQL_RES * result;
   MYSQL_ROW row;
   
   reply[0] = '\0';
   if(t->trust_id[0])
   {
      sprintf(query, "SELECT timetable_variation, flags FROM trust_movement WHERE trust_id = '%s' ORDER BY created DESC", t->trust_id);
      if(!db_query(query))
      {
         result = db_store_result();
         if((row = mysql_fetch_row(result)))
         {
            word flags = atoi(row[1]);
            switch(flags & 0x0018)
            {
            case 0x0000: sprintf(reply, "%sE", row[0]); break;
            case 0x0008: strcpy(reply, "OT"); break;
            case 0x0010: sprintf(reply, "%sL", row[0]); break;
            case 0x0018: strcpy(reply, "??");
            }
         }
         mysql_free_result(result);
      }
   }
   return reply;
}
