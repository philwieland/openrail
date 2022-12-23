/*
    Copyright (C) 2016, 2017, 2018, 2019 Phil Wieland

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

/*
  Note: Bytes served.
If the logformat selected in the apache configuration includes %O this is the number of bytes sent.
If the logformat uses %b then this is the number of bytes in the page, EXCLUDING the headers.

I believe sulzer uses %O while napier uses %b.
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
#include <stdarg.h>
#include <math.h>

#include "misc.h"
#include "build.h"

#define NAME "jiankong"
// jiankong is pinyin for Monitor

// Notes on what is counted.

// Requests resulting in a 4xx response are not counted except in the 4xx count and the unique IP count.
// Requests from Tarbock are not counted except in the unique IP count.
// Requests from recognised bots are not counted except in the bot count and the unique IP count.
// Requests for recognised open rail pages are only counted in one category.
// GET requests not covered by any of the above are counted as Other GET.  Only unfamiliar
// ones are also logged.
// Other log file entries not matching any of the above are silently ignored.

#ifndef RELEASE_BUILD
#define BUILD "1131p"
#else
#define BUILD RELEASE_BUILD
#endif

#define INPUT_FILE_friedbread   "/tmp/access.log"
#define INPUT_FILE_napier       "/var/log/apache2/zz-mass-hosting.access.log"
#define INPUT_FILE_sulzer       "/var/log/apache2/access.log"
#define INPUT_FILE_oakwood      "/var/log/apache2/access.log"
#define INPUT_FILE_arnosgrove   "/var/log/apache2/access.log"
#define INPUT_FILE_woodgreen    "/var/log/apache2/access.log"
#define INPUT_FILE_turnpike     "/var/log/apache2/access.log"
#define INPUT_FILE              ((host == oakwood)?INPUT_FILE_oakwood:((host == arnosgrove)?INPUT_FILE_arnosgrove:((host == woodgreen)?INPUT_FILE_woodgreen:((host == turnpike)?INPUT_FILE_turnpike:INPUT_FILE_friedbread))))


#define LINE_LENGTH      512
#define ANALYSE_INTERVAL 60

static time_t start_time, now, analyse_due, last_processed;
static off_t last_seek;

static word debug, run, interrupt, analyse_header;
static char zs[4096];

// Time in hours (local) when daily statistical report is produced.
// (Set > 23 to disable daily report.)
#define REPORT_HOUR 4
#define REPORT_MINUTE 5

// Stats
enum stats_types {Sum, Max, RA, NoList};
enum stats_categories {LogLines, Bytes, UniqueIP, GETLiverailPage, GETLiverailUpdate, GETLiverailTrain, GETLivesigUpdate, GETLivesigQuery, GETLivesigOther, GETQuery, GETOther, GETBot, Fail4xx, HttpQueuePeak, HttpQueueRA, ApacheThreadPeak, ApacheThreadRA, LoadAveragePeak, RXLast, RXBytes, TXLast, TXBytes, MAXstats};
enum stats_types stats_type[MAXstats] = {Sum, Sum, Sum, Sum,          Sum,              Sum,            Sum,              Sum,             Sum,             Sum,      Sum,     Sum,      Sum,    Max,           RA,          Max,              RA,             Max,             NoList, Sum,     NoList, Sum,     };
static qword period_stats[MAXstats];
static qword day_stats[MAXstats];
static qword grand_stats[MAXstats];
static const char * stats_category[MAXstats] = 
   {
      "Log lines analysed", 
      "Bytes served",
      "Unique IP",
      "Liverail page", "Liverail update", "Liverail train", "Livesig update", "Livesig query", "Livesig other", "Railquery", "Other page", "Robot", "Request fail 4xx", 
      "HTTP queue peak", "",
      "Apache threads peak", "",
      "5 min load average peak",
      "", "RX Bytes", "", "TX Bytes",
   };

// Hash
struct hash_e { struct hash_e * next;
   char key[64];
   qword count;
};
typedef struct hash_e * hash;
typedef struct hash_e hash_entry;

// IP hashes
static hash h_ip_period, h_ip_day; 

// Server
static enum hosts {friedbread, oakwood, arnosgrove, woodgreen, turnpike} host;

static void perform(void);
static void analyse(const word first_pass);
static void analyse_line(const char * const l);
static void daily_report(void);
static hash_entry * hash_add(hash * h, const char * const k);
static hash_entry * hash_find(const hash * const h, const char * const k);
static hash_entry * hash_find_add(hash * h, const char * const k);
#if 0
static word hash_remove(hash * h, const char * const k);
#endif
static word hash_clear(hash * h);
#if 0
static void hash_dump(const hash * const h);
#endif
static word hash_count(const hash * const h);
static void zero_stats(qword * s);
static void process_stats(qword * f, qword * t); 
static char * display_la(const qword n);

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
   _log_init(debug?"/tmp/jiankong.log":"/var/log/garner/jiankong.log", debug?1:0);
   //_log_init(debug?"/tmp/jiankong.log":"/var/log/garner/jiankong.log", 2);

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
      
      if((lfp = open("/var/run/jiankong.pid", O_RDWR|O_CREAT, 0640)) < 0)
      {
         _log(CRITICAL, "Unable to open pid file \"/var/run/jiankong.pid\".  Aborting.");
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

   // Identify server
   {
      char server[256];
      if(gethostname(server, sizeof(server))) strcpy(server, "(unknown host)");
      if(!strcmp(server, "friedbread")) host = friedbread;
      else if (!strcmp(server, "oakwood"))    host = oakwood;
      else if (!strcmp(server, "arnosgrove")) host = arnosgrove;
      else if (!strcmp(server, "woodgreen"))  host = woodgreen;
      else if (!strcmp(server, "turnpike"  )) host = turnpike;
      else
      {
         _log(ABEND, "Unrecognised server \"%s\".", server);
         exit(1);
      }
   }

   // Zero the stats
   {
      zero_stats(period_stats);
      zero_stats(day_stats);
      zero_stats(grand_stats);
   }

   // Startup delay
   {
      struct sysinfo info;
      word logged = false;
      word i;
      while(run && !debug && !sysinfo(&info) && info.uptime < (512 + 256))
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

   start_time = time(NULL);
   {
      struct tm * broken = localtime(&start_time);
      last_report_day = broken->tm_wday;
   }
   analyse_due = start_time - start_time%ANALYSE_INTERVAL + ANALYSE_INTERVAL;

   last_processed = 0;
   last_seek = 0;
   analyse_header = 999;
   // Unique IP lists
   h_ip_period = NULL;
   h_ip_day    = NULL;

   analyse(true);

   while(run)
   {   
      while( run && ((now = time(NULL)) < analyse_due))
      {
         sleep(1);
      }

      if(run)
      {
         analyse(false);
         analyse_due = now - now%ANALYSE_INTERVAL + ANALYSE_INTERVAL;
      }

      if(run)
      {
         struct tm * broken = localtime(&now);
         if(broken->tm_wday != last_report_day && broken->tm_hour >= REPORT_HOUR && broken->tm_min >= REPORT_MINUTE)
         {
            last_report_day = broken->tm_wday;
            daily_report();
         }
      }
   }

   if(interrupt)
   {
      _log(CRITICAL, "Terminating due to interrupt.");
   }

   daily_report();

}

static void analyse(const word first_pass)
{
   FILE * fp;
   char log_line[LINE_LENGTH];

   _log(PROC, "analyse()  last_seek = %lld", last_seek);

   if(!(fp = fopen(INPUT_FILE, "r")))
   {
      _log(GENERAL, "Unable to open Apache log file \"%s\".", INPUT_FILE);
      return;
   }

   if(last_seek)
   {
      struct stat file_stat;
      if(stat(INPUT_FILE, &file_stat))
      {
         _log(GENERAL, "Failed to stat Apache log file \"%s\".", INPUT_FILE);
         return;
      }
      if(file_stat.st_size < last_seek)
      {
         _log(GENERAL, "Apache log file is smaller, probably rotated.");
         last_seek = 0; 
      }
      else if(fseeko(fp, last_seek, SEEK_SET))
      {
         _log(GENERAL, "Apache log file seek failed.  File probably rotated.");
         last_seek = 0;
      }
   }

   while(fgets(log_line, sizeof(log_line), fp))
   {
      if(!first_pass)
      {
         period_stats[LogLines]++;
         analyse_line(log_line);
      }
   }

   last_seek = ftello(fp);
   _log(DEBUG, "Last seek = %lld", last_seek);

   fclose(fp);

   // Collect period stats
   FILE * pp;
   pp = popen("ps aux | grep ^www-data | wc -l", "r");
   if(pp)
   {
      char zz[16];
      if(fgets(zz, 16, pp))
      {
         period_stats[ApacheThreadPeak] = period_stats[ApacheThreadRA] = atoll(zz);
         _log(DEBUG, "Apache threads \"%s\" = %lld.", zz, period_stats[ApacheThreadRA]);
         if(first_pass) day_stats[ApacheThreadRA] = period_stats[ApacheThreadRA];
      }
      else
      {
         _log(MAJOR, "No output collecting Apache threads.");
      }
      pclose(pp);
   }
   else
   {
      _log(MAJOR, "popen() fail collecting Apache threads.");
   }

   switch(host)
   {
   case friedbread: pp = popen("ss -l | grep 963  | cut -c1-6  ", "r"); break;
   case oakwood:    pp = popen("ss -l | grep http | cut -c12-18", "r"); break;
   case arnosgrove: pp = popen("ss -l | grep http | cut -c19-25", "r"); break;
   case woodgreen:  pp = popen("ss -l | grep http | cut -c19-25", "r"); break;
   case turnpike:   pp = popen("ss -l | grep http | cut -c19-25", "r"); break;
   }

   if(pp)
   {
      char zz[16];
      if(fgets(zz, 16, pp))
      {
         period_stats[HttpQueuePeak] = period_stats[HttpQueueRA] = atoll(zz);
         _log(DEBUG, "HTTP Queue \"%s\" = %lld.", zz, period_stats[HttpQueueRA]);
         if(first_pass) day_stats[HttpQueueRA] = period_stats[HttpQueueRA];
      }
      else
      {
         period_stats[HttpQueuePeak] = period_stats[HttpQueueRA] = 0;
         _log(DEBUG, "HTTP Queue: no output = %lld.", zz, period_stats[HttpQueueRA]);
         if(first_pass) day_stats[HttpQueueRA] = period_stats[HttpQueueRA];
      }
      pclose(pp);
   }
   else
   {
      _log(MAJOR, "popen() fail collecting HTTP Queue.");
   }

   {
      double la[3];
      if(getloadavg(la, 3) == 3)
      {
         //                                         v-- 5 minute load average
         period_stats[LoadAveragePeak] = llround(la[1]*100);
      }
      else
      {
         _log(MAJOR, "getloadaverage() failed.");
      }
   }

   // RX stats
   switch(host)
   {
   case friedbread: pp = popen("cat /proc/net/dev | grep eth0 | cut -d: -f2 | tr -s ' ' | cut -d ' ' -f1", "r"); break;
   case oakwood:    pp = popen("cat /proc/net/dev | grep venet0 | cut -d: -f2 | tr -s ' ' | cut -d ' ' -f2", "r"); break;
   case arnosgrove: pp = popen("", "r"); break;
   case woodgreen:  pp = popen("cat /proc/net/dev | grep eth0 | cut -d: -f2 | tr -s ' ' | cut -d ' ' -f2", "r"); break;
   case turnpike:   pp = popen("cat /proc/net/dev | grep eth0 | cut -d: -f2 | tr -s ' ' | cut -d ' ' -f2", "r"); break;
   }
   if(pp)
   {
      char zz[32];
      if(fgets(zz, 24, pp))
      {
         qword value = atoll(zz);
         if(period_stats[RXLast] && (value > period_stats[RXLast]))
         {
            period_stats[RXBytes] = value - period_stats[RXLast];
         }
         else
         {
            period_stats[RXBytes] = 0;
         }
         period_stats[RXLast] = value;
         //_log(GENERAL, "value = %lld, RX Bytes = %lld.", value, period_stats[RXBytes]);
      }
      else
      {
         _log(MAJOR, "No output collecting network RX.");
      }
      pclose(pp);
   }
   else
   {
      _log(MAJOR, "popen() fail collecting network RX.");
   }

   // TX stats
   switch(host)
   {
   case friedbread: pp = popen("cat /proc/net/dev | grep eth0 | cut -d: -f2 | tr -s ' ' | cut -d ' ' -f9", "r"); break;
   case oakwood:    pp = popen("cat /proc/net/dev | grep venet0 | cut -d: -f2 | tr -s ' ' | cut -d ' ' -f10", "r"); break;
   case arnosgrove: pp = popen("", "r"); break;
   case woodgreen:  pp = popen("cat /proc/net/dev | grep eth0 | cut -d: -f2 | tr -s ' ' | cut -d ' ' -f10", "r"); break;
   case turnpike:   pp = popen("cat /proc/net/dev | grep eth0 | cut -d: -f2 | tr -s ' ' | cut -d ' ' -f10", "r"); break;
   }
   if(pp)
   {
      char zz[32];
      if(fgets(zz, 24, pp))
      {
         qword value = atoll(zz);
         if(period_stats[TXLast] && (value > period_stats[TXLast]))
         {
            period_stats[TXBytes] = value - period_stats[TXLast];
         }
         else
         {
            period_stats[TXBytes] = 0;
         }
         period_stats[TXLast] = value;
         //_log(GENERAL, "value = %lld, TX Bytes = %lld.", value, period_stats[TXBytes]);
      }
      else
      {
         _log(MAJOR, "No output collecting network TX.");
      }
      pclose(pp);
   }
   else
   {
      _log(MAJOR, "popen() fail collecting network TX.");
   }


   /*
   _log(GENERAL, "IP Hash contents period:");
   hash_dump(&h_ip_period);
   _log(GENERAL, "IP Hash contents today:");
   hash_dump(&h_ip_day);
   */   

   // Merge period stats into day ones
   process_stats(period_stats, day_stats);

   // Periodic report
   if(analyse_header++ > 47)
   {
      _log(GENERAL, "        |               |        |  - - - - - - - Excluding Tarbock Road and bot requests - - - - - - -  |        |        |                 |                 |        |             |             |");
      _log(GENERAL, "   Log  |       Bytes   | Unique |          Diagram         |         Liverail         |  rail- |  Other |    Bot | Request|  Apache threads |   HTTP queue    | 5m load|      Interface bytes      |");
      _log(GENERAL, "  lines |       served  |    IPs |   page | update |  query |   page | update |  train |  query |    GET |    GET |fail 4xx|    now       RA |    now       RA | average|       RX    |       TX    |");
      analyse_header = 0;
   }
   {
      char report[256], column[64];
      sprintf(column, "%7s |", commas_q(period_stats[LogLines]));
      strcpy(report, column);
      sprintf(column, "%14s |", commas_q(period_stats[Bytes]));
      strcat(report, column);
      sprintf(column, "%7s |", commas(hash_count(&h_ip_period)));
      strcat(report, column);
      //sprintf(column, "%7s |", commas(hash_count(&h_ip_day)));
      //strcat(report, column);
      sprintf(column, "%7s |", commas(period_stats[GETLivesigOther]));
      strcat(report, column);
      sprintf(column, "%7s |", commas(period_stats[GETLivesigUpdate]));
      strcat(report, column);
      sprintf(column, "%7s |", commas(period_stats[GETLivesigQuery]));
      strcat(report, column);
      sprintf(column, "%7s |", commas(period_stats[GETLiverailPage]));
      strcat(report, column);
      sprintf(column, "%7s |", commas(period_stats[GETLiverailUpdate]));
      strcat(report, column);
      sprintf(column, "%7s |", commas(period_stats[GETLiverailTrain]));
      strcat(report, column);
      sprintf(column, "%7s |", commas(period_stats[GETQuery]));
      strcat(report, column);
      sprintf(column, "%7s |", commas(period_stats[GETOther]));
      strcat(report, column);
      sprintf(column, "%7s |", commas(period_stats[GETBot]));
      strcat(report, column);
      sprintf(column, "%7s |", commas(period_stats[Fail4xx]));
      strcat(report, column);
      sprintf(column, "%7s  ", commas_q(period_stats[ApacheThreadPeak]));
      strcat(report, column);
      sprintf(column, "%7s |", commas_q(day_stats[ApacheThreadRA]));
      strcat(report, column);
      sprintf(column, "%7s  ", commas_q(period_stats[HttpQueuePeak]));
      strcat(report, column);
      sprintf(column, "%7s |", commas_q(day_stats[HttpQueueRA]));
      strcat(report, column);
      sprintf(column, "%7s |", display_la(period_stats[LoadAveragePeak]));
      strcat(report, column);
      sprintf(column, "%12s |", commas_q(period_stats[RXBytes]));
      strcat(report, column);
      sprintf(column, "%12s |", commas_q(period_stats[TXBytes]));
      strcat(report, column);

      _log(GENERAL, report);
   }

   // Examine RA figures in day_stats and instant figures in period_stats to see if we need to raise an alarm
   // todo

   zero_stats(period_stats);
   //if(hash_count(&h_ip_period) < 40) hash_dump(&h_ip_period);
   hash_clear(&h_ip_period);
}

static void analyse_line(const char * const l)
{
   _log(PROC, "analyse_line(\"%s\")", l);
   char from_ip[LINE_LENGTH], logname[LINE_LENGTH], user[LINE_LENGTH], timestamp[LINE_LENGTH], request[LINE_LENGTH], status[LINE_LENGTH], bytes[LINE_LENGTH], referrer[LINE_LENGTH], useragent[LINE_LENGTH];
   word valid = true;

   const char * i = l;
   char * j;

   // Parse

   if(host == friedbread)
   {
      // Skip extra field
      while(*i && *i != ' ') i++;
      while(*i && *i == ' ') i++;
   }

   j = from_ip;
   while(*i && *i != ' ') *j++ = *i++;
   *j = '\0';

   j = logname;
   while(*i && *i == ' ') i++;
   while(*i && *i != ' ') *j++ = *i++;
   *j = '\0';

   j = user;
   while(*i && *i == ' ') i++;
   while(*i && *i != ' ') *j++ = *i++;
   *j = '\0';

   j = timestamp;
   while(*i && *i != '[') i++;
   if(*i) i++;
   while(*i && *i != ']') *j++ = *i++;
   *j = '\0';
   if(*i) i++;

   _log(DEBUG, "From IP \"%s\", Logname \"%s\", User \"%s\", Timestamp \"%s\".", from_ip, logname, user, timestamp);

   time_t stamp = 0;
   struct tm broken;
   j = strptime(timestamp, "%d/%b/%Y:%H:%M:%S %z", &broken);
   if(j && !*j) stamp = mktime(&broken);

   _log(DEBUG, "Timestamp decodes as %ld %s.", stamp, time_text(stamp, true));
   if(!stamp) valid = false;

   j = request;
   while(*i && *i != '"') i++;
   if(*i) i++;
   while(*i && *i != '"') *j++ = *i++;
   *j = '\0';
   if(*i) i++;

   j = status;
   while(*i && *i == ' ') i++;
   while(*i && *i != ' ') *j++ = *i++;
   *j = '\0';

   j = bytes;  // Excluding headers
   while(*i && *i == ' ') i++;
   while(*i && *i != ' ') *j++ = *i++;
   *j = '\0';

   j = referrer;
   while(*i && *i != '"') i++;
   if(*i) i++;
   while(*i && *i != '"') *j++ = *i++;
   *j = '\0';
   if(*i) i++;

   j = useragent;
   while(*i && *i != '"') i++;
   if(*i) i++;
   while(*i && *i != '"') *j++ = *i++;
   *j = '\0';
   if(*i) i++;

   size_t b = atol(bytes);
   period_stats[Bytes] += b;
   _log(DEBUG, "Request \"%s\", Status \"%s\", Bytes \"%s\" (%ld)", request, status, bytes, b);
   
   if(valid)
   {
      // Update database
      hash_entry * e;

      // Make special IP key
      // TODO

      if((e = hash_find_add(&h_ip_day,    from_ip))) e->count++;
      if((e = hash_find_add(&h_ip_period, from_ip))) e->count++;
      
      // Analyse requests
      // Exclude Tarbock Road and cockfosters (jiancha)
      if(!strcmp(from_ip, "80.229.14.237") || !strcmp(from_ip, "162.220.240.27"))
      {
         // Ignore 
      }

      else if(status[0] == '4') 
      {
         period_stats[Fail4xx]++;
         if(strcmp(request, "-") || strcmp(status, "408")) // Don't log the "-" 408 0 "-" "-" entries
         {
            _log(MINOR, "Request \"%s\" returned status \"%s\".", request, status);
         }
      }

      // Check for bots
      else if(strcasestr(useragent, "bingbot")     ||
              strcasestr(useragent, "Baiduspider") ||
              strcasestr(useragent, "Googlebot")   ||
              strcasestr(useragent, "BLEXBot")     ||
              strcasestr(useragent, "YandexBot")   ||
              strcasestr(useragent, "DotBot")      ||
              strcasestr(useragent, "SemrushBot")  ||
              strcasestr(useragent, "adscanner")   ||
              strcasestr(useragent, "AhrefsBot")) period_stats[GETBot]++;

      //                                  1         2         3         4
      //                         1234567890123456789012345678901234567890
      else if(!strncmp(request, "GET /rail/liverail/train",  24)) period_stats[GETLiverailTrain]++;
      else if(!strncmp(request, "GET /rail/liverail/panelu", 25)) period_stats[GETLiverailUpdate]++;
      else if(!strncmp(request, "GET /rail/liverail/sumu",   23)) period_stats[GETLiverailUpdate]++;
      else if(!strncmp(request, "GET /rail/liverail/depu",   23)) period_stats[GETLiverailUpdate]++;
      else if(!strncmp(request, "GET /rail/liverail/panel",  24)) period_stats[GETLiverailPage]++;
      else if(!strncmp(request, "GET /rail/liverail/sum",    22)) period_stats[GETLiverailPage]++;
      else if(!strncmp(request, "GET /rail/liverail/dep",    22)) period_stats[GETLiverailPage]++;
      else if(!strncmp(request, "GET /rail/liverail/frt",    22)) period_stats[GETLiverailPage]++;
      else if(!strncmp(request, "GET /rail/liverail/full",   23)) period_stats[GETLiverailPage]++;
      else if(!strncmp(request, "GET /rail/livesig/U",       19)) period_stats[GETLivesigUpdate]++;
      else if(!strncmp(request, "GET /rail/livesig/Q",       19)) period_stats[GETLivesigQuery]++;
      else if(!strncmp(request, "GET /rail/livesig/",        18)) period_stats[GETLivesigOther]++;
      else if(!strncmp(request, "GET /rail/query/",          16)) period_stats[GETQuery]++;
      else if(!strncmp(request, "GET ", 4))
      {
         period_stats[GETOther]++;
         // Don't log some expected ones.  (Still counted in Other total.)
         if(strncmp(request, "GET /auxy/liverail.js",      20) &&
            strncmp(request, "GET /auxy/liverail.css",     21) &&
            strncmp(request, "GET /rail/liverail/jiancha", 25) &&
            strncmp(request, "GET /favicon.ico",           15) &&
            strncmp(request, "GET /auxy/livesig.",         16) &&
            strncmp(request, "GET /auxy/railquery.js",     21) &&
            strncmp(request, "GET / ",                      5) &&
            strncmp(request, "GET /robots.txt",            14) &&
            strncmp(request, "GET /rail/liverail ",        18) )
 
            _log((GENERAL), "GET Unrecognised:  \"%s\", status \"%s\".", request, status);
      }
   }
}

static void daily_report(void)
{
   char zs[128];
   word i;
   char report[2048];

   _log(GENERAL, "");
   sprintf(zs, "%25s: %-16s Total", "", "Day");
   _log(GENERAL, zs);
   strcpy(report, zs);
   strcat(report, "\n");

   sprintf(zs, "%25s: %-16s %ld days", "Run time", "", (time(NULL) - start_time)/(24*60*60));
   _log(GENERAL, zs);
   strcat(report, zs);
   strcat(report, "\n");
   process_stats(day_stats, grand_stats); 
   day_stats[UniqueIP] = hash_count(&h_ip_day);
   for(i=0; i<MAXstats; i++)
   {
      if(stats_type[i] != NoList && stats_type[i] != RA)
      {
         if(i == LoadAveragePeak)
         {
            sprintf(zs, "%25s: %-16s ", stats_category[i], display_la(day_stats[i]));
            strcat(zs, display_la(grand_stats[i]));
         }
         else
         {
            sprintf(zs, "%25s: %-16s ", stats_category[i], commas_q(day_stats[i]));
            if(i != UniqueIP) strcat(zs, commas_q(grand_stats[i]));
         }
         _log(GENERAL, zs);
         strcat(report, zs);
         strcat(report, "\n");
      }
   }
   email_alert(NAME, BUILD, "Statistics Report", report);
   zero_stats(day_stats);
   hash_clear(&h_ip_day);
}

static hash_entry * hash_add(hash * h, const char * const k)
{
   // N.B. Does not detect nor prevent duplicate keys
   _log(PROC, "hash_add(~,\"%s\")", k);
   if(strlen(k) > 60) 
   {
      _log(MAJOR, "hash_add() called with invalid key.");
      return NULL;
   }
   hash_entry * n = (hash_entry *) malloc(sizeof(hash_entry));
   hash_entry * e;
   hash_entry * p;
   if(!n)
   {
      _log(CRITICAL, "hash_add() failed to allocate memory.");
      return NULL;
   }

   // Fill new entry
   strcpy(n->key, k);
   n->count = 0; 

   // Insert it in the hash
   if(!*h)
   {
      _log(DEBUG, "hash_add() adding first entry, key = \"%s\".", k);
      *h = n;
      n->next = NULL;
   }
   else
   {
      e = *h;
      if(strcmp(e->key, k) >= 0) 
      {
         // Goes in first
         n->next = e;
         *h = n;
      }
      else
      {
         while(e && strcmp(e->key, k) < 0) 
         {
            p = e;
            e = e->next;
         }
         n->next = e;
         p->next = n;
      }
   } 
   return n;
}

static hash_entry * hash_find(const hash * const h, const char * const k)
{
   int i;
   hash_entry * e = *h;
   while(e)
   {
      if(!(i = strcmp(e->key, k))) return e;
      if(i > 0) return NULL;
      e = e->next;
   }
   return NULL;
}

static hash_entry * hash_find_add(hash * h, const char * const k)
{
   // Return found entry, or a zero'ed new one
   hash_entry * r = hash_find(h, k);
   if(r)
   {
      _log(DEBUG, "hash_find_add(, \"%s\") found existing entry.", k);
      return r;
   }
   _log(DEBUG, "hash_find_add(, \"%s\") created new entry.", k);
   return hash_add(h, k);
}

#if 0
static word hash_remove(hash * h, const char * const k)
{
   // return 0 for success, 1 for not found.
   int i;
   hash_entry * p;
   hash_entry * e = *h;
   while(e)
   {
      if(!(i = strcmp(e->key, k)))
      {
         // Hit
         if(e == *h)
            *h = e->next;
         else
            p->next = e->next;
         free(e);
         return 0; 
      }
      if(i > 0) return 1;
      p = e;
      e = e->next;
   }
   return 1;  // Not found
}
#endif

static word hash_clear(hash * h)
{
   // Clears out all entries.
   hash_entry * d;
   hash_entry * e = *h;
   while(e)
   {
      d = e->next;
      free(e);
      e = d; 
   }
   *h = NULL;
   return 0;
}

#if 0
static void hash_dump(const hash * const h)
{
   hash_entry * e = *h;
   while(e)
   {
      _log(GENERAL, "   %10lld \"%s\"", e->count, e->key);
      e = e->next;
   }
}
#endif

static word hash_count(const hash * const h)
{
   _log(PROC, "hash_count(%ld) ->%ld", h, *h);
   word result = 0;
   hash_entry * e = *h;
   while(e)
   {
      result++;
      e = e->next;
   }
   _log(DEBUG, "hash_count() returns %d", result);
   return result;
}

static void zero_stats(qword * s)
{
   word i;
   for(i = 0; i < MAXstats; i++)
   {
      switch(stats_type[i])
      {
      case Sum:
      case Max:
      case RA:
         s[i] = 0;
         break;
      case NoList:
         break;
      }
   }
}

static void process_stats(qword * f, qword * t)
{
   word i;
   qword new_ti;
   for(i = 0; i < MAXstats; i++)
   {
      switch(stats_type[i])
      {
      case Sum: t[i] += f[i]; break;
      case Max: if(f[i] > t[i]) t[i] = f[i]; break;
      case RA:
         new_ti = (t[i]*9 + f[i]) / 10; 
         t[i] = new_ti + ((new_ti < f[i])?1:0); 
         break;
      case NoList: break;
      }
   }
}

static char * display_la(const qword n)
{
   static char result[64];

   sprintf(result, "%llu.%02llu", n/100, n%100);

   return result;
}

