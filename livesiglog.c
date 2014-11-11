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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <regex.h>
#include <time.h>
#include <mysql.h>
#include <unistd.h>

#include "misc.h"
#include "db.h"


#define NAME "livesiglog"
#define BUILD "VA19"

static word analyse(const char * const filename);
static word analyse_line(const char * const line);

static regex_t match;
static time_t when;
static word debug;

#define USERS 512
static struct {
   dword key;
   char name [32];
   time_t start_time, last_on, elapsed;
   word on_line, visits;
} database[USERS];
static word database_next;

#define on(a,b) database[(a)].start_time = (b); database[(a)].last_on = (b); database[(a)].on_line = true; database[(a)].visits++;
#define off(a)  database[(a)].elapsed += (database[(a)].last_on - database[(a)].start_time); database[(a)].on_line = false;

int main(int argc, char **argv)
{
   int c;
   word usage = false;
   debug = false;

   while ((c = getopt (argc, argv, ":d")) != -1)
   {
      switch (c)
      {
      case 'd':
         debug = true;
         break;
      case ':':
         break;
      case '?':
      default:
         usage = true;
         break;
      }
   }

   // Yesterday
   {
      struct tm broken;
      when = time(NULL);
      broken = *localtime(&when);
      broken.tm_hour = 12;
      when = timelocal(&broken);
      when -= 24*60*60;
      // When is now noon yesterday +- 1 hour 
   }

   if(usage)
   {
      printf("\tUsage: %s [-d]\n\n", argv[0] );
      exit(1);
   }

   char host[256];
   if(gethostname(host, sizeof(host))) strcpy(host, "(unknown host)");
   printf("\nReport from %s build %s at %s\n\n", NAME, BUILD, host);

   struct tm * broken = localtime(&when);
   char date[32];
   strftime(date, 32, "%d/%b/%Y", broken);
   printf("Analysing logs for %s\n", date);

   // Create regex and compile it
   // 88.215.61.57 - - [18/Oct/2014:16:43:44 +0100] "GET /rail/livesig/U/1594 HTTP/1.1" 200 334 "http://www.charlwoodhouse.co.uk/rail/livesig" "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)"
   char regex[256];
   sprintf(regex, "^([[:digit:]]{1,3}\\.[[:digit:]]{1,3}\\.[[:digit:]]{1,3}\\.[[:digit:]]{1,3}).+\\[(%s:[:,[:digit:]]{8}.+)\\].+/rail/livesig/U/", date);
   
   if(regcomp(&match, regex, REG_ICASE + REG_EXTENDED))
   {
      printf("FATAL - Failed to compile regex.  \"%s\"\n", regex);
      exit(2);
   }
   if(debug) printf("Regex = \"%s\"\n", regex);

   // Initialise database
   database_next = 0;

   if(!analyse("/var/log/apache2/access.log.1") && !analyse("/var/log/apache2/access.log"))
   {
      // Final scan and print report
      printf("\n         IP       Visits Hours\n");
      word index;
      time_t hours, total;
      total = 0;
      for(index = 0; index < database_next; index++)
      {
         // Finalise database
         if(database[index].on_line) off(index);

         // Print report
         hours = (database[index].elapsed + 30 * 60)/3600;
         total += database[index].elapsed;
         printf("%16s  %-6d %ld\n", database[index].name,  database[index].visits, hours);
      }
      hours = (total + 30 * 60)/3600;
      printf("%d visitors logged, total hours %ld\n", database_next, hours);
   }

   printf("\n");

   exit(0);
}

static word analyse(const char * const filepath)
{
   char line[256];
   // Return error code or 0 for success. 
   FILE * fp;

   if((fp = fopen(filepath, "r")))
   {
      printf("Analysing \"%s\".\n", filepath);
      while(fgets(line, sizeof(line), fp) && !analyse_line(line));
      fclose(fp);
   }
   else
   {
      printf("Failed to open \"%s\" for reading.\n", filepath);
      return 1;
   }
   return 0;
}

static word analyse_line(const char * const line)
{
   // Return error code or 0 for success.
   time_t timestamp;
   regmatch_t matches[8];
   if(debug) printf("%s", line);

   if(regexec(&match, line, 4, matches, 0))
   {
      // Miss
      return 0;
   }
   
   char ip[32], datestamp[32];
   extract_match(line, matches, 1, ip, sizeof(ip));
   extract_match(line, matches, 2, datestamp, sizeof(datestamp));

   // Deduce ip-key
   dword ip_key = atol(ip);
   word index = 0;
   while(ip[index++] != '.');
   ip_key = ip_key * 0x100 + atol(ip + index++);
   while(ip[index++] != '.');
   ip_key = ip_key * 0x100 + atol(ip + index++);
   while(ip[index++] != '.');
   ip_key = ip_key * 0x100 + atol(ip + index++);

   // Deduce timestamp
   {
      struct tm broken;
      broken.tm_isdst = -1;
      if(!strptime(datestamp, "%d/%b/%Y:%H:%M:%S %z", &broken))
      {
         printf("Failed to parse timestamp \"%s\".", datestamp);
         return 2;
      }
      timestamp = timelocal(&broken);
      if(debug) printf("Parsed \"%s\" to get %ld %s\n", datestamp, timestamp, time_text(timestamp, false));
   }
   if(debug) printf("HIT:  IP=\"%s\" datestamp=\"%s\" key=0x%lx deduced timestamp = %s\n", ip, datestamp, ip_key, time_text(timestamp, false));

   // In database?
   for(index = 0; index < database_next && database[index].key != ip_key; index++);
   if(index < database_next)
   {
      // Existing visitor
      if(database[index].on_line)
      {
         if(database[index].last_on < timestamp - 32)
         {
            off(index);
            on(index, timestamp);
         }
         else
         {
            database[index].last_on = timestamp;
         }
      }
      else
      {
         on(index, timestamp);
      }
   }
   else
   {
      // New visitor
      if(database_next >= USERS - 1)
      {
         printf("Run out of space in database, more than %d visitors.  Aborting.", USERS);
         return 1;
      }
      if(debug) printf("Adding %s to database.\n", ip);
      database[database_next].key = ip_key;
      database[database_next].elapsed = 0;
      database[database_next].visits = 0;
      strcpy(database[database_next].name, ip);
      on(database_next, timestamp);
      database_next++;
   }

   return 0;
}

