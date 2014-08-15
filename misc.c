/*
    Copyright (C) 2013, 2014 Phil Wieland

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
#include <time.h>
#include <unistd.h>
#include <sys/timex.h>
#include <stdarg.h>
#include "misc.h"

static char log_file[512];
static word debug;

/* Public data */
conf_t conf;

char * time_text(const time_t time, const byte local)
{
   struct tm * broken;
   static char result[32];

   broken = local?localtime(&time):gmtime(&time);
      
   sprintf(result, "%02d/%02d/%02d %02d:%02d:%02d%s",
           broken->tm_mday, 
           broken->tm_mon + 1, 
           broken->tm_year % 100,
           broken->tm_hour,
           broken->tm_min,
           broken->tm_sec, 
           local?"":"Z");
   return result;
}

char * date_text(const time_t time, const byte local)
{
   static char result[32];
   strcpy(result, time_text(time, local));
   result[8] = '\0';
   return result;
}

time_t parse_datestamp(const char * string)
{
   // ONLY works for yyyy-mm-dd
   // Return 0 for failure to parse
   // Returns the time_t value for 12:00Z on the date in question.

   char zs[128];
   if(strlen(string) != 10) return 0;
   strcpy(zs, string);
   strcat(zs, "T12:00:00Z");
   return parse_timestamp(zs);
}
                               
time_t parse_timestamp(const char * string)
{
   // ONLY works for yyyy-mm-ddThh:mm:ssZ
   // Return 0 for failure to parse

   char zs[128];

   if(strlen(string) < 20 || strlen(string) > 64) return 0;
   if(string[19] != 'Z') return 0;
                
   strcpy(zs, string);

   zs[19] = '\0';

   struct tm broken;

   char * rc = strptime(zs, "%FT%T", &broken);

   if(rc == NULL || *rc != '\0') return 0;

   return timegm(&broken);
}

void _log(const byte level, const char * text, ...)
{
   char log[32];
   FILE * fp;

   if(debug == 3) return;
   if((level == PROC || level == DEBUG) && !debug) return;

   va_list vargs;
   va_start(vargs, text);

   time_t now = time(NULL);
   struct tm * broken = gmtime(&now);
      
   sprintf(log, "%02d/%02d/%02d %02d:%02d:%02dZ ",
           broken->tm_mday, 
           broken->tm_mon + 1, 
           broken->tm_year % 100,
           broken->tm_hour,
           broken->tm_min,
           broken->tm_sec);

   if(debug)
   {
      switch(level)
      {
      case GENERAL: strcat(log, "[GENERAL] "); break;
      case PROC:    strcat(log, "[PROC   ] "); break;
      case DEBUG:   strcat(log, "[DEBUG  ] "); break;
      case MINOR:   strcat(log, "[MINOR  ] "); break;
      case MAJOR:   strcat(log, "[MAJOR  ] "); break;
      case CRITICAL:strcat(log, "[CRIT.  ] "); break;
      default:      strcat(log, "[       ] "); break;
      }
   }
   else
   {
      strcat(log, "] ");
      if(level == MINOR   ) strcat(log, "MINOR: ");
      if(level == MAJOR   ) strcat(log, "MAJOR: ");
      if(level == CRITICAL) strcat(log, "CRITICAL: ");
   }

   if(!debug)
   {
      if(log_file[0] && (fp = fopen(log_file, "a")))
      {
         fprintf(fp, "%s", log);
         vfprintf(fp, text, vargs);
         fprintf(fp, "\n");
         fclose(fp);
      }
   }
   else
   {
      if(debug == 1) 
      {
         printf("%s", log);
         vprintf(text, vargs);
         printf("\n");
      }
      if(log_file[0] && (fp = fopen(log_file, "a")))
      {
         fprintf(fp, "%s", log);
         vfprintf(fp, text, vargs);
         fprintf(fp, "\n");
         fclose(fp);
      }
   }

   va_end(vargs);

   return;
}


void _log_init(const char * l, const word d)
{
   // d is debug mode. 
   // 0 Normal running
   // 1 debug, print as well as log file
   // 2 debug, no print
   // 3 No logging at all.
   if(strlen(l) < 500) strcpy(log_file, l);
   else log_file[0] = '\0';
   debug = d;
}

char * commas(const dword n)
{
   static char result[32];
   char zs[32];
   word i,j,k;

   sprintf(zs, "%ld", n);
   k = strlen(zs);

   j = 0;
   for(i = 0; i < k && j<30; i++)
   {
      result[j++] = zs[i];
      if(((i % 3) == ((k-1) % 3)) && (i < k - 1)) result[j++] = ',';
   }
   result[j] = '\0';

   return result;
}

char * commas_q(const qword n)
{
   static char result[64];
   char zs[64];
   word i,j,k;

   sprintf(zs, "%lld", n);
   k = strlen(zs);

   j = 0;
   for(i = 0; i < k && j<30; i++)
   {
      result[j++] = zs[i];
      if(((i % 3) == ((k-1) % 3)) && (i < k - 1)) result[j++] = ',';
   }
   result[j] = '\0';

   return result;
}

char * show_spaces(const char * string)
{
   // Replace spaces in a string with an inverted delta.  String truncated if too long.
   static char result[128];
   word i,j;

   for(j=i=0; string[i] && j < 127; i++)
   {
      if(string[i] == ' ')
      {
         result[j++] = '&';
         result[j++] = 'n';
         result[j++] = 'a';
         result[j++] = 'b';
         result[j++] = 'l';
         result[j++] = 'a';
         result[j++] = ';';
      }
      else
      {
         result[j++] = string[i];
      }
   }
   result[j++] = '\0';
   return result;
}

word email_alert(const char * const name, const char * const build, const char * const title, const char * const message)
{
   FILE * fp;
   char command[1024], tmp_file[256], host[256];
   int i;

   _log(PROC, "email_alert()");

   if(!conf.report_email[0]) return 0;

   sprintf(tmp_file, "/tmp/email-%lld", time_us());

   if(gethostname(host, sizeof(host))) strcpy(host, "(unknown host)");

   while(!(fp=fopen(tmp_file, "w")))
   {
      if(strlen(tmp_file) < 40) strcat(tmp_file, "x");
      else
      {
         _log(MAJOR, "email_alert():  Failed to open email file.");
         return 1;
      }
   }

   fprintf(fp, "Report from %s build %s at %s\n\n%s\n", name, build, host, message);
   fclose(fp);
   sprintf(command, "/bin/cat %s | /usr/bin/mail -s \"[openrail-%s] %s\" %s >>/tmp/email_alert.log 2>&1", 
           tmp_file, name, title, conf.report_email);
   i = system(command);
   
   _log(DEBUG, "email_alert():  system(\"%s\") returned %d", command, i);

   unlink(tmp_file);
           
   // Success
   return 0;
}

char * abbreviated_host_id(void)
{
   // Return a "unique name" of this host, so that more than one machine can connect to the stream without an id clash
   // Currently this is just the first part of the FQDN but it could be something more clever in the future.
   static char hostname[256];
   word i;

   gethostname(hostname, sizeof(hostname));

   for(i=0; hostname[i] && hostname[i] != '.'; i++);
   hostname[i] = '\0';

   _log(DEBUG, "abbreviated_host_id() return value \"%s\".", hostname);

   return hostname;
}

char * show_time(const char * const input)
{
   static char output[16];

   switch(strlen(input)) 
   {
   case 1:
      if (input[0] == 'H')
         strcpy(output, "&half;");
      else
         strcpy(output, input);
      break;

   case 2:
      strcpy(output, input);
      if(output[1] == 'H')
      {
         strcpy(output+1, "&half;");
      }
      break;
   case 4:
   case 5:
      // hh:mmH
      output[0] = input[0];
      output[1] = input[1];
      output[2] = ':';
      output[3] = input[2];
      output[4] = input[3];
      output[5] = '\0';
      if(input[4] == 'H')
      {
         strcat(output, "&half;");
      }
      break;

   default:
      strcpy(output, input);
      break;
   }

   return output;
}

char * show_time_text(const char * const input)
{
   static char output[16];

   switch(strlen(input)) 
   {
   case 4:
   case 5:
      // hh:mmH
      output[0] = input[0];
      output[1] = input[1];
      output[2] = ':';
      output[3] = input[2];
      output[4] = input[3];
      output[5] = input[4];
      output[6] = '\0';
      break;

   default:
      strcpy(output, input);
      break;
   }

   return output;
}

#define CONFIG_SIZE 2048
int load_config(const char * const filepath)
{
   char *line_start, *val_start, *val_end, *line_end, *ic;
   static char buf[CONFIG_SIZE];

   FILE *cfg;
   if((cfg = fopen(filepath, "r")))
   {
      ssize_t z = fread(buf, 1, CONFIG_SIZE, cfg);
      fclose(cfg);
      if(z < 1) return 2;
   }
   else
      return 1;

   buf[CONFIG_SIZE - 1] = '\0';

   conf.db_server = conf.db_name = conf.db_user = conf.db_pass = conf.nr_user = conf.nr_pass = conf.debug = conf.report_email = &buf[CONFIG_SIZE - 1];
   conf.stomp_topics = conf.stomp_topic_names = &buf[CONFIG_SIZE - 1];

   line_start=buf;
   while(1)
   {
      ic = strchr(line_start, ':');
      line_end = strchr(line_start, '\n');
      
      // config is finished
      if (line_end == NULL)
         return 0;

      if(*line_start == '\n' || *line_start == '#')
      {
         // Blank line or comment
      }
      else
      {
         if(!ic || ic > line_end)
         {
            // Colon missing.  Error.
            return 3;
         }
         val_start = ic;
         val_end = line_end;
         while (*(--ic) == ' ');
         *(++ic) = 0;
         while (*(++val_start) == ' ');
         while (*(--val_end) == ' ');
         *(++val_end) = 0;

         if(*val_start == '"' && *(val_end-1) == '"')
         {
            val_start++;
            val_end--;
            *val_end=0;
         }

         if (strcmp(line_start, "db_server") == 0)
            conf.db_server = val_start;
         else if (strcmp(line_start, "db_name") == 0)
            conf.db_name = val_start;
         else if (strcmp(line_start, "db_user") == 0)
            conf.db_user = val_start;
         else if (strcmp(line_start, "db_password") == 0)
            conf.db_pass = val_start;
         else if (strcmp(line_start, "nr_user") == 0)
            conf.nr_user = val_start;
         else if (strcmp(line_start, "nr_password") == 0)
            conf.nr_pass = val_start;
         else if (strcmp(line_start, "report_email") == 0)
            conf.report_email = val_start;
         else if (strcmp(line_start, "debug") == 0)
            conf.debug = val_start;
         else if (strcmp(line_start, "stomp_topics") == 0)
            conf.stomp_topics = val_start;
         else if (strcmp(line_start, "stomp_topic_names") == 0)
            conf.stomp_topic_names = val_start;
      }
      line_start = line_end + 1;
   }
}

qword time_ms(void)
{
   static struct timeval ha_clock;
   gettimeofday(&ha_clock, NULL);
   qword result = ha_clock.tv_sec;
   result = result * 1000 + ha_clock.tv_usec / 1000;

   return result;
}

qword time_us(void)
{
   static struct timeval ha_clock;
   gettimeofday(&ha_clock, NULL);
   qword result = ha_clock.tv_sec;
   result = result * 1000000L + ha_clock.tv_usec;

   return result;
}

ssize_t read_all(const int socket, void * buffer, const size_t size)
{
   // TODO Needs a timeout of some sort, for error recovery.

   // Just the same as read() except it blocks until size bytes have been read, or an end-of-file or error occurs.
   // Return -1 = error, 0 = EOF, or size = success
   // Suitable for blocking sockets only.
   ssize_t l;
   size_t got = 0;
   while(got < size)
   {
      l = read(socket, buffer + got, size - got);
      if(l < 1) return l;
      got += l;
   }
   return size;
}
