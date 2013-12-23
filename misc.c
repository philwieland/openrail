/*
    Copyright (C) 2013 Phil Wieland

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

   char zs[128];
   if(strlen(string) != 10) return 0;
   strcpy(zs, string);
   strcat(zs, "T12:00:00Z");
   return parse_timestamp(zs);
}
                               
time_t parse_timestamp(const char * string)
{
   // ONLY works for yyyy-mm-ddThh:mm:ssZ
   char zs[128];

   if(strlen(string) < 20 || strlen(string) > 64) return 0;
   if(string[19] != 'Z') return 0;
                
   strcpy(zs, string);
   if(zs[19] != 'Z') return 0;

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

   if((level == PROC || level == DEBUG) && !debug) return;

   va_list vargs;
   va_start(vargs, text);

   strcpy(log, time_text(time(NULL), false));

   // strcat(log, log_module_10);
   strcat(log, " ");

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

char * commas_ll(const unsigned long long int n)
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
   static char result[128];
   word i,j;

   for(j=i=0; string[i] && j < 100; i++)
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
   // DANGER - Recursion:  DO NOT RAISE A CRITICAL ALERT IN HERE!
   FILE * fp;
   char command[1024], zs[256], tmp_file[256], host[256];
   int i;

   _log(PROC, "email_alert()");

   struct ntptimeval ha_clock;
   ntp_gettime(&ha_clock);

   sprintf(tmp_file, "/tmp/email-%ld.%09ld", ha_clock.time.tv_sec, ha_clock.time.tv_usec);

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
   sprintf(command, "/usr/bin/nohup /usr/sbin/sendreport \"[openrail-%s] %s\" %s  >%s.log 2>&1 &", 
           name, title, tmp_file, tmp_file);
   i = system(command);
   
   sprintf(zs, "system(\"%s\") returned %d", command, i);
   _log(DEBUG, zs);
           
   // Success
   return 0;
}

char * abbreviated_host_id(void)
{
   static char hostname[256];
   word i;

   gethostname(hostname, sizeof(hostname));

   for(i=0; hostname[i] && hostname[i] != '.'; i++);
   hostname[i] = '\0';

   {
      char zs[512];
      sprintf(zs, "abbreviated_host_id() return value \"%s\".", hostname);
      _log(DEBUG, zs);
   }

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

