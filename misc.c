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
#include <netinet/in.h>
#include <errno.h>
#include <netdb.h>
#include "misc.h"

static char log_file[512];
static word log_mode;

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

char * day_date_text(const time_t time, const byte local)
{
   struct tm * broken;
   static char result[32];
   char * days[] = {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"};

   broken = local?localtime(&time):gmtime(&time);
      
   sprintf(result, "%s %02d/%02d/%02d",
           days[broken->tm_wday],
           broken->tm_mday, 
           broken->tm_mon + 1, 
           broken->tm_year % 100
           );
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

   if(log_mode == 3) return;
   if((level == PROC || level == DEBUG) && (log_mode == 0 || log_mode == 4)) return;

   va_list vargs0, vargs1;
   va_start(vargs0, text);
   va_copy(vargs1, vargs0);

   time_t now = time(NULL);
   struct tm * broken = gmtime(&now);
      
   sprintf(log, "%02d/%02d/%02d %02d:%02d:%02dZ ",
           broken->tm_mday, 
           broken->tm_mon + 1, 
           broken->tm_year % 100,
           broken->tm_hour,
           broken->tm_min,
           broken->tm_sec);

   if(log_mode == 1 || log_mode == 2)
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

   // Write to log file
   if(log_file[0] && (fp = fopen(log_file, "a")))
   {
      fprintf(fp, "%s", log);
      vfprintf(fp, text, vargs0);
      fprintf(fp, "\n");
      fclose(fp);
   }
   
   // Print as well
   if(log_mode == 1 || log_mode == 4) 
   {
      printf("%s", log);
      vprintf(text, vargs1);
      printf("\n");
   }

   va_end(vargs0);
   va_end(vargs1);

   return;
}


void _log_init(const char * l, const word d)
{
   // d is log_mode mode. 
   // 0 Normal running
   // 1 debug, print as well as log file
   // 2 debug, no print
   // 3 No logging at all.
   // 4 Normal running plus print.
   if(strlen(l) < 500) strcpy(log_file, l);
   else log_file[0] = '\0';
   log_mode = d;
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
   // Read config file.
   // Notes:
   // Blank lines and lines beginning with # will be ignored.
   // Other than that, all lines must contain <setting>:<value>  
   // Spaces either side of the : are optional and will be stripped.
   // Value may be enclosed in quotes ("), these will be stripped.
   // A correctly formatted line containing an unrecognised setting (e.g. foo: bar) will be silently ignored.
   //
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
   conf.stomp_topics = conf.stomp_topic_names = conf.stomp_topic_log = &buf[CONFIG_SIZE - 1];

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
         else if (strcmp(line_start, "stomp_topic_log") == 0)
            conf.stomp_topic_log = val_start;
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

static fd_set sockets;
static int stompy_socket;
word open_stompy(const word port)
{
   struct sockaddr_in serv_addr;
   struct hostent *server;
   stompy_socket = -1;

   _log(GENERAL, "Connecting socket to stompy...");
   stompy_socket = socket(AF_INET, SOCK_STREAM, 0);
   if (stompy_socket < 0) 
   {
      _log(CRITICAL, "Failed to create client socket.  Error %d %s", errno, strerror(errno));
      return 1;
   }
   
   server = gethostbyname("localhost");
   if (server == NULL) 
   {
      _log(CRITICAL, "Failed to resolve localhost\".");
      close(stompy_socket);
      stompy_socket = -1;
      return 1;
   }

   bzero((char *) &serv_addr, sizeof(serv_addr));
   serv_addr.sin_family = AF_INET;
   bcopy((char *)server->h_addr, 
         (char *)&serv_addr.sin_addr.s_addr,
         server->h_length);
   serv_addr.sin_port = htons(port);

   /* Now connect to the server */
   int rc = connect(stompy_socket, &serv_addr, sizeof(serv_addr));
   if(rc)
   {
      _log(CRITICAL, "Failed to connect.  Error %d %s", errno, strerror(errno));
      close(stompy_socket);
      stompy_socket = -1;
      return 1;
   }
   _log(GENERAL, "Connected.  Waiting for messages...");
   FD_ZERO(&sockets);
   FD_SET(stompy_socket, &sockets);
   return 0;
}

word read_stompy(void * buffer, const size_t max_size, const word seconds)
{
   // Given a blocking socket, blocks until a full STOMP frame has been read, or end-of-file/error/timeout
   // Return 0 Success.
   //        1 End of file.
   //        2 Error.  See errno.
   //        3 Timeout.
   //        4 Closed.
   //        5 Message too long.
   //        6 Timeout on message body.

   ssize_t l;
   size_t got = 0;
   ssize_t length;
   word result = 0;
   fd_set active_sockets;
   struct timeval wait_time;
   _log(PROC, "read_stompy(~, %ld, %d)", max_size, seconds);

   if(stompy_socket < 0) return 4;


   while(got < sizeof(ssize_t) && !result)
   {
      active_sockets = sockets;
      wait_time.tv_sec = seconds;
      wait_time.tv_usec = 0;
      int r = select(FD_SETSIZE, &active_sockets, NULL, NULL, seconds?(&wait_time):NULL);
      _log(DEBUG, "First select returns %d.", r);
      if(r == 0) result = 3;
      if(r <  0) result = 2;

      if(!result)
      {
         l = read(stompy_socket, buffer + got, sizeof(size_t) - got);
         if(l < 0) result = 2;
         if(l == 0) result = 1;
         got += l;
      }
   }

   if(result) return result;

   memcpy(&length, buffer, sizeof(ssize_t));
   _log(DEBUG, "Received frame length = 0x%zx", length);
   if(length > max_size) 
   {
      _log(MAJOR, "read_stompy() Error 5:  Received length 0x%08zx exceeds limit 0x%08zx.", length, max_size);
      return 5;
   }

   got = 0;
   while(got < length && !result)
   {
      active_sockets = sockets;
      wait_time.tv_sec = seconds;
      wait_time.tv_usec = 0;
      int r = select(FD_SETSIZE, &active_sockets, NULL, NULL, seconds?(&wait_time):NULL);
      _log(DEBUG, "Second select returns %d.", r);
      if(r == 0) result = 6;
      if(r <  0) result = 2;

      if(!result)
      {
         l = read(stompy_socket, buffer + got, length - got);
         if(l < 0) result = 2;
         if(l == 0) result = 1;
         got += l;
      }
   }

   if(result == 6) _log(MAJOR, "read_stompy() Error 6:  Timeout while waiting for message body.  Received 0x%08zx of 0x%08zx bytes.", got, length);

   return result;
}

word ack_stompy(void)
{
   _log(PROC, "ack_stompy()");
   if(stompy_socket < 0) return 1;
   if(write(stompy_socket, "A", 1) < 1) return 1;
   return 0;
   
}
void close_stompy(void)
{
   _log(PROC, "close_stompy()");
   if(stompy_socket >= 0) close(stompy_socket);
}

void extract_match(const char * const source, const regmatch_t * const matches, const unsigned int match, char * result, const size_t max_length)
{
   // Helper for regex matches.
   size_t size;

   size = matches[match].rm_eo - matches[match].rm_so;
   if(size > max_length - 1) 
   {
      char zs[128];
      sprintf(zs, "extract_match():  String length 0x%zx truncated to 0x%zx.", size, max_length - 1);
      _log(MAJOR, zs);
      size = max_length - 1;
   }

   strncpy(result, source + matches[match].rm_so, size);
   result[size] = '\0';
}

