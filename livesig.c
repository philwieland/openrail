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
#include <sys/time.h>
#include <mysql.h>
#include <unistd.h>
#include <sys/vfs.h>

#include "misc.h"
#include "db.h"

static void page(void);
static void update(void);
static void query(void);
static char * location_name(const char * const tiploc);

#define NAME "livesig"
#define BUILD "VA20"

word debug;
enum {PageMode, UpdateMode, QueryMode} mode;
static time_t now;
static char parameters[10][128];

int main()
{
   now = time(NULL);

   qword start_time = time_ms();

   char * parms = getenv("PARMS");
   // Parse parms
   word i, j, k, l;
   i = j = k = l = 0;
   if(parms && parms[0] == '/') i++;
   while(j < 10 && parms[i] && k < 128 && parms)
   {
      if(parms[i] == '/')
      {
         parameters[j++][k] = '\0';
         k = 0;
         i++;
      }
      else
      {
         // Due to the simple nature of the queries we can use brute force here to bar little bobby tables and others...
         if((parms[i] >= 'A' && parms[i] <= 'Z') || (parms[i] >= '0' && parms[i] <= '9'))
            parameters[j][k++] = parms[i++];
         else
            i++;
         l = j;
      }
   }
   if(k) parameters[j++][k] - '\0';

   while(j < 10) parameters[j++][0] = '\0';

   if(load_config("/etc/openrail.conf"))
   {
      printf("Failed to load config.\n");
      exit(1);
   }

   debug = !strcasecmp(conf.debug,"true");

   // Set up log
   {
      struct tm * broken = localtime(&now);
      char logfile[128];

      sprintf(logfile, "/tmp/livesig-%04d-%02d-%02d.log", broken->tm_year + 1900, broken->tm_mon + 1, broken->tm_mday);
      _log_init(logfile, debug?2:3);
   }

   if(parms)
   {
      _log(GENERAL, "PARMS = \"%s\"", parms);
   }
   else
   {
      _log(GENERAL, "No PARMS provided!");
   }

   if(!strcasecmp(parameters[0], "u")) 
   {
      // Update
      printf("Content-Type: text/plain\nCache-Control: no-cache\n\n");
      mode = UpdateMode;
   }
   else if(!strcasecmp(parameters[0], "q"))
   {
      // Query
      printf("Content-Type: text/plain\nCache-Control: no-cache\n\n");
      mode = QueryMode;
   }
   else
   {
      // Page
      mode = PageMode;
      printf("Content-Type: text/html; charset=iso-8859-1\n\n");
      printf("<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\" \"http://www.w3.org/TR/html4/loose.dtd\">\n");
      printf("<html xmlns=\"http://www.w3.org/1999/xhtml\" lang=\"en\" xml:lang=\"en\">\n");
      printf("<head>\n");
      printf("<title>%s %s</title>\n", NAME, BUILD);
      printf("<link rel=\"stylesheet\" type=\"text/css\" href=\"/auxy/livesig.css\">\n");
      printf("</head>\n");
      printf("<body style=\"font-family: arial,sans-serif; background:#eeeeee\" onload=\"startup();\">\n");
      printf("<script type=\"text/javascript\" src=\"/auxy/livesig.js\"></script>\n");
   }

   // Initialise database
   db_init(conf.db_server, conf.db_user, conf.db_pass, conf.db_name);

   _log(GENERAL, "Parameters:  (l = %d)", l);
   for(i=0;i <10; i++)
   {
      _log(GENERAL, "%d = \"%s\"", i, parameters[i]);
   }

   switch(mode)
   {
   case PageMode: page(); break;
   case UpdateMode: update(); break;
   case QueryMode: query(); break;
   }

   char host[256];
   if(gethostname(host, sizeof(host))) host[0] = '\0';
   qword elapsed = time_ms();
   elapsed -= start_time;
   time_t last_actual = 0;
   if(!db_query("SELECT last_td_actual FROM status"))
   {
      MYSQL_RES * result = db_store_result();
      MYSQL_ROW row;
      if((row = mysql_fetch_row(result)))
      {
         last_actual = atol(row[0]);
      }
      mysql_free_result(result);
   }
      
   switch(mode)
   {
   case UpdateMode:
      printf("%d|%s|%s|%s|%s\n", ((time(NULL) - last_actual < 64)?0:1), time_text(last_actual, 1), NAME, BUILD, host);
      break;

   case PageMode:
      printf("</body></html>\n\n");
      break;

   case QueryMode:
      break;
   }
   exit(0);
}

static void page(void)
{
   printf("<object id=\"diagram\" width=\"1260\" height=\"290\" type=\"image/svg+xml\" data=\"/auxy/livesig.svg\"></object>\n");

   printf("<table width=\"1260\"><tr><td align=\"left\" id=\"bottom-line\">&copy;2014 Phil Wieland.  Live data from Network Rail under <a href=\"http://www.networkrail.co.uk/data-feeds/terms-and-conditions\">this licence</a>.</td>");
   printf("<td align=\"right\"><a href=\"/\">Home</a></td><td width=\"10%%\">&nbsp;</td>");
   printf("<td align=\"right\"><a href=\"/about.html\">About livesig</a></td></tr></table>");
}

static void update(void)
{
   char query[1024];
   word new_handle;
   word handle = atoi(parameters[1]);
   MYSQL_RES * result;
   MYSQL_ROW row;

   if(!db_query("SELECT MAX(handle) from td_updates"))
   {
      result = db_store_result();
      if((row = mysql_fetch_row(result)) && row[0]) 
      {
         new_handle = atoi(row[0]);
      }
      else
      {
         new_handle = 0;
      }
   }
   else
   {
      printf("reload\n");
      return;
   }
   _log(DEBUG, "Handle = %d, new_handle = %d", handle, new_handle);

   if(handle > new_handle)
   {
      // Send all
      printf("%d\n", new_handle);
      if(!db_query("SELECT * FROM td_states"))
      {
         result = db_store_result();
         while((row = mysql_fetch_row(result))) 
         {
            printf("%s|%s\n", row[1], row[2]);
         }
         mysql_free_result(result);
      }  
   }
   else
   {
      // Send updates
      printf("%d\n", new_handle);
      sprintf(query, "SELECT * FROM td_updates where handle > %d", handle);
      if(!db_query(query))
      {
         result = db_store_result();
         while((row = mysql_fetch_row(result))) 
         {
            printf("%s|%s\n", row[2], row[3]);
         }
         mysql_free_result(result);
      }  
   }
}

static void query(void)
{
   char headcode[8], query[256];
   MYSQL_RES * result0, * result1;
   MYSQL_ROW row0;
   dword schedule_id;

   if(strlen(parameters[1]) != 4)
   {
      printf("Not found.\n");
      return;
   }
   strcpy(headcode, parameters[1]);

   sprintf(query, "SELECT cif_schedule_id FROM trust_activation WHERE created > %ld AND SUBSTR(trust_id,3,4) = '%s' ORDER BY created DESC", now-(24*60*60), headcode);

   if(db_query(query))
   {
      printf("DB error.\n");
      return;
   }
   result0 = db_store_result();
   while((row0 = mysql_fetch_row(result0))) 
   {
      schedule_id = atol(row0[0]);

      sprintf(query, "SELECT * from cif_schedule_locations WHERE cif_schedule_id = %ld AND (tiploc_code = 'HUYTON' OR tiploc_code = 'HUYTJUN' OR tiploc_code = 'LVRPLSH' OR tiploc_code = 'WVRTTEC')", schedule_id);
      if(db_query(query))
      {
         printf("DB error.\n");
         return;
      }
      
      result1 = db_store_result();
      if(mysql_num_rows(result1))
      {
         // HIT!
         mysql_free_result(result1);
         mysql_free_result(result0);
         
         // Note we use WTT time, not GBTT.
         sprintf(query, "SELECT tiploc_code, departure FROM cif_schedule_locations WHERE record_identity = 'LO' AND cif_schedule_id = %ld", schedule_id);
         if(!db_query(query))
         {
            result0 = db_store_result();
            if((row0 = mysql_fetch_row(result0)))
            {
               printf("%s %s to ", show_time_text(row0[1]), location_name(row0[0]));
            }
            mysql_free_result(result0);
         }
     
         sprintf(query, "SELECT tiploc_code FROM cif_schedule_locations WHERE record_identity = 'LT' AND cif_schedule_id = %ld", schedule_id);
         if(!db_query(query))
         {
            result0 = db_store_result();
            if((row0 = mysql_fetch_row(result0)))
            {
               printf("%s\n", location_name(row0[0]));
            }
            mysql_free_result(result0);
         }
         return;
      }
   }
   mysql_free_result(result0);
   
   printf("Not found.\n");
}

static char * location_name(const char * const tiploc)
{
   static char response[128];
   char query[128];
   MYSQL_RES * result;
   MYSQL_ROW row;

   response[0] = '\0';

   sprintf(query, "SELECT name FROM friendly_names_20 WHERE tiploc = '%s'", tiploc);
   if(!db_query(query))
   {
      result = db_store_result();
      if((row = mysql_fetch_row(result)))
      {
         strcpy(response, row[0]);
      }
      else
      {
         sprintf(query, "INSERT INTO friendly_names_20 VALUES('%s', '')", tiploc);
         db_query(query);
      }
      mysql_free_result(result);
   }

   if(!strlen(response))
   {
      sprintf(query, "SELECT SUBSTR(fn, 1, 20) FROM corpus WHERE tiploc = '%s'", tiploc);
      if(!db_query(query))
      {
         result = db_store_result();
         if((row = mysql_fetch_row(result)) && row[0][0])
         {
            strcpy(response, row[0]);
         }
         mysql_free_result(result);
      }
   }

   if(!strlen(response))
   {
      strcpy(response, tiploc);
   }
   return response;
}
