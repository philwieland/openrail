/*
    Copyright (C) 2014, 2015, 2016, 2017, 2018, 2019, 2020 Phil Wieland

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
#include "build.h"

static void page(void);
static void update(void);
static void query(void);
static char * location_name(const char * const tiploc);
static char * show_handle(const dword h);

#define NAME "livesig"

#ifndef RELEASE_BUILD
#define BUILD "1607p"
#else
#define BUILD RELEASE_BUILD
#endif

// Set to 1 to disable caching of maps, for use at times of frequent updates.
#define NO_CACHE_MAPS 0

word debug;
enum {PageMode, UpdateMode, QueryMode} mode;
static time_t now;
#define PARMS 16
#define PARMSIZE 128
static char parameters[PARMS][PARMSIZE];

int main()
{
   now = time(NULL);

   char * parms = getenv("PARMS");
   // Parse parms
   word i, j, k, l;
   i = j = k = l = 0;
   if(parms && parms[0] == '/') i++;
   while(j < PARMS && parms[i] && k < PARMSIZE - 1 && parms)
   {
      if(parms[i] == '/')
      {
         parameters[j++][k] = '\0';
         k = 0;
         i++;
      }
      else
      {
         // Due to the simple nature of the queries we can use brute force here to bar Little Bobby Tables and others...
         if((parms[i] >= 'A' && parms[i] <= 'Z') || (parms[i] >= '0' && parms[i] <= '9'))
            parameters[j][k++] = parms[i++];
         else
            i++;
         l = j;
      }
   }
   if(k) parameters[j++][k] = '\0';

   while(j < PARMS) parameters[j++][0] = '\0';

   {
      char config_file_path[256];
      word p;

      strcpy(config_file_path, "/etc/openrail.conf");

      for(p = 0; p <= l; p++)
      {
         if(!strncasecmp(parameters[p], "_conf", 5))
         {
            strcpy(config_file_path, "/etc/");
            strcat(config_file_path, parameters[p] + 5);
         }
      } 

      char * config_fail;
      if((config_fail = load_config(config_file_path)))
      {
         printf("<p>Failed to read config file \"%s\":  %s</p>\n", config_file_path, config_fail);
         exit(0);
      }
   }

   debug = *conf[conf_debug];

   // Set up log
   {
      char logfile[128];

      sprintf(logfile, "/var/log/garner/web/%s.log", NAME);
      // Set to no logging at all (3) in normal mode.
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
      // parameters[0] is map number
      mode = PageMode;
   }

   // Initialise database
   db_init(conf[conf_db_server], conf[conf_db_user], conf[conf_db_password], conf[conf_db_name], DB_MODE_NORMAL);

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

   exit(0);
}

static void page(void)
{
   word map_id = atoi(parameters[0]);

   printf("Content-Type: text/html; charset=iso-8859-1\n");
#if NO_CACHE_MAPS
   printf("Cache-Control: no-cache\n");
#endif
   printf("\n");
   printf("<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\" \"http://www.w3.org/TR/html4/loose.dtd\">\n");
   printf("<html xmlns=\"http://www.w3.org/1999/xhtml\" lang=\"en\" xml:lang=\"en\">\n");
   printf("<head>\n");
   printf("<title>%s %s</title>\n", NAME, BUILD);
   printf("<link rel=\"stylesheet\" type=\"text/css\" href=\"/auxy/livesig.css\">\n");
   printf("</head>\n");
   printf("<body style=\"font-family: arial,sans-serif; background:#eeeeee\" onload=\"startup();\">\n");
   printf("<script type=\"text/javascript\" src=\"/auxy/livesig.js\"></script>\n");

   // See if the requested map exists.
   char filename[2048];
   FILE * fp;

   sprintf(filename, "%s/auxy/livesig%d.svg", getenv("DOCUMENT_ROOT"), map_id);

   if((fp = fopen(filename, "r")))
   {
      fclose(fp);
      printf("<object id=\"diagram\" type=\"image/svg+xml\" data=\"/auxy/livesig%d.svg\"></object>\n", map_id);
   }
   else
   {
      printf("<p>Map %d not found.  <a href=\"/\">Please select a map from the list on the home page.</a></p>\n", map_id);
   }
   struct tm * broken = localtime(&now);
   printf("<table width=\"1260\"><tr><td align=\"left\" id=\"bottom-line\">&copy;%04d.  Live data from Network Rail under <a href=\"http://www.networkrail.co.uk/data-feeds/terms-and-conditions\">this licence</a>.</td>\n", broken->tm_year + 1900);
   printf("<td align=\"right\"><a href=\"/\">Home Page and other maps</a></td><td width=\"10%%\">&nbsp;</td>\n");
   printf("<td align=\"right\"><a href=\"/about.html\">About livesig</a></td></tr></table>\n");
   printf("</body></html>\n\n");
}

static void update(void)
{
   char query[1024];
   dword new_handle;
   dword handle = strtol(parameters[1], NULL, 36);
   MYSQL_RES * result;
   MYSQL_ROW row;

   // parameters[1] = handle
   // Describer(s) in parameters[2..] 

   if(!db_query("SELECT MAX(handle) from td_updates"))
   {
      result = db_store_result();
      if((row = mysql_fetch_row(result)) && row[0]) 
      {
         new_handle = atol(row[0]);
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
      printf("%s\n", show_handle(new_handle));
      sprintf(query, "SELECT * FROM td_states WHERE (SUBSTRING(k,3,1) = 'b' OR SUBSTRING(k,3,1) = 's') AND (k LIKE '%s%%'", parameters[2]);
      word p = 3;
      while(p < PARMS && parameters[p][0])
      {
         char q[256];
         sprintf(q, " OR k LIKE '%s%%'", parameters[p]);
         strcat(query, q);
         p++;
      }
      // Ordered so that blank ones come first, to avoid overfilling the arrays in client js.
      // (This would FAIL if a k was present multiple times in td_states.)
      strcat(query, ") ORDER BY v");
      if(!db_query(query))
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
      printf("%s\n", show_handle(new_handle));
      sprintf(query, "SELECT * FROM td_updates where handle > %d AND (k LIKE '%s%%'", handle, parameters[2]);
      word p = 3;
      while(p < PARMS && parameters[p][0])
      {
         char q[256];
         sprintf(q, " OR k LIKE '%s%%'", parameters[p]);
         strcat(query, q);
         p++;
      }
      // Ordered so that blank ones come first, to avoid overfilling the arrays in client js.
      // (This would FAIL if a k was present multiple times in td_updates.)
      strcat(query, ") ORDER BY v");
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

   // End the output with the data for the status line.
   {
      char host[256], query[256], q[256];
      if(gethostname(host, sizeof(host))) host[0] = '\0';
      time_t last_actual = 0;
      sprintf(query, "SELECT min(last_timestamp) FROM describers WHERE id = '%s'", parameters[2]);
      word p = 3;
      while(p < PARMS && parameters[p][0])
      {
         sprintf(q, " OR id = '%s'", parameters[p]);
         strcat(query, q);
         p++;
      }
      if(!db_query(query))
      {
         MYSQL_RES * result = db_store_result();
         MYSQL_ROW row;
         if((row = mysql_fetch_row(result)))
         {
            last_actual = atol(row[0]);
         }
         mysql_free_result(result);
      }
      strcpy(query, time_text(last_actual, 1));
      query[14] = '\0'; // Chop off the seconds.

      time_t now = time(NULL);
      // WA14 Changed 64 to 96
      printf("%d|%s|%s|%s|%s", ((now - last_actual < 96)?0:1), query, NAME, BUILD, host);

      {
         // Banner
         if(!db_query("SELECT banner, banner1, expires FROM banners WHERE type = 'diagram'"))
         {
            MYSQL_RES * result = db_store_result();
            MYSQL_ROW row;
            if((row = mysql_fetch_row(result)) && row[0][0])
            {
               time_t expires = atol(row[2]);
               if(expires == 0 || expires > now)
               {
                  printf("|%s|%s", row[0], row[1]);
               }
            }
            mysql_free_result(result);
         }
      }
      printf("\n");
   }
}

static void query(void)
{
   char headcode[8], re_ob_headcode[8], query[2048], query1[256];
   MYSQL_RES * result0, * result1;
   MYSQL_ROW row0;
   dword schedule_id;

   // TIPLOCS in parameters[2..]

   if(strlen(parameters[1]) > 4)
   {         
      _log(DEBUG, "query() Not found.  Parameter error.");
      printf("Not found.\n");
      return;
   }
   strcpy(headcode, parameters[1]);
   while(strlen(headcode) < 4) strcat(headcode, " ");

   if(headcode[0] >= '0' && headcode[0] <= '2' && 
      headcode[1] >= '0' && headcode[1] <= '9' &&
      headcode[2] >= '0' && headcode[2] <= '5' &&
      headcode[3] >= '0' && headcode[3] <= '9')
   {
      // Train time, not reporting number.
      struct tm * broken = localtime(&now);
      static const char * days_runs[8] = {"runs_su", "runs_mo", "runs_tu", "runs_we", "runs_th", "runs_fr", "runs_sa", "runs_su"};
      sprintf(query, "SELECT s.id FROM cif_schedules AS s INNER JOIN cif_schedule_locations AS l ON s.id = l.cif_schedule_id WHERE l.tiploc_code = 'LVRPLSH' AND l.departure = '%s' AND s.deleted > %ld AND (s.%s) AND (s.schedule_start_date <= %ld) AND (s.schedule_end_date >= %ld) AND train_status != 'B' AND train_status != '5' ORDER BY LOCATE(s.CIF_stp_indicator, 'ONPC')",
           headcode, now + (12*60*60), days_runs[broken->tm_wday], now + (12*60*60), now - (12*60*60));
      if(!db_query(query))
      {
         result0 = db_store_result();
         if((row0 = mysql_fetch_row(result0))) 
         {
            schedule_id = atol(row0[0]);
            mysql_free_result(result0);

            printf("Allocated to %c%c:%c%c", headcode[0], headcode[1], headcode[2], headcode[3]);

            sprintf(query, "SELECT tiploc_code FROM cif_schedule_locations WHERE record_identity = 'LT' AND cif_schedule_id = %u", schedule_id);
            if(!db_query(query))
            {
               result0 = db_store_result();
               if((row0 = mysql_fetch_row(result0)))
               {
                  printf(" to %s", location_name(row0[0]));
               }
               mysql_free_result(result0);
            }
            printf("\n");
            return;
         }
         mysql_free_result(result0);
      }
      printf("Allocated to %c%c:%c%c departure.\n", headcode[0], headcode[1], headcode[2], headcode[3]);
      return;
   }

   // Reverse the de-obfuscation process
   strcpy(re_ob_headcode, headcode);
   sprintf(query, "SELECT obfus_hc FROM obfus_lookup WHERE true_hc = '%s' ORDER BY created DESC LIMIT 1", headcode);
   if(!db_query(query))
   {
      result0 = db_store_result();
      if((row0 = mysql_fetch_row(result0)) && row0[0][0]) 
      {
         _log(DEBUG, "query() Real headcode \"%s\", obfuscated headcode \"%s\" found in obfuscation lookup table.", headcode, row0[0]);
         strcpy(re_ob_headcode, row0[0]);
      }
      mysql_free_result(result0);
   }

   sprintf(query, "SELECT cif_schedule_id FROM trust_activation WHERE created > %ld AND (SUBSTR(trust_id,3,4) = '%s' OR SUBSTR(trust_id,3,4) = '%s') ORDER BY created DESC", now-(24*60*60), headcode, re_ob_headcode);

   if(db_query(query))
   {
      _log(DEBUG, "query() Database error 1.");
      printf("Database error 1.\n");
      return;
   }
   result0 = db_store_result();
   while((row0 = mysql_fetch_row(result0))) 
   {
      schedule_id = atol(row0[0]);

      sprintf(query, "SELECT * from cif_schedule_locations WHERE cif_schedule_id = %u AND (0", schedule_id);

      word p = 2;
      while(p < PARMS && parameters[p][0])
      {
         sprintf(query1, " OR tiploc_code = '%s'", parameters[p++]);
         strcat(query, query1);
      }
      strcat(query, ")");

      if(db_query(query))
      {
         _log(DEBUG, "query() Database error 2.");
         printf("Database error 2.\n");
         mysql_free_result(result0);
         return;
      }
      
      result1 = db_store_result();
      if(mysql_num_rows(result1))
      {
         // HIT!
         mysql_free_result(result1);
         mysql_free_result(result0);
         
         // Note we use WTT time, not GBTT.
         sprintf(query, "SELECT tiploc_code, departure FROM cif_schedule_locations WHERE record_identity = 'LO' AND cif_schedule_id = %u", schedule_id);
         if(!db_query(query))
         {
            result0 = db_store_result();
            if((row0 = mysql_fetch_row(result0)))
            {
               _log(DEBUG, "query() %s %s to ", show_time_text(row0[1]), location_name(row0[0]));
               printf("%s %s to ", show_time_text(row0[1]), location_name(row0[0]));
            }
            mysql_free_result(result0);
         }
     
         sprintf(query, "SELECT tiploc_code FROM cif_schedule_locations WHERE record_identity = 'LT' AND cif_schedule_id = %u", schedule_id);
         if(!db_query(query))
         {
            result0 = db_store_result();
            if((row0 = mysql_fetch_row(result0)))
            {
               _log(DEBUG, "query() %s", location_name(row0[0]));
               printf("%s\n", location_name(row0[0]));
            }
            mysql_free_result(result0);
         }
         return;
      }
      else
      {
         mysql_free_result(result1);
      }
   }

   _log(DEBUG, "query() Not found in database."); 

   // Translate some non-train ones.

   dword * key = (dword *) headcode;

   switch(*key)
   {
   case 0x20323431: /* 142  */ printf("Stabled class 142 unit(s).\n"); break;
   case 0x20303531: /* 150  */ printf("Stabled class 150 unit(s).\n"); break;
   case 0x20363531: /* 156  */ printf("Stabled class 156 unit(s).\n"); break;
   case 0x20383531: /* 158  */ printf("Stabled class 158 unit(s).\n"); break;
   case 0x20353831: /* 185  */ printf("Stabled class 185 unit(s).\n"); break;
   case 0x20353931: /* 195  */ printf("Stabled class 195 unit(s).\n"); break;
   case 0x20393133: /* 319  */ printf("Stabled class 319 unit(s).\n"); break;
   case 0x20333233: /* 323  */ printf("Stabled class 323 unit(s).\n"); break;
   case 0x20313333: /* 331  */ printf("Stabled class 331 unit(s).\n"); break;
   case 0x20303933: /* 390  */ printf("Stabled class 390 unit(s).\n"); break;
   case 0x20373933: /* 397  */ printf("Stabled class 397 unit(s).\n"); break;
   case 0x20323038: /* 802  */ printf("Stabled class 802 unit(s).\n"); break;
   case 0x2020424c: /* LB   */
   case 0x4b4f4c42: /* BLOK */ printf("Line blocked.\n"); break;
   case 0x20203354: /* T3   */ printf("Absolute possession."); break;
   case 0x434d4544: /* DEMC */
   case 0x43494d44: /* DMIC */
   case 0x494d4544: /* DEMI */ printf("Failed train.\n"); break;
   case 0x54455331: /* 1SET */ printf("Stabled unit.\n"); break;
   case 0x54455332: /* 2SET */ printf("Two stabled units.\n"); break;
   case 0x20544d45: /* EMT  */ printf("Stabled East Midlands Trains unit.\n"); break;
   case 0x49584154: /* TAXI */ printf("Allerton depot shuttle.\n"); break;
   default: printf("Not found.\n"); break;
   }
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

static char * show_handle(const dword h)
{
   const char convert[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
   static char result[16];
   dword v = h;
   word i = 15;

   if(!v) 
   {
      return "0";
   }
   while(v)
   {
      result[i--] = convert[v%36];
      v /= 36;
   }
   return result + i + 1;
}

      
