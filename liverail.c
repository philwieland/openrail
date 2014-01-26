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
#include <regex.h>
#include <time.h>
#include <sys/time.h>
#include <mysql.h>
#include <unistd.h>

#include "misc.h"
#include "db.h"

static void depsheet(void);
static void display_choice(MYSQL_RES * result0, const time_t when);
static void display_control_panel(const char * const location, const time_t when);
static void report_train(const dword cif_schedule_location_id, const time_t when, const word huyton_special);
static void report_train_summary(const dword cif_schedule_location_id, const time_t when, const word ntrains);
static void as(void);
static char * show_date(const time_t time, const byte local);
static void train(void);
static void train_text(void);
static char * location_name_link(const char * const tiploc, const word use_cache, const time_t when);
static char * location_name(const char * const tiploc, const word use_cache);
static char * show_stanox(const char * const stanox);
static char * show_stanox_link(const char * const stanox);
static char * show_act(const char * const input);
static char * show_trust_time(const char * const ms, const word local);
static char * show_trust_time_nocolon(const char * const ms, const word local);
static char * show_expected_time(const char * const scheduled, const word deviation, const word late);


#define NAME "Garner Live Rail"
#define BUILD "V125"

#define COLUMNS 4
#define URL_BASE "/rail/liverail/"

word debug, refresh;
static time_t now;
static char parameters[10][128];

#define CATEGORIES 12
static const char * categories[CATEGORIES] = {
   "OUUnadvertised Ordinary Passenger",
   "OOOrdinary Passenger",
   "OSStaff Train",
   "OWMixed",
   "XUUnadvertised Express",
   "XXExpress Passenger",
   "XZSleeper (Domestic)",
   "BRBus replacement",
   "BSBus WTT",
   "EEEmpty Coaching Stock",
   "ESECS and staff",
   "ZZLight Locomotove",
};

#define POWER_TYPES 10
static const char * power_types[POWER_TYPES] = {
   "D  Diesel",
   "DEMDiesel Electric Multiple Unit",
   "DMUDiesel Mechanical Multiple Unit",
   "E  Electric",
   "ED Electro-diesel",
   "EMLEMU plus D, E, or ED locomotive",
   "EMUElectric Multiple Unit",
   "EPUElectric Parcels Unit",
   "HSTHigh Speed Train",
   "LDSDiesel Shunting Locomotive",
};

static const char * days[] = {"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"};
// Days runs fields
static const char * days_runs[8] = {"runs_su", "runs_mo", "runs_tu", "runs_we", "runs_th", "runs_fr", "runs_sa", "runs_su"};

// (Hours * 60 + Minutes) * 4
#define DAY_START  4*60*4

// location name cache
#define CACHE_SIZE 16
static char cache_key[CACHE_SIZE][8];
static char cache_val[CACHE_SIZE][128];


// Display modes
enum modes {FULL, SUMMARY, UPDATE, MOBILE, AS, TRAIN, TRAINT} mode ;

static word mobile_trains, mobile_time;

int main()
{
   char zs[1024];

   now = time(NULL);

   struct timeval ha_clock;
   gettimeofday(&ha_clock, NULL);
   qword start_time = ha_clock.tv_sec;
   start_time = start_time * 1000 + ha_clock.tv_usec / 1000;


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
         parameters[j][k++] = parms[i++];
         l = j;
      }
   }
   if(k) parameters[j++][k] - '\0';

   while(j < 10) parameters[j++][0] = '\0';

   debug = !strcasecmp(parameters[l], "debug");
   refresh = !strcasecmp(parameters[l], "r");

   if(load_config("/etc/openrail.conf"))
   {
      printf("Failed to load config.\n");
      exit(1);
   }

   // Set up log
   {
      struct tm * broken = localtime(&now);
      char logfile[128];

      sprintf(logfile, "/tmp/liverail-%04d-%02d-%02d.log", broken->tm_year + 1900, broken->tm_mon + 1, broken->tm_mday);
      _log_init(logfile, debug?2:0);
   }

   if(parms)
   {
      _log(GENERAL, "PARMS = \"%s\"", parms);
   }
   else
      _log(GENERAL, "No PARMS provided!");

   if(refresh || debug) parameters[l][0] = '\0';

   mode = FULL;
   if(!strcasecmp(parameters[0], "depsheet")) mode = FULL;
   else if(!strcasecmp(parameters[0], "depsum")) mode = SUMMARY;
   else if(!strcasecmp(parameters[0], "depsumu")) mode = UPDATE;
   else if(!strcasecmp(parameters[0], "depsumm")) mode = MOBILE;
   else if(!strcasecmp(parameters[0], "as")) mode = AS;
   else if(!strcasecmp(parameters[0], "train")) mode = TRAIN;
   else if(!strcasecmp(parameters[0], "train_text")) mode = TRAINT;
   _log(DEBUG, "Display mode  = %d", mode);

   if(mode == FULL || mode == SUMMARY || mode == AS || mode == TRAIN || mode == TRAINT)
   {
      printf("Content-Type: text/html; charset=iso-8859-1\n\n");
      printf("<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\" \"http://www.w3.org/TR/html4/loose.dtd\">\n");
      printf("<html xmlns=\"http://www.w3.org/1999/xhtml\" lang=\"en\" xml:lang=\"en\">\n");
      printf("<head>\n");
      printf("<title>%s %s</title>\n", NAME, BUILD);
      printf("<link rel=\"stylesheet\" type=\"text/css\" href=\"/liverail.css\">\n");
      printf("</head>\n");
      printf("<body style=\"font-family: arial,sans-serif;\" onload=\"startup();\">\n");
      printf("<script type=\"text/javascript\" src=\"/liverail.js\"></script>\n");
   }
   else
   {
      printf("Content-Type: text/plain\n\n");
   }
   // Initialise location name cache
   location_name(NULL, false);

   // Initialise database
   db_init(conf.db_server, conf.db_user, conf.db_pass, conf.db_name);

   sprintf(zs, "Parameters:  (l = %d)", l);
   _log(GENERAL, zs);
   for(i=0;i <10; i++)
   {
      sprintf(zs, "%d = \"%s\"", i, parameters[i]);
      _log(GENERAL, zs);
   }

   switch(mode)
   {
   case FULL:
   case SUMMARY:
   case MOBILE:
   case UPDATE:
      depsheet();
      break;

   case AS:
      as();
      break;

   case TRAIN:
      train();
      break;

   case TRAINT:
      train_text();
      break;
   }

   if(mode == FULL || mode == SUMMARY || mode == UPDATE || mode == TRAIN || mode == TRAINT)
   {
      char host[256];
      if(gethostname(host, sizeof(host))) host[0] = '\0';
      gettimeofday(&ha_clock, NULL);
      qword elapsed = ha_clock.tv_sec;
      elapsed = elapsed * 1000 + ha_clock.tv_usec / 1000;
      elapsed -= start_time;
      if(mode == UPDATE)
      {
         printf("Report updated at %s by %s %s at %s.  Elapsed time %s ms.\n", time_text(time(NULL), 1), NAME, BUILD, host, commas_q(elapsed));
      }
      else
      {
         printf("<p id=\"bottom-line\">Report completed at %s by %s %s at %s.  Elapsed time %s ms.</p>\n", time_text(time(NULL), 1), NAME, BUILD, host, commas_q(elapsed));
         printf("</body></html>\n\n");
      }
   }
   exit(0);
}

static void display_choice(MYSQL_RES * result0, const time_t when)
{
   MYSQL_ROW row0;
   
   printf("<p>Select desired location</p>\n");

   printf("<table>");
   printf("<tr class=\"small-table\"><th>TIPLOC</th><th>Location</th></tr>\n");

   while((row0 = mysql_fetch_row(result0)) ) 
   {
      printf("<tr class=\"small-table\"><td>%s</td><td>%s</td></tr>\n", row0[0],  location_name_link(row0[0], false, when));
   }
   printf("</table>");
}

static void display_control_panel(const char * const location, const time_t when)
{
   printf("<table><tr><td class=\"control-panel-row\">\n");
   
   printf("&nbsp;Show trains at <input type=\"text\" id=\"search_loc\" size=\"32\" maxlength=\"64\" value=\"%s\" onkeydown=\"if(event.keyCode == 13) search_onclick(); else ar_off();\">\n", location);

   struct tm * broken = localtime(&now);
   word d = broken->tm_mday;
   word m = broken->tm_mon;
   word y = broken->tm_year;
   broken = localtime(&when);
   printf("on <input type=\"text\" id=\"search_date\" size=\"8\" maxlength=\"8\" value=\"");
   if(!when || (d == broken->tm_mday && m == broken->tm_mon && y == broken->tm_year))
   {
   }
   else
   {
      printf("%02d/%02d/%02d", broken->tm_mday, broken->tm_mon + 1, broken->tm_year % 100);
   }
   printf("\" onkeydown=\"if(event.keyCode == 13) search_onclick(); else ar_off();\">\n");

   printf(" <button id=\"search\" class=\"cp-button\" onclick=\"search_onclick();\">Show</button>\n");
   printf(" <button id=\"search\" class=\"cp-button\" onclick=\"summary_onclick();\">Show Summary</button>\n");

   printf("&nbsp;</td><td width=\"8%%\"></td><td class=\"control-panel-row\">&nbsp;<button id=\"as_rq\" class=\"cp-button\" onclick=\"as_rq_onclick();\">Advanced Search</button>&nbsp;\n");

   printf("&nbsp;</td><td width=\"8%%\"></td><td class=\"control-panel-row\">&nbsp;Auto-refresh&nbsp;<input type=\"checkbox\" id=\"ar\" onclick=\"ar_onclick();\"%s>&nbsp;\n", refresh?" checked":"");
   
   printf("</td><td id=\"progress\" class=\"control-panel-row\" style=\"display:none;\" width=\"1%%\" valign=\"top\">&nbsp;");

   printf("</td></tr></table>");
}


static void depsheet(void)
{
   word cif_schedule_count;
   word huyton_special;


#define MAX_TRAINS 2048
   struct train_details
   {
      dword cif_schedule_id;
      byte next_day;
      byte valid;
      dword cif_schedule_location_id;
      word sort_time;
      char cif_train_uid[8];
      char cif_stp_indicator;
   } 
   trains[MAX_TRAINS];
   word train_sequence[MAX_TRAINS];

   word train_count;

   MYSQL_RES * result0, * result1;
   MYSQL_ROW row0, row1;
   char query[4096], zs[256];

   time_t when;
   char location[128];
   // Process parameters
   // DATE
   {
      struct tm broken;
      // Pre-populate with today, or yesterday if before the threshold
      broken = *(localtime(&now));
      if((4 * (broken.tm_hour * 60 + broken.tm_min)) < DAY_START)
      {
         when = now - 24*60*60;
         broken = *(localtime(&when)); 
      }
      if(atoi(parameters[2]) > 0 && atoi(parameters[3]) > 0)
      {
         broken.tm_mday = atoi(parameters[2]);
         broken.tm_mon = atoi(parameters[3]) - 1;
         if(atoi(parameters[4])) broken.tm_year = atoi(parameters[4]) + 100;
      }
      broken.tm_hour = 12;
      broken.tm_min = 0;
      broken.tm_sec = 0;
      broken.tm_isdst = -1;
      when = timegm(&broken);
   }

   // LOCATION
   if(parameters[1][0])
   {
      db_real_escape_string(location, parameters[1], strlen(parameters[1]));
      huyton_special = false;
      {
         MYSQL_ROW row0;
         char query[1024];
         word done = false;
         
         // Decode 3alpha and search for friendly name match
         // Process location NLC to TIPLOC
         if(strlen(location) == 3)
         {
            sprintf(query, "SELECT tiploc FROM corpus WHERE 3alpha = '%s'", location);
            if(!db_query(query))
            {
               result0 = db_store_result();
               if((row0 = mysql_fetch_row(result0)) && row0[0][0]) 
               {
                  strcpy(location, row0[0]);
                  done = true;
               }
               mysql_free_result(result0);
            }
         }
         if(!done && strlen(location) < 8)
         {
            // Try TIPLOC
            sprintf(query, "SELECT tiploc FROM corpus WHERE tiploc = '%s'", location);
            if(!db_query(query))
            {
               result0 = db_store_result();
               if((row0 = mysql_fetch_row(result0)) && row0[0][0]) 
               {
                  strcpy(location, row0[0]);
                  done = true;
               }
               mysql_free_result(result0);
            }
         }
         if(!done)
         {
            // Try fn
            sprintf(query, "SELECT tiploc FROM corpus WHERE fn like '%%%s%%' and tiploc != '' order by fn", location);
            if(!db_query(query))
            {
               result0 = db_store_result();
               word found = mysql_num_rows(result0);
               
               if(found == 1 && (row0 = mysql_fetch_row(result0)) && row0[0][0]) 
               {
                  strcpy(location, row0[0]);
                  done = true;
                  
                  mysql_free_result(result0);
               }
               else if(found > 0)
               {
                  display_choice(result0, when);
                  return;
               }
               else mysql_free_result(result0);
            }
         }
      }
   }
   else
   {
      huyton_special = true;
      location[0] = '\0';
   }
   
   struct tm broken = *localtime(&when);

   // Differences for different reports
   switch(mode)
   {
   case SUMMARY:
   case UPDATE:
   case MOBILE:
      if(huyton_special)
      {
         huyton_special = false;
         strcpy(location, "HUYTON");
      }
      break;

   default:
      break;
   }

   // Additional parameters for mobile
   if(mode ==MOBILE)
   {
      // Time
      mobile_time   = atoi(parameters[5]);
      if(mobile_time < 400) mobile_time += 2400;
      mobile_trains = atoi(parameters[6]);
   }

   switch(mode)
   {
   case FULL:
   case SUMMARY:
      display_control_panel(location, when);
      printf("<h2>");
      if(mode == SUMMARY) printf ("Departures from ");
      else printf("Services at ");
      printf("%s on %s %02d/%02d/%02d</h2>", (huyton_special?"Huyton and Huyton Junction" : location_name(location, false)), days[broken.tm_wday % 7], broken.tm_mday, broken.tm_mon + 1, broken.tm_year % 100);
      break;

   default:
      break;
   }
   
   //                    0                          1                      2                             3         4
   strcpy(query, "SELECT cif_schedules.id, cif_schedules.CIF_train_uid, cif_schedules.CIF_stp_indicator, next_day, sort_time");
   strcat(query, " FROM cif_schedules INNER JOIN cif_schedule_locations");
   strcat(query, " ON cif_schedules.id = cif_schedule_locations.cif_schedule_id");
   if(huyton_special)
   {
      strcat(query, " WHERE ((cif_schedule_locations.tiploc_code = 'HUYTON') OR (cif_schedule_locations.tiploc_code = 'HUYTJUN'))");
   }
   else
   {
      sprintf(zs, " WHERE (cif_schedule_locations.tiploc_code = '%s')", location);
      strcat(query, zs);
   }
   
   strcat(query, " AND (cif_schedules.CIF_stp_indicator = 'N' OR cif_schedules.CIF_stp_indicator = 'P' OR cif_schedules.CIF_stp_indicator = 'O')");
   
   sprintf(zs, " AND deleted >= %ld", when);
   strcat(query, zs);
   
   // Select the day
   broken.tm_hour = 12;
   broken.tm_min = 0;
   broken.tm_sec = 0;
   word day = broken.tm_wday;
   word yest = (day + 6) % 7;
   word tom = (day + 1) % 7;
   when = timegm(&broken);
   
   sprintf(zs, " AND ((((%s) AND (schedule_start_date <= %ld) AND (schedule_end_date >= %ld) AND (NOT next_day)) AND (sort_time >= %d))",  days_runs[day],  when + 12*60*60, when - 12*60*60, DAY_START);
   strcat(query, zs);
   sprintf(zs, " OR   (((%s) AND (schedule_start_date <= %ld) AND (schedule_end_date >= %ld) AND (    next_day)) AND (sort_time >= %d))",  days_runs[yest], when - 12*60*60, when - 36*60*60, DAY_START);
   strcat(query, zs);
   sprintf(zs, " OR   (((%s) AND (schedule_start_date <= %ld) AND (schedule_end_date >= %ld) AND (NOT next_day)) AND (sort_time <  %d))",  days_runs[tom],  when + 36*60*60, when + 12*60*60, DAY_START); 
   strcat(query, zs);
   sprintf(zs, " OR   (((%s) AND (schedule_start_date <= %ld) AND (schedule_end_date >= %ld) AND (    next_day)) AND (sort_time <  %d)))", days_runs[day],  when + 12*60*60, when - 12*60*60, DAY_START);
   strcat(query, zs);
   
   if(mode == SUMMARY || mode == UPDATE || mode == MOBILE)
   {
      sprintf(zs, " AND (public_departure != '' OR departure != '') AND (train_status = 'P' OR train_status = 'B' OR train_status = '1' OR train_status = '5')");
      strcat(query, zs);
   }
   
   strcat(query, " ORDER BY LOCATE(cif_schedules.CIF_stp_indicator, 'NPO'), cif_schedule_id"); 
   
   word index, i;
   dword cif_schedule_id;
   
   // 1. Collect a list of unique schedule_id
   train_count = 0;
   //printf("<p>[%s]</p>", query);
   
   if(!db_query(query))
   {
      result0 = db_store_result();
      while((row0 = mysql_fetch_row(result0)))
      {
         //printf("<br>%s ", row0[0]);
         if(train_count >= MAX_TRAINS)
         {
            printf("<p>Error: MAX_TRAINS exceeded.</p>");
            return;
         }
         cif_schedule_id =  atol(row0[0]);
         for(i=0; i < train_count && trains[i].cif_schedule_id != cif_schedule_id; i++);
         
         if(i == train_count)
         {
            // Insert in array
            trains[train_count].cif_schedule_id = cif_schedule_id;
            strcpy(trains[train_count].cif_train_uid, row0[1]);
            trains[train_count].cif_stp_indicator = row0[2][0];
            trains[train_count].valid = true;
            trains[train_count].sort_time = atoi(row0[4]);
            trains[train_count].next_day = atoi(row0[3]);
            trains[train_count].cif_schedule_location_id = 0;
            train_count++;
         }
      }
      mysql_free_result(result0);
   }
   
   // 2. Cancel any which are overriden
   for(index = 1; index < train_count; index++)
   {
      if(trains[index].valid && trains[index].cif_stp_indicator == 'O')
      {
         for(i = 0; i < index; i++)
         {
            if(!strcmp(trains[i].cif_train_uid, trains[index].cif_train_uid))
            {
               // Hit
               trains[i].valid = false;
            }
         }
      }
   }
   
   // 3. Next, remove those which are cancelled, and remove those overriden by overlays that don't call
   // NOTE:  Overlay may not call at this station!
   sprintf(zs, "3. Commencing C and O check.  day = %d", day);
   _log(DEBUG, zs);
   
   for(index = 0; index < train_count; index++)
   {
      if(trains[index].valid)
      {
         sprintf(zs, "Testing index %d train \"%s\", sort_time = %d, next_day = %d", index, trains[index].cif_train_uid, trains[index].sort_time, trains[index].next_day );
         _log(DEBUG, zs);
         //                    0   1                 2                               3
         strcpy(query, "SELECT id, CIF_stp_indicator ");
         strcat(query, " FROM cif_schedules");
         strcat(query, " WHERE (cif_stp_indicator = 'C' OR cif_stp_indicator = 'O')");
         sprintf(zs, " AND (CIF_train_uid = '%s')", trains[index].cif_train_uid);
         strcat(query, zs);
         sprintf(zs, " AND (deleted >= %ld)", when);
         strcat(query, zs);

         if(trains[index].next_day && trains[index].sort_time >= DAY_START)
         {
            sprintf(zs, " AND ((%s) AND (schedule_start_date <= %ld) AND (schedule_end_date >= %ld))",  days_runs[yest], when - 12*60*60, when - 36*60*60);
            strcat(query, zs);
         }
         else if(trains[index].next_day && trains[index].sort_time < DAY_START)
         {
            sprintf(zs, " AND ((%s) AND (schedule_start_date <= %ld) AND (schedule_end_date >= %ld))", days_runs[day],  when + 12*60*60, when - 12*60*60);
            strcat(query, zs);
         }
         else if(!trains[index].next_day && trains[index].sort_time >= DAY_START)
         {
            sprintf(zs, " AND ((%s) AND (schedule_start_date <= %ld) AND (schedule_end_date >= %ld))",  days_runs[day],  when + 12*60*60, when - 12*60*60);
            strcat(query, zs);
         }
         else if(!trains[index].next_day && trains[index].sort_time < DAY_START)
         {
            sprintf(zs, " AND ((%s) AND (schedule_start_date <= %ld) AND (schedule_end_date >= %ld))",  days_runs[tom],  when + 36*60*60, when + 12*60*60); 
            strcat(query, zs);
         }
         if(!db_query(query))
         {
            result0 = db_store_result();
            while((row0 = mysql_fetch_row(result0)))
            {
               _log(DEBUG, "Index C or O match:");
               if(row0[1][0] == 'C')
               {
                  // Cancelled
                  trains[index].valid = false;
                  //printf("<br>Step 3:  %d invalidated due to C", index);
                  sprintf(zs, "Schedule %ld (%s) cancelled by schedule %s.", trains[index].cif_schedule_id, trains[index].cif_train_uid, row0[0]);
                  _log(DEBUG, zs);
               }
               else 
               {
                  // Overlay
                  // We will come here with an overlay we already know about OR one which *doesn't come here*
                  // In either case we invalidate this schedule.  If the overlay comes here it will already be in the list, somewhere.
                  _log(DEBUG, "Examining overlay.");
                  dword overlay_id = atol(row0[0]);
                  if(overlay_id > trains[index].cif_schedule_id || trains[index].cif_stp_indicator == 'N' || trains[index].cif_stp_indicator == 'P')
                  {
                     // Supercede
                     trains[index].valid = false;
                     _log(DEBUG, "Step 3:  %d invalidated due to O id = %s", index, row0[0]);
                  }
               }
            }
            mysql_free_result(result0);
         }
      }
   }

   // 4. We now have a list of unique garner schedule ids, all of which call here once OR MORE
   // in the huyton_special case they may call at huyton or junction or both but will still have only one entry here.
   // All entries have .cif_schedule_location_id == 0 now, we will set this as we process them,
   // any still ==0 at the end of step 4 will be huyton_special HUYTJUN-only trains for step 4a to process. 
   _log(DEBUG, "4. Add any second calls.  Poplulate schedule_location_d field.");
   word limit = train_count;
   word instances;

   for(index = 0; index < limit; index++)
   {
      if(trains[index].valid)
      {
         instances = 0;

         if(huyton_special)
         {
            sprintf(query, "SELECT id,sort_time from cif_schedule_locations WHERE cif_schedule_id = %ld AND (tiploc_code = 'HUYTON')", trains[index].cif_schedule_id);
         }
         else
         {
            sprintf(query, "SELECT id,sort_time from cif_schedule_locations WHERE cif_schedule_id = %ld AND tiploc_code = '%s'", trains[index].cif_schedule_id, location);
         }
         if(!db_query(query))
         {
            result1 = db_store_result();
            while((row1 = mysql_fetch_row(result1)))
            {
               if(instances++)
               {
                  // Second or subsequent visit to this location.  Append to end of array
                  trains[train_count].cif_schedule_id   = trains[index].cif_schedule_id;
                  strcpy(trains[train_count].cif_train_uid, trains[index].cif_train_uid);
                  trains[train_count].cif_stp_indicator = trains[index].cif_stp_indicator;
                  trains[train_count].valid = true;
                  trains[train_count].sort_time = atoi(row1[1]);
                  trains[train_count].next_day          = trains[index].next_day;
                  trains[train_count].cif_schedule_location_id = atol(row1[0]);
                  //printf("<br>Step 4:  Second visit for %ld, populated entry %d", trains[index].cif_schedule_id, train_count);
                  train_count++;
               }
               else
               {
                  // First visit.  Update this entry
                  trains[index].cif_schedule_location_id = atol(row1[0]);
                  trains[index].sort_time  = atoi(row1[1]);
                  //printf("<br>Step 4:  Populated entry %d, id=%ld", index, trains[index].cif_schedule_id);
               }
            }
            mysql_free_result(result1);
         }
      }
   }

   //4a. Need to repeat step 4 for junction in huyton special mode, and merge the results.
   // Any train which is reported at both will already be correctly in the table
   if(huyton_special)
   {
      _log(DEBUG, "4a. HUYTJUN - Add any trains here which aren't populated yet.  Poplulate schedule_location_d field.");
      word sort_time;
      limit = train_count;
      for(index = 0; index < limit; index++)
      {
         // Danger!  Could test for cif_schedule_location_id == 0 here BUT what if first visit has a huyton time but second visit don't!
         if(trains[index].valid)
         {
            instances = 0;
            sprintf(query, "SELECT id, sort_time from cif_schedule_locations WHERE cif_schedule_id = %ld AND (tiploc_code = 'HUYTJUN')", trains[index].cif_schedule_id);
            if(!db_query(query))
            {
               result1 = db_store_result();
               while((row1 = mysql_fetch_row(result1)))
               {
                  sort_time = atoi(row1[1]);
                  {
                     char zlog[1024];
                     sprintf(zlog, "Step 4a: Try index %d id = %ld finds location_id = %s sort time = %d limit = %d", index, trains[index].cif_schedule_id, row1[0], sort_time, limit);
                     _log(DEBUG, zlog);
                  }
                  word hit = false;
                  for(i = 0; i < limit && !hit; i++)
                  {
                     if(trains[i].valid && trains[i].cif_schedule_location_id)
                     {
                        if(trains[i].cif_schedule_id == trains[index].cif_schedule_id)
                        {
                           if((trains[i].sort_time < sort_time + 16) && (trains[i].sort_time > sort_time - 16))
                           {
                              // We've found a populated entry which looks like this train at this call
                              hit = true;
                              char zlog[1024];
                              sprintf(zlog, "Hit:  Found %d:  valid = %d  cif_schedule_location_id = %ld sort_time = %d",
                                      i, trains[i].valid, trains[i].cif_schedule_location_id, trains[i].sort_time);
                              _log(DEBUG, zlog);
                           }
                        }
                     }
                  }
                        
                  if(!hit)
                  {
                     _log(DEBUG, "Miss - Similar entry not found so this one should be added.");
                     // We haven't got a "similar" entry, so assume it doesn't go to HUYTON and needs adding.
                     if(instances++)
                     {
                        // Second or subsequent visit to this location.  Append to end of array
                        trains[train_count].cif_schedule_id = trains[index].cif_schedule_id;
                        strcpy(trains[train_count].cif_train_uid, trains[index].cif_train_uid);
                        trains[train_count].cif_stp_indicator = trains[index].cif_stp_indicator;
                        trains[train_count].valid = true;
                        trains[train_count].sort_time = sort_time;
                        trains[train_count].next_day = trains[index].next_day;
                        trains[train_count].cif_schedule_location_id = atol(row1[0]);
                        // printf("Step 4a: Second visit for %ld, populated entry %d", trains[index].cif_schedule_id, train_count);
                        train_count++;
                     }
                     else
                     {
                        // First visit.  Update this entry
                        trains[index].cif_schedule_location_id = atol(row1[0]);
                        trains[index].sort_time = sort_time;
                       //printf(" Step 4a:  Populated entry %d, id=%ld", index, trains[index].cif_schedule_id);
                     }
                  }
               }
               mysql_free_result(result1);
            }
         }
      }
   }


   // 5. Bubble Sort
   {
      word i,j;

      word run = true;

      // First, mung the sort_time so that early hours trains come after the others:
      for(j=0; j< train_count; j++)
      {
         // Early hours trains come after all the others.
         if(trains[j].sort_time < DAY_START) trains[j].sort_time += 10000;
         train_sequence[j] = j;
         //if(!trains[j].cif_schedule_location_id) printf("<br>Step 5: %d not populated.", j);
      }

      for(j = train_count; run && j > 1; j--)
      {
         run = false;
         for(i=1; i < j; i++)
         {
            if(trains[train_sequence[i]].sort_time < trains[train_sequence[i-1]].sort_time)
            {
               run = true;
               // Swap
               word tempo = train_sequence[i];
               train_sequence[i] = train_sequence[i-1];
               train_sequence[i-1] = tempo;
            }
         }
      }
   }

   switch(mode)
   {
   case SUMMARY:
   case UPDATE:
   case MOBILE:
      cif_schedule_count = 0;
      for(index = 0; index < train_count; index++)
      {
         if(trains[index].valid)
         {
            cif_schedule_count++;
         }
      }
      report_train_summary(0, when, cif_schedule_count);
      for(index = 0; index < train_count; index++)
      {
         if(trains[train_sequence[index]].valid)
         {
            report_train_summary(trains[train_sequence[index]].cif_schedule_location_id, when, cif_schedule_count);
         }
      }
      if(mode == SUMMARY) printf("</table>\n");
      break;

   case FULL:
      printf("<table>\n");
      printf("<tr class=\"small-table\"><th>Detail</th><th>Type</th><th>ID</th><th>CIF ID</th><th>Latest Live Data</th><th>P</th><th>Times WTT(Public)</th><th>From</th><th>To</th></tr>\n");
      cif_schedule_count = 0;
      for(index = 0; index < train_count; index++)
      {
         // printf("%s<br>\n", row0[0]);
         if(trains[train_sequence[index]].valid)
         {
            cif_schedule_count++;
            report_train(trains[train_sequence[index]].cif_schedule_location_id, when, huyton_special);
         }
      }
      
      printf("</table>\n");
      break;

   default:
      break;
   }
}

static void report_train(const dword cif_schedule_location_id, const time_t when, const word huyton_special)
{
   MYSQL_RES * result0, * result1;
   MYSQL_ROW row0, row1;

   char query[1024];
   word vstp;

   // Find schedule id
   dword cif_schedule_id;
   word next_day, sort_time;
   sprintf(query, "select cif_schedule_id, next_day, sort_time from cif_schedule_locations where id = %ld", cif_schedule_location_id);
   db_query(query);
   result0 = db_store_result();
   if((row0 = mysql_fetch_row(result0)))
   {
      cif_schedule_id = atol(row0[0]); 
      next_day        = atoi(row0[1]);
      sort_time       = atoi(row0[2]);
   }
   else
   {
      _log(MAJOR, "report_train() failed to determine cif_schedule_id.");
   }
   mysql_free_result(result0);

   // Calculate start_date from when, deducting 24h if next_day and/or adding 24h if after 00:00.
   time_t start_date = when - (next_day?(24*60*60):0);
   if(sort_time < DAY_START) start_date += (24*60*60);

   //                     0             1                    2                  3              4              5                  6           
   sprintf(query, "SELECT train_status, schedule_start_date, schedule_end_date, signalling_id, CIF_train_uid, CIF_stp_indicator, update_id FROM cif_schedules WHERE id = %ld", cif_schedule_id);

   if(!db_query(query))
   {
      result0 = db_store_result();

      if((row0 = mysql_fetch_row(result0)))
      {
         printf("<tr class=\"small-table\">");
         vstp = (row0[6][0] == '0' && row0[6][1] == 0);

         // Link
         printf("<td><a class=\"linkbutton\" href=\"%strain/%ld/%s\">Details</a></td>\n", URL_BASE, cif_schedule_id, show_date(start_date, false));
         
         // Status
         if(vstp) printf("<td class=\"small-table-vstp\">V");
         else if(row0[0][0] == 'F' || row0[0][0] == '2' || row0[0][0] == '3') printf("<td class=\"small-table-freight\">");
         else printf("<td>");

         switch(row0[0][0])
         {
         case 'B': printf("Bus</td>");         break;
         case 'F': printf("Freight</td>");     break;
         case 'P': printf("Pass.</td>");   break;
         case 'T': printf("Trip</td>");        break;
         case '1': printf("STP pass.</td>");    break;
         case '2': printf("STP fr.</td>"); break;
         case '3': printf("STP trip</td>");    break;
         case '5': printf("STP bus</td>");     break;
         default:  printf("%s</td>", row0[0]); break;
         }

         // Signalling ID
         printf("<td>");
         if(row0[3][0])
            printf("%s", row0[3]);
         else
            printf("%ld", cif_schedule_id);
         printf("</td>");

         // CIF ID and CIF STP indicator
         printf("<td>%s(", show_spaces(row0[4]));

         switch(row0[5][0])
         {
         case 'C': printf("C"); break;
         case 'N': printf("N"); break;
         case 'O': printf("O"); break;
         case 'P': printf("P"); break;
         default:  printf("%s", row0[5]);  break;
         }
         printf(")</td>");

         // TRUST
         {
            char query[512], zs[128], zs1[128], report[1024], class[32];
            word deduced;
            MYSQL_RES * result2;
            MYSQL_ROW row2;
            report[0] = class[0] = '\0';
            struct tm * broken = gmtime(&start_date);
            byte dom = broken->tm_mday;

            // Then, only accept activations where dom matches, and are +- 4 days (To eliminate last month's activation.)  YUK
            sprintf(query, "SELECT created, trust_id, deduced FROM trust_activation WHERE cif_schedule_id = %ld AND substring(trust_id FROM 9) = '%02d' AND created > %ld AND created < %ld order by created", cif_schedule_id, dom, when - 4*24*60*60, when + 4*24*60*60);
            if(!db_query(query))
            {
               result1 = db_store_result();
               while((row1 = mysql_fetch_row(result1)))
               {
                  deduced = atoi(row1[2]);
                  strcpy(zs, time_text(atol(row1[0]), true)); // Local time
                  if(report[0]) strcat(report, "<br>");
                  sprintf(zs1, "%s Activated.", zs);
                  strcat(report, zs1);
                  strcpy(class, "small-table-act");
                  sprintf(query, "SELECT created, reason, type from trust_cancellation where trust_id='%s' AND created > %ld AND created < %ld order by created ", row1[1], when - 4*24*60*60, when + 4*24*60*60);
                  if(!db_query(query))
                  {
                     result2 = db_store_result();
                     while((row2 = mysql_fetch_row(result2)))
                     {
                        sprintf(report, "%s Cancelled %s (%s)", time_text(atol(row2[0]), true), row2[1], row2[2]);
                        strcpy(class, "small-table-cape");
                     }
                     mysql_free_result(result2);
                  }
                  sprintf(query, "SELECT created, event_type, planned_event_type, platform, loc_stanox, actual_timestamp, timetable_variation, variation_status from trust_movement where trust_id='%s' AND created > %ld AND created < %ld order by actual_timestamp desc, planned_timestamp desc, created desc limit 1", row1[1], when - 4*24*60*60, when + 4*24*60*60);
                  if(!db_query(query))
                  {
                     result2 = db_store_result();
                     while((row2 = mysql_fetch_row(result2)))
                     {
                        // Abandon the report so far and just show the last movement.
                        strcpy(report, show_trust_time(row2[5], true));
                        sprintf(zs1, " %c ", row2[1][0]);
                        strcat(report, zs1);
                        strcat(report, show_stanox(row2[4]));
                     
                        if(row2[3][0])
                        {
                           if(row2[3][0] == ' ')
                           {
                              sprintf(zs1, " P%s", row2[3] + 1);
                           }
                           else
                           {
                              sprintf(zs1, " P%s", row2[3]);
                           }
                           strcat(report, zs1);
                        }
                        word variation = atoi(row2[6]);
                        sprintf(zs1, " %s %s", variation?row2[6]:"", row2[7]);
                        strcat(report, zs1);
                     }
                     mysql_free_result(result2);
                  }
               }
               mysql_free_result(result1);
            }
            if(class[0]) printf("<td class=\"%s\">", class);
            else printf("<td>");
            if(deduced) printf("Deduced activation.<br>");
            printf("%s</td>", report);
         }

         // Arrival and departure         
         sprintf(query, "SELECT arrival, public_arrival, departure, public_departure, pass, platform, tiploc_code FROM cif_schedule_locations where id = %ld", cif_schedule_location_id);
         if(!db_query(query))
         {
            result1 = db_store_result();
            
            if((row1 = mysql_fetch_row(result1)))
            {
               printf("<td>%s</td><td>\n", row1[5]);
               if(huyton_special && !strcmp(row1[6], "HUYTJUN")) printf("Junction: ");
               if(row1[0][0]) printf("a: %s", show_time(row1[0])); // arrival
               if(row1[1][0]) printf("(%s)",  show_time(row1[1])); // public arrival
               if(row1[2][0]) printf(" d: %s",show_time(row1[2])); // dep
               if(row1[3][0]) printf("(%s)",  show_time(row1[3])); // public dep
               if(row1[4][0]) printf("p: %s", show_time(row1[4])); // pass
               printf("</td>");
            }
            mysql_free_result(result1);
         }
               
         // From
         sprintf(query, "SELECT tiploc_code, departure, public_departure FROM cif_schedule_locations where cif_schedule_id = %ld AND record_identity = 'LO'", cif_schedule_id);
         printf("<td>");
         if(!db_query(query))
         {
            result1 = db_store_result();
            
            if((row1 = mysql_fetch_row(result1)))
            {
               printf("%s %s", location_name_link(row1[0], true, when), show_time(row1[1]));
               if(row1[2][0]) printf("(%s)", show_time(row1[2]));
            }
            mysql_free_result(result1);
         }
         printf("</td>");

         // To
         sprintf(query, "SELECT tiploc_code, arrival, public_arrival FROM cif_schedule_locations where cif_schedule_id = %ld AND record_identity = 'LT'", cif_schedule_id);
         printf("<td>");
         if(!db_query(query))
         {
            result1 = db_store_result();
            
            if((row1 = mysql_fetch_row(result1)))
            {
               printf("%s %s", location_name_link(row1[0], true, when), show_time(row1[1]));
               if(row1[2][0]) printf("(%s)", show_time(row1[2]));
            }
            mysql_free_result(result1);
         }
         printf("</td>");
         printf("</tr>\n");

      }
   
      mysql_free_result(result0);
   }

   return;
}

static void report_train_summary(const dword cif_schedule_location_id, const time_t when, const word ntrains)
{
   MYSQL_RES * result0, * result1;
   MYSQL_ROW row0, row1;

   char query[1024];
   static word trains, rows, train, row, bus, shown;
   static word nlate, ncape, nbus, ndeduced, narrival;
   word vstp, status, mobile_sched, mobile_act;
   char actual[16], expected[16];
   word deviation, deduced, late;
   dword cif_schedule_id;
   word next_day, sort_time;
   char train_details[256], depart[8], wtt_depart[8], destination[128], tiploc[16], analysis[64], mobile_analysis[32];
   struct tm * broken;

   deviation = late = status = bus = deduced = 0;

   _log(PROC, "report_train_summary(%ld, %ld, %d)", cif_schedule_location_id, when, ntrains);

   // Initialise
   if(cif_schedule_location_id == 0)
   {
      trains = ntrains;
      rows = (trains + COLUMNS - 1) / COLUMNS;
      if(rows < 20) rows = 20;
      train = row = 0;
      nlate = ncape = nbus = ndeduced = narrival = shown = 0;
      broken = localtime(&when);
      if(mode == UPDATE)
      {
         printf("%d%02d%s\n", trains, broken->tm_mday, BUILD);
      }
      else if(mode == SUMMARY)
      {
         printf("\n<input type=\"hidden\" id=\"display_handle\" value=\"%d%02d%s\">", trains, broken->tm_mday, BUILD);
         printf("<table><tr>");
      }
      return;
   }

   // Before print
   if(row && (!(row % rows)))
   {
      if(mode == SUMMARY) printf("</table></td>\n");
   }

   if((!(row % rows)) && row < trains)
   {
      if(mode == SUMMARY) printf("<td><table><tr class=\"summ-table-head\"><th>Train</th><th>Report</th></tr>\n");
   }
 
   // Find schedule id
   sprintf(query, "select cif_schedule_id, next_day, sort_time, public_departure, tiploc_code, departure from cif_schedule_locations where id = %ld", cif_schedule_location_id);
   db_query(query);
   result0 = db_store_result();
   if((row0 = mysql_fetch_row(result0)))
   {
      cif_schedule_id = atol(row0[0]); 
      next_day        = atoi(row0[1]);
      sort_time       = atoi(row0[2]);
      strcpy(wtt_depart, row0[5]);
      strcpy(depart, row0[3]);
      if(!depart[0]) strcpy(depart, row0[5]);
      strcpy(expected, row0[3]);
      strcpy(tiploc, row0[4]);
      _log(DEBUG, "Got schedule id etc.");
   }
   else
   {
      _log(MAJOR, "report_train() failed to determine cif_schedule_id.");
      return;
   }
   mysql_free_result(result0);

   // Calculate start_date from when, deducting 24h if next_day and/or adding 24h if after 00:00.
   time_t start_date = when - (next_day?(24*60*60):0);
   if(sort_time < DAY_START) start_date += (24*60*60);

   //                     0             1                    2                  3              4              5                  6           
   sprintf(query, "SELECT train_status, schedule_start_date, schedule_end_date, signalling_id, CIF_train_uid, CIF_stp_indicator, update_id FROM cif_schedules WHERE id = %ld", cif_schedule_id);

   if(!db_query(query))
   {
      result0 = db_store_result();

      if((row0 = mysql_fetch_row(result0)))
      {
         vstp = (row0[6][0] == '0' && row0[6][1] == 0);
         bus = (row0[0][0] == 'B' || row0[0][0] == '5');

         // To
         sprintf(query, "SELECT tiploc_code FROM cif_schedule_locations where cif_schedule_id = %ld AND record_identity = 'LT'", cif_schedule_id);
         if(!db_query(query))
         {
            result1 = db_store_result();
            
            if((row1 = mysql_fetch_row(result1)))
            {
               strcpy(destination, location_name(row1[0], true));
            }
            mysql_free_result(result1);
         }

         {
            char zs[512];
            sprintf(zs, "Got destination = \"%s\"", destination);
            _log(DEBUG, zs);
         }

         // Link
         sprintf(train_details, "<a class=\"linkbutton-summary\" href=\"%strain/%ld/%s\">%s %s</a>", URL_BASE, cif_schedule_id, show_date(start_date, false), depart, destination);

         {
            char zs[512];
            sprintf(zs, "Got train details = \"%s\"", train_details);
            _log(DEBUG, zs);
         }
         status = 0;
         // TRUST
         if(!bus)
         {
            char query[512], trust_id[16];
            MYSQL_RES * result2;
            MYSQL_ROW row2;
            broken = gmtime(&start_date);
            byte dom = broken->tm_mday;

            // Only accept activations where dom matches, and are +- 4 days (To eliminate last month's activation.)  YUK
            sprintf(query, "SELECT created, trust_id, deduced FROM trust_activation WHERE cif_schedule_id = %ld AND substring(trust_id FROM 9) = '%02d' AND created > %ld AND created < %ld order by created", cif_schedule_id, dom, when - 4*24*60*60, when + 4*24*60*60);
            if(!db_query(query))
            {
               result1 = db_store_result();
               if((row1 = mysql_fetch_row(result1)))
               {
                  status = 1;
                  strcpy(trust_id, row1[1]);
                  deduced = (row1[2][0] != '0');
               }
               mysql_free_result(result1);
            }

            if(status)
            {
               sprintf(query, "SELECT event_type, loc_stanox, actual_timestamp, timetable_variation, variation_status, planned_timestamp from trust_movement where trust_id='%s' AND created > %ld AND created < %ld order by actual_timestamp, planned_timestamp, created", trust_id, when - 4*24*60*60, when + 4*24*60*60);
               if(!db_query(query))
               {
                  result1 = db_store_result();
                  while((row1 = mysql_fetch_row(result1)))
                  {
                     if(status < 4)
                     {
                        status = 2;
                        strcpy(actual, row1[2]);
                        deviation = atoi(row1[3]);
                        late = !strcasecmp("late", row1[4]);
                     }
                     if(status < 5)
                     {
                        sprintf(query, "SELECT tiploc FROM corpus WHERE stanox = %s", row1[1]);
                        if(!db_query(query))
                        {
                           result2 = db_store_result();
                           if((row2 = mysql_fetch_row(result2)))
                           {
                              if(!strcasecmp(tiploc, row2[0]))
                              {
                                 if(!strcasecmp("departure", row1[0]))
                                 {
                                    // Got a departure report at our station
                                    // Check if it is about the right time, in case train calls twice.
                                    {
                                       char z[8];
                                       z[0] = wtt_depart[0]; z[1] = wtt_depart[1]; z[2] = '\0';
                                       word sched = atoi(z)*60;
                                       z[0] = wtt_depart[2]; z[1] = wtt_depart[3];
                                       sched += atoi(z);
                                       time_t planned_timestamp = atol(row1[5]);
                                       struct tm * broken = localtime(&planned_timestamp);
                                       word planned = broken->tm_hour * 60 + broken->tm_min;
                                       if(planned > sched - 3 && planned < sched + 3) // This might fail close to midnight!
                                       {
                                          // Near enough!
                                          status = 5;
                                          strcpy(actual, row1[2]);
                                          deviation = atoi(row1[3]);
                                          late = !strcasecmp("late", row1[4]);
                                       }
                                    }
                                 }
                                 else if(status < 4)
                                 {
                                    // Got an arrival from our station AND haven't seen a departure yet
                                    char z[8];
                                    z[0] = wtt_depart[0]; z[1] = wtt_depart[1]; z[2] = '\0';
                                    word sched = atoi(z)*60;
                                    z[0] = wtt_depart[2]; z[1] = wtt_depart[3];
                                    sched += atoi(z);
                                    time_t planned_timestamp = atol(row1[5]);
                                    struct tm * broken = localtime(&planned_timestamp);
                                    word planned = broken->tm_hour * 60 + broken->tm_min;
                                    if(planned > sched - 6 && planned < sched + 6) // This might fail close to midnight!
                                    {
                                       // Near enough!
                                       status = 4;
                                    }
                                 }
                              }
                           }
                           mysql_free_result(result2);
                        }
                     }
                  }
                  mysql_free_result(result1);
               }
            }
            
            if(status < 4)
            {
               sprintf(query, "SELECT created, reason, type from trust_cancellation where trust_id='%s' AND created > %ld AND created < %ld order by created ", trust_id, when - 4*24*60*60, when + 4*24*60*60);                  
               if(!db_query(query))
               {                   
                  result2 = db_store_result();
                  if((row2 = mysql_fetch_row(result2)))
                  {
                     status = 3;
                  }
                  mysql_free_result(result2);
                  
               }
            }
         }
      }
      mysql_free_result(result0);
   }

   // Build analysis
   mobile_sched = atoi(depart);
   mobile_act = mobile_sched;
   char row_class[32];
   switch(status)
   {
   case 0: 
      if(bus) 
      {
         sprintf(analysis, "<td>(Bus)");
         strcpy(mobile_analysis, "Bus");
         nbus++;
      }
      else 
      {
         sprintf(analysis, "<td>&nbsp"); 
         strcpy(mobile_analysis, "");
      }
      strcpy(row_class, "summ-table-idle");
      break;

   case 1: // Activated
      sprintf(analysis, "<td>Activated");
      strcpy(mobile_analysis, "");
      strcpy(row_class, "summ-table-act");
      break;

   case 2: // Moved
      strcpy(row_class, "summ-table-move");
      if(!deviation ) sprintf(analysis, "<td class=\"summ-table-good\">Exp. On time");
      else if(deviation < 3) sprintf(analysis, "<td class=\"summ-table-good\">Exp. %s %d%s",  show_expected_time(expected, deviation, late), deviation, late?"L":"E");
      else if(deviation < 6) sprintf(analysis, "<td class=\"summ-table-minor\">Exp. %s %d%s", show_expected_time(expected, deviation, late), deviation, late?"L":"E");
      else                   sprintf(analysis, "<td class=\"summ-table-major\">Exp. %s %d%s", show_expected_time(expected, deviation, late), deviation, late?"L":"E");
      sprintf(mobile_analysis, "%d%s", deviation, late?"L":"E");
      if(deviation >= 3) nlate++;
      mobile_act = mobile_sched; // TODO
      break;

   case 3: // Cape
      strcpy(row_class, "summ-table-cape");
      sprintf(analysis, "<td class=\"small-table-crit\">Cancelled");
      strcpy(mobile_analysis, "");
      ncape++;
      break;

   case 4: // Arrived
   case 5: // Departed
      strcpy(row_class, "summ-table-gone");
      if(!deviation ) sprintf(analysis, "<td class=\"summ-table-good\">On time");
      else if(deviation < 3) sprintf(analysis, "<td class=\"summ-table-good\">%s %d%s",  show_trust_time_nocolon(actual, true), deviation, late?"L":"E");
      else if(deviation < 6) sprintf(analysis, "<td class=\"summ-table-minor\">%s %d%s", show_trust_time_nocolon(actual, true), deviation, late?"L":"E");
      else                   sprintf(analysis, "<td class=\"summ-table-major\">%s %d%s", show_trust_time_nocolon(actual, true), deviation, late?"L":"E");
      sprintf(mobile_analysis, "%d%s", deviation, late?"L":"E");
      if(deviation >= 3) nlate++;
      mobile_act = mobile_sched; // TODO
      break;

   }

   if(deduced) ndeduced++;
   if(status == 4) narrival++;

   // Mung mobile times
   if(mobile_sched < 400) mobile_sched += 2400;
   if(mobile_act   < 400) mobile_act   += 2400;

   _log(DEBUG, "Mobile times: sched %d, act %d, requested time = %d", mobile_sched, mobile_act, mobile_time);

   // Print it
   switch(mode)
   {
   case UPDATE:
      printf("tr%d%d|%s|<td>%s</td>%s", row/rows, row%rows, row_class, train_details, analysis );
      if(deduced || status == 4)
      {
         printf("&nbsp;<span class=\"symbols\">");
         if(deduced) printf("&loz;");
         if(status == 4) printf("&alpha;");
         printf("</span>");
      }
      printf("</td>\n");
      break;

   case SUMMARY:
      printf("<tr id=\"tr%d%d\" class=\"%s\"><td>%s</td>%s", row/rows, row%rows, row_class, train_details, analysis);
      // Symbols
      if(deduced || status == 4)
      {
         printf("&nbsp;<span class=\"symbols\">");
         if(deduced) printf("&loz;");
         if(status == 4) printf("&alpha;");
         printf("</span>");
      }
      printf("</td></tr>\n");
      break;

   case MOBILE:
      if(shown < mobile_trains)
      {
         if(mobile_sched > mobile_time || mobile_act > mobile_time)
         {
            printf("%s %s|%d|%s|%ld\n", depart, destination, status, mobile_analysis, 0L);
            shown++;
         }
      }
      break;

   default:
      break;
   }

   // After print
   row++;
   train++;

   if(train == trains)
   {
      // Last one printed, do the tail
      while((row++) % rows)
      {
         if(mode == SUMMARY)
         {
            printf("<tr class=\"summ-table-idle\"><td>&nbsp;</td><td>&nbsp;</td></tr>\n");
         }
      }
      if(mode == SUMMARY) printf("</table></td><td>");
      
      // Last column of outer table - Key
      if(mode == UPDATE)
      {
         printf("tr49|summ-table-idle|<td>%d trains.</td>\n", trains - nbus);
         printf("tr410|summ-table-idle|<td%s>%d not on time.</td>\n", nlate?" class=\"summ-table-minor\"":"", nlate);
         printf("tr411|summ-table-idle|<td%s>%d cancelled.</td>\n", ncape?" class=\"summ-table-cape\"":"", ncape);
         printf("tr412|summ-table-idle|<td>%d buses.</td>\n", nbus);
         printf("tr413|summ-table-idle|<td>%d activation deduced.</td>\n", ndeduced);
         printf("tr414|summ-table-idle|<td>%d departure not reported.</td>\n", narrival);
      }
      else if(mode == SUMMARY)
      {
         printf("<table>");
         printf("<tr class=\"summ-table-head\"><th>Key</th></tr>\n");
         printf("<tr class=\"summ-table-move\"><td>Train moving.</td></tr>");
         printf("<tr class=\"summ-table-act\"><td>Train activated.</td></tr>");
         printf("<tr class=\"summ-table-idle\"><td>&nbsp;</td></tr>");
         printf("<tr class=\"summ-table-idle\"><td>Deduced activation.&nbsp;&nbsp;<span class=\"symbols\">&loz;</span></td></tr>");
         printf("<tr class=\"summ-table-idle\"><td>Departure not reported.&nbsp;&nbsp;<span class=\"symbols\">&alpha;</span></td></tr>");
         printf("<tr class=\"summ-table-idle\"><td>&nbsp;</td></tr>");
         printf("<tr class=\"summ-table-idle\"><td>&nbsp;</td></tr>");
         printf("<tr class=\"summ-table-head\"><th>Statistics</th></tr>\n");
         printf("<tr id=\"tr49\" class=\"summ-table-idle\"><td>%d trains.</td></tr>", trains - nbus);
         printf("<tr id=\"tr410\" class=\"summ-table-idle\"><td%s>%d not on time.</td></tr>", nlate?" class=\"summ-table-minor\"":"", nlate);
         printf("<tr id=\"tr411\" class=\"summ-table-idle\"><td%s>%d cancelled.</td></tr>", ncape?" class=\"summ-table-cape\"":"", ncape);
         printf("<tr id=\"tr412\" class=\"summ-table-idle\"><td>%d buses.</td></tr>", nbus);
         printf("<tr id=\"tr413\" class=\"summ-table-idle\"><td>%d activation deduced.</td></tr>", ndeduced);
         printf("<tr id=\"tr414\" class=\"summ-table-idle\"><td>%d departure not reported.</td></tr>", narrival);
         row = 14;
         while((row++) % rows)
         {
            printf("<tr class=\"summ-table-idle\"><td>&nbsp;</td></tr>\n");
         }
         printf("</table>");
            
         printf("</td></tr></table>\n");
      }
   }
   return;
}

static void as(void)
{
   if((!parameters[1][0]) && (!parameters[2][0]))
   {
      // No parameters, display screen
      printf("<table><tr valign=\"top\"><td class=\"control-panel\">");
      printf("<h4>Go direct to schedule</h4>\n");
      printf("Garner schedule ID <input type=\"text\" id=\"as-g-id\" size=\"10\" maxlength=\"10\" value=\"\" onkeydown=\"if(event.keyCode == 13) as_go_onclick();\">&nbsp; &nbsp\n");
      printf("<br><button class=\"cp-button\" onclick=\"as_go_onclick();\">Go</button>\n");

      printf("</td><td width=\"10%%\">&nbsp;</td><td class=\"control-panel\">\n");
   
      printf("<h4>Advanced Search</h4>\n");
      printf("<table>");
      printf("<tr><td>CIF UID</td><td><input type=\"text\" id=\"as-uid\" size=\"16\" maxlength=\"64\" value=\"\" onkeydown=\"if(event.keyCode == 13) as_search_onclick();\"></td><td></td></tr>\n");
      printf("<tr><td>Headcode</td><td><input type=\"text\" id=\"as-head\" size=\"16\" maxlength=\"64\" value=\"\" onkeydown=\"if(event.keyCode == 13) as_search_onclick();\"></td><td></td></tr>\n");
      printf("</table>\n");

      printf("<br><button class=\"cp-button\" onclick=\"as_search_onclick();\">Search</button>\n");

      printf("</td></tr></table>\n");
   }
   else
   {
      MYSQL_RES * result0, * result1;
      MYSQL_ROW row0, row1;
      char query[1024], zs[128];

      if(parameters[1][0] || parameters[2][0])
      {
         //                      0            1                    2                     3                  4             5               6       7
         sprintf(query, "SELECT train_status, schedule_start_date, schedule_end_date, signalling_id, CIF_train_uid, CIF_stp_indicator, update_id, id, ");
         strcat(query, "runs_mo, runs_tu, runs_we, runs_th, runs_fr, runs_sa, runs_su, deleted");
         strcat(query,  " FROM cif_schedules WHERE ");
         if(parameters[2][0])
         {
            // Sanitise
            char safe[256];
            db_real_escape_string(safe, parameters[2], strlen(parameters[2]));
            sprintf(zs, "signalling_id = '%s'", safe);
         }
         else
         {
            // Sanitise
            char safe[256];
            db_real_escape_string(safe, parameters[1], strlen(parameters[1]));
            // Insert missing leading space if necessary
            if(strlen(parameters[1]) == 5)
               sprintf(zs, "CIF_train_uid = ' %s'", safe);
            else
               sprintf(zs, "CIF_train_uid = '%s'", safe);
         }
         strcat(query, zs);
         strcat(query, " ORDER BY schedule_start_date LIMIT 128");
         if(!db_query(query))
         {
            result0 = db_store_result();
            word matches = mysql_num_rows(result0);
            if(matches == 0) 
            {
               mysql_free_result(result0);
               display_control_panel("", 0);
               printf("<p>No matching trains found.</p>");
               mysql_free_result(result0);
               return;
            }
            if(matches == 1)
            {
               row0 = mysql_fetch_row(result0);
               strcpy(parameters[1], row0[7]);
               parameters[2][0]= '\0';
               mysql_free_result(result0);
               train();
               return;
            }

            // Found multiple matches, display the list
            display_control_panel("" , 0);
            printf("<p>%d matching schedules found.</p>\n", matches);
            printf("<table>\n");
            printf("<tr class=\"small-table\"><th>Detail</th><th>Type</th><th>ID</th><th>CIF ID</th><th>STP</th><th>From</th><th>To</th><th>Days</th><th>Valid dates</th></tr>\n");
            while((row0 = mysql_fetch_row(result0)))
            {
               printf("<tr class=\"small-table\">");
               word vstp = (row0[6][0] == '0' && row0[6][1] == 0);
           
               // Link
               dword id = atol(row0[7]);
               printf("<td><a class=\"linkbutton\" href=\"%strain/%ld\">Details</a></td>\n", URL_BASE, id);

               // Status
               if(vstp) printf("<td class=\"small-table-vstp\">V");
               else if(row0[0][0] == 'F' || row0[0][0] == '2' || row0[0][0] == '3') printf("<td class=\"small-table-freight\">");
               else printf("<td>");

               switch(row0[0][0])
               {
               case 'B': printf("Bus</td>");         break;
               case 'F': printf("Freight</td>");     break;
               case 'P': printf("Passenger</td>");   break;
               case 'T': printf("Trip</td>");        break;
               case '1': printf("STP passenger</td>");    break;
               case '2': printf("STP freight</td>"); break;
               case '3': printf("STP trip</td>");    break;
               case '5': printf("STP bus</td>");     break;
               default:  printf("%s</td>", row0[0]); break;
               }

               // ID (Headcode if we know it)
               printf("<td>");
               if(row0[3][0])
                  printf("%s", row0[3]);
               else
                  printf("%ld", id);
               printf("</td>");
               
               // CIF ID
               printf("<td>%s</td>", show_spaces(row0[4]));
               
               // CIF STP indicator
               switch(row0[5][0])
               {
               case 'C': printf("<td>Cancelled</td>"); break;
               case 'N': printf("<td>New</td>");          break;
               case 'O': printf("<td>Overlay</td>");      break;
               case 'P': printf("<td>Permanent</td>");    break;
               default:  printf("<td>%s</td>", row0[5]);  break;
               }
               
               // From
               sprintf(query, "SELECT tiploc_code, departure, public_departure FROM cif_schedule_locations where cif_schedule_id = %ld AND record_identity = 'LO'", id);
               printf("<td>");
               if(!db_query(query))
               {
                  result1 = db_store_result();
                  
                  if((row1 = mysql_fetch_row(result1)))
                  {
                     printf("%s %s", location_name_link(row1[0], true, 0), show_time(row1[1]));
                     if(row1[2][0]) printf("(%s)", show_time(row1[2]));
                  }
                  mysql_free_result(result1);
               }
               printf("</td>");
               
               // To
               sprintf(query, "SELECT tiploc_code, arrival, public_arrival FROM cif_schedule_locations where cif_schedule_id = %ld AND record_identity = 'LT'", id);
               printf("<td>");
               if(!db_query(query))
               {
                  result1 = db_store_result();
               
                  if((row1 = mysql_fetch_row(result1)))
                  {
                     printf("%s %s", location_name_link(row1[0], true, 0), show_time(row1[1]));
                     if(row1[2][0]) printf("(%s)", show_time(row1[2]));
                  }
                  mysql_free_result(result1);
               }
               printf("</td>");

               // Days
               printf("<td>");
               if(row0[ 8][0] == '1') printf("Mo ");
               if(row0[ 9][0] == '1') printf("Tu ");
               if(row0[10][0] == '1') printf("We ");
               if(row0[11][0] == '1') printf("Th ");
               if(row0[12][0] == '1') printf("Fr ");
               if(row0[13][0] == '1') printf("Sa ");
               if(row0[14][0] == '1') printf("Su ");
               printf("</td>");
               // Dates
               {
                  time_t start = atol(row0[1]);
                  time_t end   = atol(row0[2]);
                  time_t deleted = atol(row0[15]);
                  printf("<td>%s", date_text(start, 0));
                  printf(" - %s", date_text(end, 0));
                  if(deleted < now) printf(" Schedule deleted %s.", date_text(deleted, 0));
                  printf("</td>");
               }
               printf("</tr>");        
            }
         mysql_free_result(result0);
            printf("</table>\n");
            if(matches >= 128) printf("<p>Matches limited to 128.</p>\n");
            return;            
         }
         printf("<p>No matching trains found.</p>");
         return;
      }
      else
      {
         printf("<p>No search parameters entered.</p>");
         return;
      }
   }
}

static void train(void)
{
   char query[2048], zs[128];
   MYSQL_RES * result0, * result1;
   MYSQL_ROW row0, row1;
   char train[128];

   // Decode parameters
   dword schedule_id = atol(parameters[1]);
   if(!schedule_id) return;

   // Date
   time_t when;
   if(atoi(parameters[2]) > 0 && atoi(parameters[3]) > 0)
   {
      struct tm broken;
      broken = *(localtime(&now)); // Pre-populate
      broken.tm_mday = atoi(parameters[2]);
      broken.tm_mon = atoi(parameters[3]) - 1;
      if(atoi(parameters[4])) broken.tm_year = atoi(parameters[4]) + 100;
      broken.tm_hour = 12;
      broken.tm_min = 0;
      broken.tm_sec = 0;
      broken.tm_isdst = -1;
      when = timegm(&broken);
   }
   else when = 0;
   // In here when = 0 if no date supplied, NOT today

   display_control_panel("", when);

   train[0] = '\0';

   sprintf(query, "SELECT tiploc_code, departure FROM cif_schedule_locations WHERE record_identity = 'LO' AND cif_schedule_id = %ld", schedule_id);
   if(!db_query(query))
   {
      result0 = db_store_result();
      if((row0 = mysql_fetch_row(result0)))
      {
         sprintf(train, "%s %s to ", show_time(row0[1]), location_name(row0[0], true));
      }
      mysql_free_result(result0);
   }
   sprintf(query, "SELECT tiploc_code FROM cif_schedule_locations WHERE record_identity = 'LT' AND cif_schedule_id = %ld", schedule_id);
   if(!db_query(query))
   {
      result0 = db_store_result();
      if((row0 = mysql_fetch_row(result0)))
      {
         strcat (train, location_name(row0[0], true));
      }
      mysql_free_result(result0);
   }

   //                      0        1                         2                  3              4                     5          6              7              
   strcpy(query, "SELECT update_id, CIF_bank_holiday_running, CIF_stp_indicator, CIF_train_uid, applicable_timetable, atoc_code, uic_code, schedule_end_date, ");
   //              8              9                 10            11                   12                       13                  14               15             
   strcat(query, "signalling_id, CIF_train_category, CIF_headcode, CIF_train_service_code, CIF_business_sector, CIF_power_type, CIF_timing_load, CIF_speed,");
   //              16                             17               18             19                20                       21                    22                   23                 24
   strcat(query, "CIF_operating_characteristics, CIF_train_class, CIF_sleepers, CIF_reservations, CIF_connection_indicator, CIF_catering_code, CIF_service_branding, schedule_start_date, train_status,");
   //              25       26                                           31       32       33
   strcat(query, "runs_mo, runs_tu, runs_we, runs_th, runs_fr, runs_sa, runs_su, created, deleted");
   sprintf(zs, " FROM cif_schedules WHERE id=%ld", schedule_id);
   strcat(query, zs);

   if(!db_query(query))
   {
      result0 = db_store_result();
      if((row0 = mysql_fetch_row(result0)))
      {
         printf("<h2>Train %ld (%s) %s %s</h2>\n", schedule_id, show_spaces(row0[3]), row0[8], train);

         printf("<table><tr><td style=\"vertical-align:top;\">"); // Outer table
         printf("<table>");
         printf("<tr bgcolor=\"#eeeeee\"><td>Update ID</td><td>%s</td><td>", row0[0]);
         dword up_id = atol(row0[0]);
         if(up_id == 0) printf("VSTP");
         if(up_id == 1) printf("Full");
                            
         if(up_id > 0)
         {
            sprintf(zs, "SELECT time FROM updates_processed where id = %s", row0[0]);
            if(!db_query(zs))
            {
               result1 = db_store_result();
               if((row1 = mysql_fetch_row(result1)))
               {
                  printf(" %s", time_text(atol(row1[0]), false));
               }
               mysql_free_result(result1);
            }
         }
         printf("</td></tr>\n");

         printf("<tr bgcolor=\"#eeeeee\"><td>Created</td><td></td><td>%s</td></tr>", time_text(atol(row0[32]), 0));

         if(strtoul(row0[33], NULL, 10) < 0xffffffff)
         {
            strcpy(zs, time_text(atol(row0[33]), 0));
         }
         else
         {
            zs[0] = '\0';
         }
         printf("<tr bgcolor=\"#eeeeee\"><td>Deleted</td><td></td><td>%s</td></tr>", zs);

         printf("<tr bgcolor=\"#eeeeee\"><td>Bank holiday running</td><td>%s</td><td>", row0[1]);
         switch(row0[1][0])
         {
         case 'X' : printf("Not on bank holidays."); break;
         case 'G': printf("Not on Glasgow bank holidays."); break;
         default: printf("%s", row0[1]);
         }
         printf("</td></tr>\n");

         printf("<tr bgcolor=\"#eeeeee\"><td>STP Indicator</td><td>%s</td>", row0[2]);
         // CIF STP indicator
         switch(row0[2][0])
         {
         case 'C': printf("<td>Cancelled</td>");    break;
         case 'N': printf("<td>New</td>");          break;
         case 'O': printf("<td>Overlay</td>");      break;
         case 'P': printf("<td>Permanent</td>");    break;
         default:  printf("<td>Unrecognised</td>"); break;
         }
         printf("</tr>");

         printf("<tr bgcolor=\"#eeeeee\"><td>Train UID</td><td>%s</td><td></td></tr>\n", show_spaces(row0[3]));

         // printf("<tr bgcolor=\"#eeeeee\"><td>Applicable Timetable</td><td>%s</td>", row0[4]);
         printf("<tr bgcolor=\"#eeeeee\"><td>ATOC code</td><td>%s</td><td></td></tr>", row0[5]);
         //printf("<tr bgcolor=\"#eeeeee\"><td>Traction Class</td><td>%s</td>", row0[6]);
         printf("<tr bgcolor=\"#eeeeee\"><td>UIC code</td><td>%s</td><td></td></tr>", row0[6]);
         printf("<tr bgcolor=\"#eeeeee\"><td>Days runs</td><td colspan=2>");
         if(row0[25][0] == '1') printf("Mo ");
         if(row0[26][0] == '1') printf("Tu ");
         if(row0[27][0] == '1') printf("We ");
         if(row0[28][0] == '1') printf("Th ");
         if(row0[29][0] == '1') printf("Fr ");
         if(row0[30][0] == '1') printf("Sa ");
         if(row0[31][0] == '1') printf("Su ");
         printf("</td></tr>\n");
         printf("<tr bgcolor=\"#eeeeee\"><td>Schedule Dates</td><td colspan=2>%s", date_text(atol(row0[23]), 0));
         printf(" - %s</td></tr>", date_text(atol(row0[7]), 0));
         printf("<tr bgcolor=\"#eeeeee\"><td>Signalling ID</td><td>%s</td><td></td></tr>", row0[8]);
         printf("<tr bgcolor=\"#eeeeee\"><td>Train category</td>");
         word i;
         for(i=0; i < CATEGORIES && (categories[i][0]!=row0[9][0] || categories[i][1]!=row0[9][1]) ; i++);
         if(i < CATEGORIES)
            printf("<td>%s</td><td>%s</td></tr>", row0[9], categories[i]+2);
         else if(row0[9][0])
            printf("<td>%s</td><td>Unrecognised</td></tr>", row0[9]);
         else
            printf("<td>%s</td><td></td></tr>", row0[9]);

         printf("<tr bgcolor=\"#eeeeee\"><td>TOC Headcode</td><td>%s</td><td></td></tr>\n", row0[10]);
         // "Not used" printf("<tr bgcolor=\"#eeeeee\"><td>CIF course indicator</td><td>%s</td>", row0[13]);
         printf("<tr bgcolor=\"#eeeeee\"><td>Train service code</td><td>%s</td><td></td></tr>\n", row0[11]);
         printf("<tr bgcolor=\"#eeeeee\"><td>Business Sector</td><td>%s</td><td></td></tr>\n", row0[12]);

         printf("<tr bgcolor=\"#eeeeee\"><td>Power type</td><td>%s</td><td>\n", row0[13]);
         sprintf(zs, "%-3s", row0[13]);
         for(i=0; i < POWER_TYPES && strncmp(power_types[i], zs, 3) ; i++);
         if(i < POWER_TYPES)
            printf("%s", power_types[i]+3);
         else if(row0[9][0])
            printf("Unrecognised");
         printf("</td></tr>");

           printf("<tr bgcolor=\"#eeeeee\"><td>Timing Load</td><td>%s</td><td>", row0[14]);
         if(!row0[14][1]) switch(row0[14][0])
                         {
                         case 0:   printf("&nbsp;"); break;
                         case 'A': printf("Class 14x"); break;
                         case 'E': printf("Class 158"); break;
                         case 'N': printf("Class 165/0"); break;
                         case 'S': printf("Class 150, 153, 155, or 156"); break;
                         case 'T': printf("Class 165/1 or 166"); break;
                         case 'V': printf("Class 220/1"); break;
                         case 'X': printf("Class 159"); break;
                         default:  printf("Unrecognised"); break;
                         }
         printf("</td></tr>\n");
         printf("<tr bgcolor=\"#eeeeee\"><td>Speed</td>");
         if(row0[15][0])
            printf("<td>%s</td><td>mph</td>", row0[15]);
         else
            printf("<td>&nbsp;</td><td>&nbsp;</td>");
         printf("<tr bgcolor=\"#eeeeee\"><td>Operating Characteristics</td><td>%s</td><td>", row0[16]);
         for(i=0; i<6 && row0[16][i]; i++)
         {
            if(i) printf("<br>");
            switch(row0[16][i])
            {
            case 'B': printf("Vacuum Braked"); break;
            case 'C': printf("Timed at 100mph"); break;
            case 'D': printf("DOO Coaching Stock"); break;
            case 'E': printf("Mk4 Coaches"); break;
            case 'G': printf("Trainman (Guard) Required"); break;
            case 'M': printf("Timed at 110 mph"); break;
            case 'P': printf("Push/Pull Train"); break;
            case 'Q': printf("Runs as required"); break;
            case 'R': printf("Air conditioned with PA"); break;
            case 'S': printf("Steam heated"); break;
            case 'Y': printf("Runs to Terminals/Yards as required");  break;
            case 'Z': printf("SB1C gauge. Not to be diverted w/o authority"); break;
            default:  printf("Unrecognised"); break;
            }
         }
         printf("</td></tr>\n");

         printf("<tr bgcolor=\"#eeeeee\"><td>Seating Class</td><td>%s</td><td>", row0[17]);
         switch(row0[17][0])
         {
         case 0:   printf("&nbsp;"); break;
         case 'B': printf("First and Standard."); break;
         case ' ': printf("First and Standard."); break;
         case 'S': printf("Standard only."); break;
         default:  printf("Unrecognised"); break;
         }
         printf("</td></tr>\n");

         printf("<tr bgcolor=\"#eeeeee\"><td>Sleepers</td><td>%s</td><td>", row0[18]);
         switch(row0[18][0])
         {
         case 0:   printf("&nbsp;"); break;
         case 'B': printf("First and Standard."); break;
         case 'F': printf("First only."); break;
         case 'S': printf("Standard only."); break;
         default:  printf("Unrecognised"); break;
         }
         printf("</td></tr>\n");

         printf("<tr bgcolor=\"#eeeeee\"><td>Reservations</td><td>%s</td><td>", row0[19]);
         switch(row0[19][0])
         {
         case 0:
         case '0': printf("&nbsp;"); break;
         case 'A': printf("Compulsory."); break;
         case 'E': printf("Bicycles essential."); break;
         case 'R': printf("Recommended."); break;
         case 'S': printf("Possible."); break;
         default:  printf("Unrecognised"); break;
         }
         printf("</td></tr>\n");
         printf("<tr bgcolor=\"#eeeeee\"><td>Connection Indicator</td><td></td><td>%s</td></tr>\n", row0[20]);
         printf("<tr bgcolor=\"#eeeeee\"><td>Catering Code</td><td></td><td>%s</td></tr>\n", row0[21]);
         printf("<tr bgcolor=\"#eeeeee\"><td>Service Branding</td><td></td><td>%s</td></tr>\n", row0[22]);
         printf("<tr bgcolor=\"#eeeeee\"><td>Train Status</td><td>%s</td>", row0[24]);
         switch(row0[24][0])
         {
         case 'B': printf("<td>Bus</td>");          break;
         case 'F': printf("<td>Freight</td>");      break;
         case 'P': printf("<td>Passenger</td>");    break;
         case 'T': printf("<td>Trip</td>");         break;
         case '1': printf("<td>STP passenger</td>");break;
         case '2': printf("<td>STP freight</td>");  break;
         case '3': printf("<td>STP trip</td>");     break;
         case '5': printf("<td>STP bus</td>");      break;
         default:  printf("<td>Unrecognised</td>"); break;
         }
         printf("</tr></table>\n");


         printf("</td><td width=\"5%%\">&nbsp;</td><td style=\"vertical-align:top;\">\n"); // Outer table

         //                      0              1                2            3                4        5          6      7               8                9        10    11    12                       13                 14                   15  16
         sprintf(query, "SELECT location_type, record_identity, tiploc_code, tiploc_instance, arrival, departure, pass, public_arrival, public_departure, platform, line, path, engineering_allowance, pathing_allowance, performance_allowance, id, location_type FROM cif_schedule_locations WHERE cif_schedule_id = %ld ORDER BY next_day,sort_time", schedule_id);

         if(!db_query(query))
         {
            printf("<table>");
            printf("<tr class=\"small-table\"><th>Location</th><th>Times WTT(Public)</th><th>Plat</th><th>Line</th><th>Allowances</th><th>&nbsp;</th>\n");
            result0 = db_store_result();
            while((row0 = mysql_fetch_row(result0)))
            {
               printf("<tr class=\"small-table\">\n");
               
               printf("<td>%s</td>", location_name_link(row0[2], false, when));
               printf("<td>");
               if(row0[4][0]) printf("a: %s", show_time(row0[4])); // arrival
               if(row0[7][0]) printf("(%s)",  show_time(row0[7])); // public arrival
               if(row0[5][0]) printf(" d: %s",show_time(row0[5])); // dep
               if(row0[8][0]) printf("(%s)",  show_time(row0[8])); // public dep
               if(row0[6][0]) printf("p: %s", show_time(row0[6])); // pass
               printf("</td>\n");

               printf("<td>%s</td><td>%s</td>", row0[9], row0[10]);

               printf("<td>");
               if(row0[12][0]) printf("Eng: %s ",  show_time(row0[12]));
               if(row0[13][0]) printf("Path: %s ", show_time(row0[13]));
               if(row0[14][0]) printf("Perf: %s ", show_time(row0[14]));
               printf("</td>\n");

               // Activities
               printf("<td>%s</td>\n", show_act(row0[16]));

               printf("</tr>\n");
            }
            printf("</table>\n");
         }
      }

      printf("</td></tr></table>\n"); // Outer table
      ///printf("<table><tr><td class=\"control-panel\">&nbsp;");
      printf("<a class=\"linkbutton-cp\" href=\"%strain_text/%ld\" target=\"_blank\">Display in plain text</a>\n", URL_BASE, schedule_id);
      ///printf("&nbsp;</td></tr></table>\n");

      // TRUST
      if(when)
      {
         struct tm * broken = gmtime(&when);
         printf("<table><tr>\n");
         printf("<td>Real Time Data For %s %02d/%02d/%02d</td>\n", days[broken->tm_wday % 7], broken->tm_mday, broken->tm_mon + 1, broken->tm_year % 100);
         printf("<td width = \"10%%\">&nbsp;</td>\n");
         printf("<td class=\"control-panel-row\"> Show real time data for date ");
         printf("<input type=\"text\" id=\"train_date\" size=\"8\" maxlength=\"8\" value=\"\" onkeydown=\"if(event.keyCode == 13) train_date_onclick(); else ar_off();\">\n");
         printf(" <button id=\"search\" class=\"cp-button\" onclick=\"train_date_onclick();\">Show</button> </td>\n");
         printf("</tr></table>\n");
         printf("<table>");
         printf("<tr class=\"small-table\"><th>Received</th><th>Event</th><th>Source</th><th>Location</th><th>P</th><th>Event Type</th><th>Planned Type</th><th>WTT</th><th>GBTT</th><th>Actual</th><th>Var</th><th>Next Report (Run Time)</th><th>Flags</th></tr>\n");
         char query[512];
         MYSQL_RES * result2;
         MYSQL_ROW row2;

         // Calculate dom from when, deducting 24h if next_day and/or adding 24h if after 00:00.
         // time_t start_date = when - (next_day?(24*60*60):0);
         // if(sort_time < DAY_START) start_date += (24*60*60);
         // TO BE DONE
         // NO NO NO.  The link to get here MUST have the start date of the train, NOT the date at the point clicked.
         time_t start_date = when;

         byte dom = broken->tm_mday;

         char trust_id[16];
         // Only accept activations where dom matches, and are +- 4 days (To eliminate last months activation.)  YUK
         sprintf(query, "SELECT created, trust_id, deduced FROM trust_activation WHERE cif_schedule_id = %ld AND substring(trust_id FROM 9) = '%02d' AND created > %ld AND created < %ld order by created", schedule_id, dom, start_date - 4*24*60*60, start_date + 4*24*60*60);
         if(!db_query(query))
         {
            result1 = db_store_result();
            while((row1 = mysql_fetch_row(result1)))
            {
               printf("<tr class=\"small-table-act\"><td>%s</td><td>Activation</td><td colspan=11>%sTrain ID = \"%s\"</td></tr>\n", time_text(atol(row1[0]), true), (atoi(row1[2]))?"[DEDUCED] ":"", row1[1]);
               sprintf(query, "SELECT created, reason, type from trust_cancellation where trust_id='%s' AND created > %ld AND created < %ld order by created ", row1[1], start_date - 4*24*60*60, start_date + 4*24*60*60);
               strcpy(trust_id, row1[1]);
            }
            mysql_free_result(result1);
               
            if(!db_query(query))
            {
               result2 = db_store_result();
               while((row2 = mysql_fetch_row(result2)))
               {
                  printf("<tr class=\"small-table-cape\"><td>%s</td><td>Cancellation</td><td colspan=11>Reason %s (%s)</td></tr>\n", time_text(atol(row2[0]), true), row2[1], row2[2]);
               }
               mysql_free_result(result2);
            }

            //                       0       1           2                   3         4           5                 6               7                   8                   9             10            11                12                  13                14   
            sprintf(query, "SELECT created, event_type, planned_event_type, platform, loc_stanox, actual_timestamp, gbtt_timestamp, planned_timestamp, timetable_variation, event_source, offroute_ind, train_terminated, variation_status, next_report_stanox, next_report_run_time from trust_movement where trust_id='%s' AND created > %ld AND created < %ld order by actual_timestamp, planned_timestamp, created", trust_id, start_date - 4*24*60*60, start_date + 4*24*60*60);
            if(!db_query(query))
            {
               result2 = db_store_result();
               while((row2 = mysql_fetch_row(result2)))
               {
                  printf("<tr class=\"small-table\"><td>%s</td><td>Movement</td>", time_text(atol(row2[0]), true));
                  printf("<td>%s</td>", row2[9]);
                  printf("<td>%s</td>", show_stanox_link(row2[4]));
                  printf("<td>%s</td>", row2[3]); // Platform
                  printf("<td>%s</td><td>%s</td>", row2[1], row2[2]);
                  printf("<td>%s</td>", show_trust_time(row2[7], true));
                  printf("<td>%s</td>", show_trust_time(row2[6], true));
                  printf("<td>%s</td>", show_trust_time(row2[5], true));
                  printf("<td>%s %s</td>", row2[8], row2[12]);
                  if(row2[13][0])
                     printf("<td>%s (%s)</td>", show_stanox(row2[13]), row2[14]);
                  else
                     printf("<td></td>");
                  printf("<td>%s %s</td>", (row2[10][0] == '0')?"":"Off Route", (row2[11][0] == '0')?"":"Terminated");
                  printf("</tr>\n");
               }
               mysql_free_result(result2);
            }
            
         }
      }
      else
      {
         printf("<p>\n");
         printf("<table><tr><td class=\"control-panel-row\"> Show real time data for date ");
         printf("<input type=\"text\" id=\"train_date\" size=\"8\" maxlength=\"8\" value=\"\" onkeydown=\"if(event.keyCode == 13) train_date_onclick(); else ar_off();\">\n");
         printf(" <button id=\"search\" class=\"cp-button\" onclick=\"train_date_onclick();\">Show</button> \n");
         printf("</td></tr></table>\n");
         printf("</p>\n");
      }
      printf("</table>\n");

   }
}

static void train_text(void)
{
   char query[2048], zs[128];
   MYSQL_RES * result0, * result1;
   MYSQL_ROW row0, row1;
   char train[128];

   // Decode parameters
   dword schedule_id = atol(parameters[1]);
   if(!schedule_id) return;

   train[0] = '\0';

   sprintf(query, "SELECT tiploc_code, departure FROM cif_schedule_locations WHERE record_identity = 'LO' AND cif_schedule_id = %ld", schedule_id);
   if(!db_query(query))
   {
      result0 = db_store_result();
      if((row0 = mysql_fetch_row(result0)))
      {
         sprintf(train, "%s %s to ", show_time(row0[1]), location_name(row0[0], true));
      }
   }
   sprintf(query, "SELECT tiploc_code FROM cif_schedule_locations WHERE record_identity = 'LT' AND cif_schedule_id = %ld", schedule_id);
   if(!db_query(query))
   {
      result0 = db_store_result();
      if((row0 = mysql_fetch_row(result0)))
      {
         strcat (train, location_name(row0[0], true));
      }
   }

   //                      0        1                         2                  3              4                     5          6              7              
   strcpy(query, "SELECT update_id, CIF_bank_holiday_running, CIF_stp_indicator, CIF_train_uid, applicable_timetable, atoc_code, uic_code, schedule_end_date, ");
   //              8              9                 10            11                   12                       13                  14               15             
   strcat(query, "signalling_id, CIF_train_category, CIF_headcode, CIF_train_service_code, CIF_business_sector, CIF_power_type, CIF_timing_load, CIF_speed,");
   //              16                             17               18             19                20                       21                    22                   23                 24
   strcat(query, "CIF_operating_characteristics, CIF_train_class, CIF_sleepers, CIF_reservations, CIF_connection_indicator, CIF_catering_code, CIF_service_branding, schedule_start_date, train_status,");
   //              25       26                                           31       32        33
   strcat(query, "runs_mo, runs_tu, runs_we, runs_th, runs_fr, runs_sa, runs_su, created, deleted");
   sprintf(zs, " FROM cif_schedules WHERE id=%ld", schedule_id);
   strcat(query, zs);

   printf("<pre>\n");
   if(!db_query(query))
   {
      result0 = db_store_result();
      if((row0 = mysql_fetch_row(result0)))
      {
         printf("Train %ld (%s) %s %s\n\n", schedule_id, row0[3], row0[8], train);

         //                      :
         printf("                Update ID: %s ", row0[0]);
         dword up_id = atol(row0[0]);
         if(up_id == 0) printf("VSTP");
         if(up_id == 1) printf("Full");
                            
         if(up_id > 0)
         {
            sprintf(zs, "SELECT time FROM updates_processed where id = %s", row0[0]);
            if(!db_query(zs))
            {
               result1 = db_store_result();
               if((row1 = mysql_fetch_row(result1)))
               {
                  printf(" %s", time_text(atol(row1[0]), false));
               }
               mysql_free_result(result1);
            }
         }
         printf("\n");

         printf("                  Created: %s\n", time_text(atol(row0[32]), 0));

         if(strtoul(row0[33], NULL, 10) < 0xffffffff)
         {
            strcpy(zs, time_text(atol(row0[33]), 0));
         }
         else
         {
            zs[0] = '\0';
         }
         printf("                  Deleted: %s\n", zs);

         printf("     Bank holiday running: %s ", row0[1]);
         switch(row0[1][0])
         {
         case 'X' : printf("Not on bank holidays."); break;
         case 'G': printf("Not on Glasgow bank holidays."); break;
         default: printf("%s", row0[1]);
         }
         printf("\n");

         printf("            STP Indicator: %s ", row0[2]);
         // CIF STP indicator
         switch(row0[2][0])
         {
         case 'C': printf("Cancelled");    break;
         case 'N': printf("New");          break;
         case 'O': printf("Overlay");      break;
         case 'P': printf("Permanent");    break;
         default:  printf("Unrecognised"); break;
         }
         printf("\n");

         printf("                Train UID: %s\n", row0[3]);
         printf("                ATOC code: %s\n", row0[5]);
         printf("                 UIC code: %s\n", row0[6]);
         printf("                Days runs: ");
         if(row0[25][0] == '1') printf("Mo ");
         if(row0[26][0] == '1') printf("Tu ");
         if(row0[27][0] == '1') printf("We ");
         if(row0[28][0] == '1') printf("Th ");
         if(row0[29][0] == '1') printf("Fr ");
         if(row0[30][0] == '1') printf("Sa ");
         if(row0[31][0] == '1') printf("Su ");
         printf("\n");
         printf("           Schedule Dates: %s", date_text(atol(row0[23]), 0));
         printf(" - %s\n", date_text(atol(row0[7]), 0));
         printf("            Signalling ID: %s\n", row0[8]);
         printf("           Train category: ");
         word i;
         for(i=0; i < CATEGORIES && (categories[i][0]!=row0[9][0] || categories[i][1]!=row0[9][1]) ; i++);
         if(i < CATEGORIES)
            printf("%s %s", row0[9], categories[i]+2);
         else if(row0[9][0])
            printf("%s Unrecognised", row0[9]);
         else
            printf("%s", row0[9]);
         printf("\n");
         printf("             TOC Headcode: %s\n", row0[10]);
         printf("       Train service code: %s\n", row0[11]);
         printf("          Business Sector: %s\n", row0[12]);
         printf("               Power type: %s ", row0[13]);
         sprintf(zs, "%-3s", row0[13]);
         for(i=0; i < POWER_TYPES && strncmp(power_types[i], zs, 3) ; i++);
         if(i < POWER_TYPES)
            printf("%s", power_types[i]+3);
         else if(row0[9][0])
            printf("Unrecognised");
         printf("\n");

         printf("              Timing Load: %s ", row0[14]);
         if(!row0[14][1]) switch(row0[14][0])
                         {
                         case 0:   break;
                         case 'A': printf("Class 14x"); break;
                         case 'E': printf("Class 158"); break;
                         case 'N': printf("Class 165/0"); break;
                         case 'S': printf("Class 150, 153, 155, or 156"); break;
                         case 'T': printf("Class 165/1 or 166"); break;
                         case 'V': printf("Class 220/1"); break;
                         case 'X': printf("Class 159"); break;
                         default:  printf("Unrecognised"); break;
                         }
         printf("\n");
         printf("                    Speed: ");
         if(row0[15][0])
            printf("%s mph", row0[15]);
         printf("\n");
         printf("Operating Characteristics: %s ", row0[16]);
         for(i=0; i<6 && row0[16][i]; i++)
         {
            if(i) printf(", ");
            switch(row0[16][i])
            {
            case 'B': printf("Vacuum Braked"); break;
            case 'C': printf("Timed at 100mph"); break;
            case 'D': printf("DOO Coaching Stock"); break;
            case 'E': printf("Mk4 Coaches"); break;
            case 'G': printf("Trainman (Guard) Required"); break;
            case 'M': printf("Timed at 110 mph"); break;
            case 'P': printf("Push/Pull Train"); break;
            case 'Q': printf("Runs as required"); break;
            case 'R': printf("Air conditioned with PA"); break;
            case 'S': printf("Steam heated"); break;
            case 'Y': printf("Runs to Terminals/Yards as required");  break;
            case 'Z': printf("SB1C gauge. Not to be diverted w/o authority"); break;
            default:  printf("Unrecognised"); break;
            }
         }
         printf("\n");

         printf("            Seating Class: %s ", row0[17]);
         switch(row0[17][0])
         {
         case 0:   break;
         case 'B': printf("First and Standard."); break;
         case ' ': printf("First and Standard."); break;
         case 'S': printf("Standard only."); break;
         default:  printf("Unrecognised"); break;
         }
         printf("\n");

         printf("                 Sleepers: %s ", row0[18]);
         switch(row0[18][0])
         {
         case 0:   break;
         case 'B': printf("First and Standard."); break;
         case 'F': printf("First only."); break;
         case 'S': printf("Standard only."); break;
         default:  printf("Unrecognised"); break;
         }
         printf("\n");

         printf("             Reservations: %s ", row0[19]);
         switch(row0[19][0])
         {
         case 0:
         case '0': break;
         case 'A': printf("Compulsory."); break;
         case 'E': printf("Bicycles essential."); break;
         case 'R': printf("Recommended."); break;
         case 'S': printf("Possible."); break;
         default:  printf("Unrecognised"); break;
         }
         printf("\n");
         printf("     Connection Indicator: %s\n", row0[20]);
         printf("            Catering Code: %s\n", row0[21]);
         printf("         Service Branding: %s\n", row0[22]);
         printf("             Train Status: %s ", row0[24]);
         switch(row0[24][0])
         {
         case 'B': printf("Bus");          break;
         case 'F': printf("Freight");      break;
         case 'P': printf("Passenger");    break;
         case 'T': printf("Trip");         break;
         case '1': printf("STP passenger");break;
         case '2': printf("STP freight");  break;
         case '3': printf("STP trip");     break;
         case '5': printf("STP bus");      break;
         default:  printf("Unrecognised"); break;
         }
         printf("\n");
         printf("\n");

         //                      0              1                2            3                4        5          6      7               8                9        10    11    12                       13                 14                   15  16
         sprintf(query, "SELECT location_type, record_identity, tiploc_code, tiploc_instance, arrival, departure, pass, public_arrival, public_departure, platform, line, path, engineering_allowance, pathing_allowance, performance_allowance, id, location_type FROM cif_schedule_locations WHERE cif_schedule_id = %ld ORDER BY next_day,sort_time", schedule_id);

         if(!db_query(query))
         {
            printf("                          Location   Arr     Dep     Plat    Line\n");
            result0 = db_store_result();
            while((row0 = mysql_fetch_row(result0)))
            {
               printf("%34s  %-6s  ", location_name(row0[2],false), show_time_text(row0[4]));
               // d or p
               if(row0[5][0] >= '0')
               {
                  printf("%-6s", show_time_text(row0[5]));
               }
               else
               {
                  char pd[8];
                  strcpy(pd, show_time_text(row0[6]));
                  pd[2] = '/';
                  printf("%-6s", pd);
               }
               
               printf("  %5s   %5s   ", row0[9], row0[10]);

               if(row0[12][0]) printf("Eng: %s ",  show_time_text(row0[12]));
               if(row0[13][0]) printf("Path: %s ", show_time_text(row0[13]));
               if(row0[14][0]) printf("Perf: %s ", show_time_text(row0[14]));
               printf("%s", show_act(row0[16]));

               printf("\n");
            }
         }
      }

      printf("</pre>\n");
   }
}

static char * location_name_link(const char * const tiploc, const word use_cache, const time_t when)
{
   // Not re-entrant
   // Set use_cache to false if hit is not expected, to bypass search.  Cache will still be updated.
   static char result[256];

   char * name = location_name(tiploc, use_cache);

   sprintf(result, "<a class=\"linkbutton\" href=\"%sdepsheet/%s/%s\">%s</a>", URL_BASE, tiploc, show_date(when, 0), name);

   return result;
}

static char * location_name(const char * const tiploc, const word use_cache)
{
   // Not re-entrant
   // Set use_cache to false if hit is not expected, to bypass search.  Cache will still be updated.
   char query[256];
   MYSQL_RES * result0;
   MYSQL_ROW row0;
   static word next_cache;
   word i;

   if(!tiploc)
   {
      // Initialise the cache
      for(i=0; i<CACHE_SIZE; i++)
      {
         cache_key[i][0] = '\0';
         cache_val[i][0] = '\0';
      }
      next_cache = CACHE_SIZE - 1;
      return NULL;
   }

   for(i=0; i<16 && use_cache; i++)
   {
      if(!strcasecmp(tiploc, cache_key[i]))
      {
         char zs[64];
         sprintf(zs, "Cache hit for \"%s\"", tiploc);
         _log(DEBUG, zs);
         return cache_val[i];
      }
   }

   next_cache = (next_cache + 1) % CACHE_SIZE;
   sprintf(query, "select fn from corpus where tiploc = '%s'", tiploc);
   db_query(query);
   result0 = db_store_result();
   if((row0 = mysql_fetch_row(result0)) && row0[0][0]) 
   {
      strncpy(cache_val[next_cache], row0[0], 127);
      cache_val[next_cache][127] = '\0';
   }
   else
   {
      strcpy(cache_val[next_cache], tiploc);
   }
   strcpy(cache_key[next_cache], tiploc);

   char zs[256];
   sprintf(zs, "Cache miss for \"%s\" - Returning \"%s\"", tiploc, cache_val[next_cache]);
   _log(DEBUG, zs);
   return cache_val[next_cache];
}

static char * show_stanox(const char * const stanox)
{
   static char result[256];
   char  query[256];
   MYSQL_RES * result0;
   MYSQL_ROW row0;

   sprintf(query, "SELECT fn FROM corpus WHERE stanox = '%s'", stanox);
   db_query(query);
   result0 = db_store_result();
   if((row0 = mysql_fetch_row(result0)) && row0[0][0]) 
   {
      strcpy(result, row0[0]);
   }
   else
   {
      strcpy(result, stanox);
   }

   return result;
}

static char * show_stanox_link(const char * const stanox)
{
   static char result[256];
   char  query[256];
   MYSQL_RES * result0;
   MYSQL_ROW row0;

   sprintf(query, "SELECT tiploc FROM corpus WHERE stanox = '%s'", stanox);
   db_query(query);
   result0 = db_store_result();
   if((row0 = mysql_fetch_row(result0)) && row0[0][0]) 
   {
      return location_name_link(row0[0], true, 0);
   }

   strcpy(result, stanox);
   return result;
}

static char * show_date(const time_t time, const byte local)
{
   static char result[32];
   if(time) 
   {
      strcpy(result, time_text(time, local));
      result[8] = '\0';
   }
   else result[0] = '\0';
   return result;
}

static char * show_act(const char * const input)
{
   static char output[128];

   if(input[0] == 'L' && (input[1] == 'I' || input[1] == 'O' || input[1] == 'T'))
      output[0] = '\0';
   else
      strcpy(output, input);

   return output;
}

static char * show_trust_time(const char * const s, const word local)
{
   // The input is a string containing the number of s since epoch

   static char result[32];

   time_t stamp = atol(s);

   if(!stamp)
   {
      result[0] = '\0';
      return result;
   }

   // Summer time compensation - Now performed before database entry
   // struct tm * broken = localtime(&stamp);
   // if(broken->tm_isdst) stamp -= 60*60;

   strcpy(result, time_text(stamp, local));

   // Lose the seconds
   result[14] = '\0';

   if((stamp%60) > 29) strcat(result, "&half;");

   // Lose the date
   return result + 9;
}

static char * show_trust_time_nocolon(const char * const s, const word local)
{
   // The input is a string containing the number of s since epoch

   static char result[32];
   char zs[32];
   time_t stamp = atol(s);

   if(!stamp)
   {
      result[0] = '\0';
      return result;
   }

   // Summer time compensation - Now performed before database entry
   // struct tm * broken = localtime(&stamp);
   // if(broken->tm_isdst) stamp -= 60*60;

   strcpy(zs, time_text(stamp, local));

   result[0] = zs[9];
   result[1] = zs[10];
   result[2] = zs[12];
   result[3] = zs[13];
   result[4] = '\0';

   if((stamp%60) > 29) strcat(result, "&half;");

   return result;
}

static char * show_expected_time(const char * const scheduled, const word deviation, const word late)
{
   static char result[32];
   word hour, minute;

   result[0] = scheduled[0];
   result[1] = scheduled[1];
   result[2] = '\0';
   hour = atoi(result);
   result[0] = scheduled[2];
   result[1] = scheduled[3];
   minute = atoi(result);
   minute += hour * 60;
   minute += 24*60;
   minute += late?(deviation):(-deviation);
   minute %= (24*60);
   hour = minute / 60;
   minute %= 60;
   sprintf(result, "%02d%02d", hour, minute);

   return result;
}
