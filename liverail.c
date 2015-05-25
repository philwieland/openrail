/*
    Copyright (C) 2013, 2014, 2015 Phil Wieland

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

static void depsheet(void);
static void display_choice(MYSQL_RES * result0, const char * const view, const time_t when);
static void display_control_panel(const char * const location, const time_t when);
static void report_train(const word index, const time_t when, const word huyton_special);
static void report_train_summary(const word index, const time_t when, const word ntrains);
static char * show_date(const time_t time, const byte local);
static void train(void);
static void train_text(void);
static char * location_name_and_codes(const char * const tiploc);
static char * location_name_link(const char * const tiploc, const word use_cache, const char * const view, const time_t when);
static char * location_name(const char * const tiploc, const word use_cache);
static char * show_stanox(const char * const stanox);
static char * show_stanox_link(const char * const stanox);
static char * show_act(const char * const input);
static char * show_trust_time(const char * const ms, const word local);
static char * show_trust_time_text(const char * const s, const word local);
static char * show_trust_time_nocolon(const char * const ms, const word local);
static char * show_expected_time(const char * const scheduled, const word deviation, const word late);
static void display_status(void);
static void display_status_panel(const word column);
static void mobilef(void);
static const char * const show_cape_reason(const char * const code);
static word get_sort_time(const char const * buffer);

#define NAME "Live Rail"
#define BUILD "W524"

#define COLUMNS_NORMAL 6
#define COLUMNS_PANEL 4
#define COLUMNS ((mode == PANEL || mode == PANELU)?COLUMNS_PANEL:COLUMNS_NORMAL)

#define URL_BASE "/rail/liverail/"

word debug, refresh;
static time_t now;
#define PARMS 10
#define PARMSIZE 128
static char parameters[PARMS][PARMSIZE];

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

// Message rate average over period (Seconds)
#define MESSAGE_RATE_PERIOD (5 * 60)

// location name cache
#define CACHE_SIZE 16
static char cache_key[CACHE_SIZE][8];
static char cache_val[CACHE_SIZE][128];

// User specified location TIPLOC.
char location[128];

// Display modes
enum modes        { FULL, SUMMARY, DEPART, PANEL, PANELU, SUMMARYU, DEPARTU, MOBILE, MOBILEF, TRAIN  , TRAINT, STATUS, MENUONLY, MODES} mode;
// 0x0001 HTML header and footer
// 0x0002 Update
// 0x0004 Sort using: 0 = sort_time database field. 1 = Prefer departure time
// 0x0008 Updateable - Supports smart updates.
// 0x0010 Huyton_special is HUYTON.
word modef[MODES] = {0x0001,0x000d,0x001d, 0x001d, 0x0016, 0x0006  , 0x0016 , 0x0014, 0x0000, 0x0001 , 0x0001, 0x0001, 0x0001};

static word mobile_trains, mobile_time;

#define MAX_CALLS 2048
static struct call_details
   {
      dword cif_schedule_id;
      word sort_time;
      byte next_day;
      byte valid;
      byte terminates;
      char cif_stp_indicator;
      char cif_train_uid[8];
      char arrival[6], public_arrival[6], departure[6], public_departure[6], pass[6];
      char platform[4];
      char tiploc_code[8];
   } 
   calls[MAX_CALLS];
static word call_sequence[MAX_CALLS];
static word call_count;

static dword schedules_key;

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
            parameters[j][k++] = parms[i++];
         l = j;
      }
   }
   if(k) parameters[j++][k] = '\0';

   while(j < PARMS) parameters[j++][0] = '\0';

   refresh = !strcasecmp(parameters[l], "r");

   if(load_config("/etc/openrail.conf"))
   {
      printf("Failed to load config.\n");
      exit(1);
   }

   debug = !strcasecmp(conf.debug, "true");

   // Set up log
   {
      struct tm * broken = localtime(&now);
      char logfile[128];

      sprintf(logfile, "/tmp/liverail-%04d-%02d-%02d.log", broken->tm_year + 1900, broken->tm_mon + 1, broken->tm_mday);
      _log_init(logfile, debug?2:3);
   }

   mode = MENUONLY;
   if(parms)
   {
      _log(GENERAL, "PARMS = \"%s\"", parms);
   }
   else
   {
      _log(GENERAL, "No PARMS provided!");
   }

   if(refresh) parameters[l][0] = '\0';

   if(!strcasecmp(parameters[0], "full")) mode = FULL;
   else if(!strcasecmp(parameters[0], "sum")) mode = SUMMARY;
   else if(!strcasecmp(parameters[0], "sumu")) mode = SUMMARYU;
   else if(!strcasecmp(parameters[0], "dep")) mode = DEPART;
   else if(!strcasecmp(parameters[0], "panel")) mode = PANEL;
   else if(!strcasecmp(parameters[0], "depu")) mode = DEPARTU;
   else if(!strcasecmp(parameters[0], "panelu")) mode = PANELU;
   else if(!strcasecmp(parameters[0], "mob")) mode = MOBILE;
   else if(!strcasecmp(parameters[0], "mobf")) mode = MOBILEF;
   else if(!strcasecmp(parameters[0], "train")) mode = TRAIN;
   else if(!strcasecmp(parameters[0], "train_text")) mode = TRAINT;
   else if(!strcasecmp(parameters[0], "status")) mode = STATUS;
   _log(DEBUG, "Display mode  = %d", mode);

   if(modef[mode] & 0x0001)
   {
      printf("Content-Type: text/html; charset=iso-8859-1\n\n");
      printf("<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\" \"http://www.w3.org/TR/html4/loose.dtd\">\n");
      printf("<html xmlns=\"http://www.w3.org/1999/xhtml\" lang=\"en\" xml:lang=\"en\">\n");
      printf("<head>\n");
      printf("<title>%s %s</title>\n", NAME, BUILD);
      printf("<link rel=\"stylesheet\" type=\"text/css\" href=\"/auxy/liverail.css\">\n");
      printf("</head>\n");
      printf("<body style=\"font-family: arial,sans-serif;\" onload=\"startup();\">\n");
      printf("<script type=\"text/javascript\" src=\"/auxy/liverail.js\"></script>\n");
   }
   else
   {
      printf("Content-Type: text/plain\nCache-Control: no-cache\n\n");
   }
   // Initialise location name cache
   location_name(NULL, false);

   // Initialise database
   db_init(conf.db_server, conf.db_user, conf.db_pass, conf.db_name);

   sprintf(zs, "Parameters:  (l = %d)", l);
   _log(GENERAL, zs);
   for(i=0;i < PARMS; i++)
   {
      sprintf(zs, "%d = \"%s\"", i, parameters[i]);
      _log(GENERAL, zs);
   }

   switch(mode)
   {
   case FULL:
   case SUMMARY:
   case SUMMARYU:
   case DEPART:
   case PANEL:
   case DEPARTU:
   case PANELU:
   case MOBILE:
      depsheet();
      break;

   case TRAIN:
      train();
      break;

   case TRAINT:
      train_text();
      break;

   case MENUONLY:
   case MODES:
      display_control_panel("", 0);
      break;

   case STATUS:
      display_control_panel("", 0);
      display_status();
      break;

   case MOBILEF:
      mobilef();
      break;
   }

   if(modef[mode] & 0x0003)
   {
      char host[256];
      if(gethostname(host, sizeof(host))) host[0] = '\0';
      gettimeofday(&ha_clock, NULL);
      qword elapsed = ha_clock.tv_sec;
      elapsed = elapsed * 1000 + ha_clock.tv_usec / 1000;
      elapsed -= start_time;
      if(modef[mode] & 0x0002)
      {
         printf("Updated at %s by %s %s at %s.  Elapsed time %s ms.\n", time_text(time(NULL), 1), NAME, BUILD, host, commas_q(elapsed));
      }
      else
      {
         if(mode == PANEL)
         printf("<p id=\"bottom-line\" style=\"display:none;\">Completed at %s by %s %s at %s.  Elapsed time %s ms.</p>\n", time_text(time(NULL), 1), NAME, BUILD, host, commas_q(elapsed));
         else
         printf("<p id=\"bottom-line\">Completed at %s by %s %s at %s.  Elapsed time %s ms.</p>\n", time_text(time(NULL), 1), NAME, BUILD, host, commas_q(elapsed));

         printf("</body></html>\n\n");
      }
   }
   exit(0);
}

static void display_choice(MYSQL_RES * result0, const char * const view, const time_t when)
{
   MYSQL_ROW row0;
   
   printf("<p>Select desired location</p>\n");

   printf("<table>");
   printf("<tr class=\"small-table\"><th>TIPLOC</th><th>Location</th></tr>\n");

   while((row0 = mysql_fetch_row(result0)) ) 
   {
      printf("<tr class=\"small-table\"><td>%s</td><td>%s</td></tr>\n", row0[0],  location_name_link(row0[0], false, view, when));
   }
   printf("</table>");
}

static void display_control_panel(const char * const location, const time_t when)
{
   printf("<table><tr><td class=\"control-panel-row\">\n");
   
   printf("&nbsp;Show trains at <input type=\"text\" id=\"search_loc\" size=\"24\" maxlength=\"64\" value=\"%s\" onkeydown=\"if(event.keyCode == 13) search_onclick(); else ar_off();\">\n", location);

   time_t x = now - (DAY_START * 15);
   struct tm * broken = localtime(&x);
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

   printf(" <button id=\"search\" class=\"cp-button\" onclick=\"search_onclick();\">Full</button>\n");
   printf(" <button id=\"search\" class=\"cp-button\" onclick=\"summary_onclick();\">Summary</button>\n");
   printf(" <button id=\"search\" class=\"cp-button\" onclick=\"depart_onclick();\">Departures</button>\n");

   printf("&nbsp;</td><td width=\"4%%\"></td><td class=\"control-panel-row\">&nbsp;&nbsp;<button id=\"as_rq\" class=\"cp-button\" onclick=\"as_rq_onclick();\">Advanced Search</button>");
   printf("&nbsp;&nbsp;<button id=\"status\" class=\"cp-button\" onclick=\"status_onclick();\">System Status</button>&nbsp;\n");

   printf("&nbsp;</td><td width=\"4%%\"></td><td class=\"control-panel-row\">&nbsp;Auto-refresh&nbsp;<input type=\"checkbox\" id=\"ar\" onclick=\"ar_onclick();\"%s>&nbsp;\n", refresh?" checked":"");
   
   //printf("</td><td id=\"progress\" class=\"control-panel-row\" style=\"display:none;\" width=\"1%%\" valign=\"top\">&nbsp;");
   printf("</td><td id=\"progress\" class=\"control-panel-row\" width=\"1%%\" valign=\"top\">&nbsp;");

   printf("</td></tr></table>\n");
}


static void depsheet(void)
{
   word cif_schedule_count;
   word huyton_special;

   MYSQL_RES * result0;
   MYSQL_ROW row0;
   char query[4096], zs[256];

   time_t when;

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
      // when is 12:00:00Z on the appropriate day
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
                  switch(mode)
                  {
                  case SUMMARY: display_choice(result0, "sum",  when); break;
                  case DEPART:  display_choice(result0, "dep",  when); break;
                  default:      display_choice(result0, "full", when); break;
                  }
                  mysql_free_result(result0);
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

   // Pre-report differences 
   if(modef[mode] & 0x0010)
   {
      if(huyton_special)
      {
         huyton_special = false;
         strcpy(location, "HUYTON");
      }
   }

   // Additional parameters for mobile
   if(mode == MOBILE)
   {
      // Time Minutes past midnight
      mobile_time   = atoi(parameters[5]) * 60 + atoi(parameters[6]);
      if(mobile_time < 240) 
      {
         mobile_time += (24*60);
         // Bit of a bodge!
         when -= (24*60*60);
         broken = *localtime(&when);
      }
      mobile_trains = atoi(parameters[7]);
   }

   switch(mode)
   {
   case FULL:
   case SUMMARY:
   case DEPART:
      display_control_panel(location, when);
      printf("<h2>");
      if(mode == DEPART) printf ("Passenger departures from ");
      else printf("Services at ");
      printf("%s on %s %02d/%02d/%02d</h2>", (huyton_special?"Huyton and Huyton Junction" : location_name_and_codes(location)), days[broken.tm_wday % 7], broken.tm_mday, broken.tm_mon + 1, broken.tm_year % 100);
      break;

   case PANEL:
      printf("<div style=\"display:none;\">");
      display_control_panel(location, when);
      printf("</div>");
      break;
   default:
      break;
   }
   
   //                    0                          1                      2                             3         4                5             6           7             8            9            10      11         12
   strcpy(query, "SELECT cif_schedules.id, cif_schedules.CIF_train_uid, cif_schedules.CIF_stp_indicator, next_day, sort_time, record_identity, arrival, public_arrival, departure, public_departure, pass, platform, tiploc_code");
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
   
   sprintf(zs, " AND deleted >= %ld AND created <= %ld", when + (12*60*60), when + (12*60*60));
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
   
   if(mode == DEPART || mode == DEPARTU || mode == MOBILE || mode == PANEL|| mode == PANELU)
   {
      // Passenger departures only
      sprintf(zs, " AND (public_departure != '' OR departure != '') AND (train_status = 'P' OR train_status = 'B' OR train_status = '1' OR train_status = '5')");
      strcat(query, zs);
      // And exclude ECS
      sprintf(zs, " AND (cif_train_category not like 'E%%')");
      strcat(query, zs);
   }
   
   strcat(query, " ORDER BY LOCATE(cif_schedules.CIF_stp_indicator, 'NPO'), cif_schedule_id"); 
   
   word index, i;
   
   // 1. Collect a list of visits
   _log(DEBUG, "Step 1:  Collect a list of visits.");
   call_count = 0;
   
   if(!db_query(query))
   {
      result0 = db_store_result();
      while((row0 = mysql_fetch_row(result0)))
      {
         if(call_count >= MAX_CALLS)
         {
            printf("<p>Error: MAX_CALLS exceeded.</p>");
            return;
         }

         // Insert in array
         calls[call_count].cif_schedule_id        = atol(row0[0]);
         calls[call_count].sort_time              = atoi(row0[4]);
         calls[call_count].next_day               = atoi(row0[3]);
         calls[call_count].valid                  = true;
         calls[call_count].terminates             = !(strcmp(row0[5], "LT"));;
         calls[call_count].cif_stp_indicator      = row0[2][0];
         strcpy(calls[call_count].cif_train_uid,    row0[1]);
         strcpy(calls[call_count].arrival,          row0[6]);
         strcpy(calls[call_count].public_arrival,   row0[7]);
         strcpy(calls[call_count].departure,        row0[8]);
         strcpy(calls[call_count].public_departure, row0[9]);
         strcpy(calls[call_count].pass,             row0[10]);
         strcpy(calls[call_count].platform,         row0[11]);
         strcpy(calls[call_count].tiploc_code,      row0[12]);
         // Sort time should prefer departure for summary and departure displays
         if(modef[mode] & 0x0004)
         {
            if(calls[call_count].public_departure[0]) calls[call_count].sort_time = get_sort_time(calls[call_count].public_departure);
            else if(calls[call_count].departure[0])   calls[call_count].sort_time = get_sort_time(calls[call_count].departure);
            else if(calls[call_count].pass[0])        calls[call_count].sort_time = get_sort_time(calls[call_count].pass);
         }
         call_count++;
      }
      mysql_free_result(result0);
   }
   
   // 2. Cancel any which are overriden
   for(index = 1; index < call_count; index++)
   {
      if(calls[index].valid && calls[index].cif_stp_indicator == 'O')
      {
         for(i = 0; i < index; i++)
         {
            if(!strcmp(calls[i].cif_train_uid, calls[index].cif_train_uid) && calls[i].cif_stp_indicator != 'O')
            {
               // Hit
               calls[i].valid = false;
               _log(DEBUG, "Call %d invalidated due to overlay call %d.  CIF UID \"%s\".", i, index, calls[index].cif_train_uid);
            }
         }
      }
   }
   
   // 3. Next, remove those which are cancelled, and remove those overriden by overlays that don't call
   // NOTE:  Overlay may not call at this station!
   _log(DEBUG, "3. Commencing C and O check.  day = %d", day);
   
   for(index = 0; index < call_count; index++)
   {
      if(calls[index].valid)
      {
         _log(DEBUG, "Testing index %d train \"%s\", sort_time = %d, next_day = %d", index, calls[index].cif_train_uid, calls[index].sort_time, calls[index].next_day );
         //                    0   1                 2                               3
         strcpy(query, "SELECT id, CIF_stp_indicator ");
         strcat(query, " FROM cif_schedules");
         strcat(query, " WHERE (cif_stp_indicator = 'C' OR cif_stp_indicator = 'O')");
         sprintf(zs, " AND (CIF_train_uid = '%s')", calls[index].cif_train_uid);
         strcat(query, zs);
         sprintf(zs, " AND (deleted >= %ld) AND (created <= %ld)", when + (12*60*60), when + (12*60*60));
         strcat(query, zs);

         if(calls[index].next_day && calls[index].sort_time >= DAY_START)
         {
            sprintf(zs, " AND ((%s) AND (schedule_start_date <= %ld) AND (schedule_end_date >= %ld))",  days_runs[yest], when - 12*60*60, when - 36*60*60);
            strcat(query, zs);
         }
         else if(calls[index].next_day && calls[index].sort_time < DAY_START)
         {
            sprintf(zs, " AND ((%s) AND (schedule_start_date <= %ld) AND (schedule_end_date >= %ld))", days_runs[day],  when + 12*60*60, when - 12*60*60);
            strcat(query, zs);
         }
         else if(!calls[index].next_day && calls[index].sort_time >= DAY_START)
         {
            sprintf(zs, " AND ((%s) AND (schedule_start_date <= %ld) AND (schedule_end_date >= %ld))",  days_runs[day],  when + 12*60*60, when - 12*60*60);
            strcat(query, zs);
         }
         else if(!calls[index].next_day && calls[index].sort_time < DAY_START)
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
                  calls[index].valid = false;
                  //printf("<br>Step 3:  %d invalidated due to C", index);
                  _log(DEBUG, "Schedule %ld (%s) cancelled by schedule %s.", calls[index].cif_schedule_id, calls[index].cif_train_uid, row0[0]);
               }
               else 
               {
                  // Overlay
                  // We will come here with an overlay we already know about OR one which *doesn't come here*
                  // In either case we invalidate this schedule.  If the overlay comes here it will already be in the list, somewhere.
                  _log(DEBUG, "Examining overlay.");
                  dword overlay_id = atol(row0[0]);
                  if(overlay_id != calls[index].cif_schedule_id || calls[index].cif_stp_indicator == 'N' || calls[index].cif_stp_indicator == 'P')
                  {
                     // Supercede
                     calls[index].valid = false;
                     _log(DEBUG, "Step 3:  %d invalidated due to O id = %s", index, row0[0]);
                  }
               }
            }
            mysql_free_result(result0);
         }
      }
   }

   // 4. In huyton_special mode some trains will be in then list at both Huyton and Junction.  In these cases,
   //    drop the junction entry.
   if(huyton_special)
   {
      _log(DEBUG, "4:  huyton_special - Remove trains which call at both.");
      for(index = 0; index < call_count; index++)
      {
         if(calls[index].valid)
         {
            for(i = 0; i < call_count; i++)
            {
               if(calls[i].valid && i != index
                  && calls[index].cif_schedule_id == calls[i].cif_schedule_id
                  && calls[index].sort_time > calls[i].sort_time - 16
                  && calls[index].sort_time < calls[i].sort_time + 16
                  && strcmp(calls[index].tiploc_code, calls[i].tiploc_code))
               {
                  // HIT
                  _log(DEBUG, "Found call %d and call %d for same pass of same train, schedule id %ld", index, i, calls[i].cif_schedule_id);
                  if(strcmp(calls[index].tiploc_code, "HUYTON"))
                  {
                     _log(DEBUG, "   Removing call %d", index);
                     calls[index].valid = false;
                  }
                  else
                  {
                     _log(DEBUG, "   Removing call %d", i);
                     calls[i].valid = false;
                  }
               }
            }
         }
      }
   }

   // 5. Bubble Sort
   {
      word i,j;

      word run = true;
      schedules_key = 0;

      // First, mung the sort_time so that early hours trains come after the others:
      // In the same loop, set up the sequence map, and calculate the schedules key.
      for(j=0; j< call_count; j++)
      {
         // Early hours trains come after all the others.
         if(calls[j].sort_time < DAY_START) calls[j].sort_time += 10000;
         call_sequence[j] = j;
         if(calls[j].valid) schedules_key += calls[j].cif_schedule_id; // May wrap.
      }

      for(j = call_count; run && j > 1; j--)
      {
         run = false;
         for(i=1; i < j; i++)
         {
            if(calls[call_sequence[i]].sort_time < calls[call_sequence[i-1]].sort_time)
            {
               run = true;
               // Swap
               word tempo = call_sequence[i];
               call_sequence[i] = call_sequence[i-1];
               call_sequence[i-1] = tempo;
            }
         }
      }
   }

   switch(mode)
   {
   case SUMMARY:
   case SUMMARYU:
   case DEPART:
   case PANEL:
   case DEPARTU:
   case PANELU:
   case MOBILE:
      cif_schedule_count = 0;
      for(index = 0; index < call_count; index++)
      {
         if(calls[index].valid)
         {
            cif_schedule_count++;
         }
      }
      report_train_summary(MAX_CALLS, when, cif_schedule_count);
      for(index = 0; index < call_count; index++)
      {
         if(calls[call_sequence[index]].valid)
         {
            report_train_summary(call_sequence[index], when, cif_schedule_count);
         }
      }

      // If no trains at all, tail won't get printed.
      if(!cif_schedule_count)
      {
         if(mode == SUMMARY || mode == DEPART || mode == PANEL) printf("</tr></table>\n");
      }
      //if(mode == SUMMARY || mode == DEPART|| mode == PANEL) printf("</table>\n");
      break;

   case FULL:
      printf("<table>\n");
      printf("<tr class=\"small-table\"><th>Detail</th><th>Type</th><th>ID</th><th>CIF ID</th><th>Latest Live Data</th><th>P</th><th>Times WTT(Public)</th><th>From</th><th>To</th></tr>\n");
      cif_schedule_count = 0;
      for(index = 0; index < call_count; index++)
      {
         // printf("%s<br>\n", row0[0]);
         if(calls[call_sequence[index]].valid)
         {
            cif_schedule_count++;
            report_train(call_sequence[index], when, huyton_special);
         }
      }
      
      printf("</table>\n");
      break;

   default:
      break;
   }
}

static void report_train(const word index, const time_t when, const word huyton_special)
{
   MYSQL_RES * result0, * result1;
   MYSQL_ROW row0, row1;

   char query[1024];
   word vstp;

   // Calculate start_date from when, deducting 24h if next_day and/or adding 24h if after 00:00.
   time_t start_date = when - (calls[index].next_day?(24*60*60):0);
   // N.B. sort_time has been munged by now so that ones before DAY_START have had 10000 added
   if(calls[index].sort_time >= 10000) start_date += (24*60*60);

   _log(DEBUG, "calls[index].sort_time = %d, DAY_START = %d, start_date = %s", calls[index].sort_time, DAY_START, show_date(start_date, false));
   //                     0             1                    2                  3              4              5                  6           
   sprintf(query, "SELECT train_status, schedule_start_date, schedule_end_date, signalling_id, CIF_train_uid, CIF_stp_indicator, update_id FROM cif_schedules WHERE id = %ld", calls[index].cif_schedule_id);

   if(!db_query(query))
   {
      result0 = db_store_result();

      if((row0 = mysql_fetch_row(result0)))
      {
         printf("<tr class=\"small-table\">");
         vstp = (row0[6][0] == '0' && row0[6][1] == 0);

         // Link
         printf("<td><a class=\"linkbutton\" href=\"%strain/%ld/%s\">Details</a></td>\n", URL_BASE, calls[index].cif_schedule_id, show_date(start_date, false));
         
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
            printf("%ld", calls[index].cif_schedule_id);
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
            word deduced = false;
            MYSQL_RES * result2;
            MYSQL_ROW row2;
            report[0] = class[0] = '\0';
            struct tm * broken = gmtime(&start_date);
            byte dom = broken->tm_mday; 

            // Then, only accept activations where dom matches, and are +- 15 days (To eliminate last month's activation.)  YUK
            sprintf(query, "SELECT created, trust_id, deduced FROM trust_activation WHERE cif_schedule_id = %ld AND substring(trust_id FROM 9) = '%02d' AND created > %ld AND created < %ld order by deduced", calls[index].cif_schedule_id, dom, when - 15*24*60*60, when + 15*24*60*60);
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
                  sprintf(query, "SELECT created, reason, type, reinstate from trust_cancellation where trust_id='%s' AND created > %ld AND created < %ld order by created ", row1[1], when - 15*24*60*60, when + 15*24*60*60);
                  if(!db_query(query))
                  {
                     result2 = db_store_result();
                     while((row2 = mysql_fetch_row(result2)))
                     {
                        if(atoi(row2[3]) == 1)
                        {
                           sprintf(report, "%s Reinstated", time_text(atol(row2[0]), true));
                           strcpy(class, "small-table-act");
                        }
                        else
                        {
                           sprintf(report, "%s Cancelled %s %s", time_text(atol(row2[0]), true), show_cape_reason(row2[1]), row2[2]);
                           strcpy(class, "small-table-cape");
                        }
                     }
                     mysql_free_result(result2);
                  }
                  sprintf(query, "SELECT created, event_type, planned_event_type, platform, loc_stanox, actual_timestamp, timetable_variation, variation_status from trust_movement where trust_id='%s' AND created > %ld AND created < %ld order by actual_timestamp desc, planned_timestamp desc, created desc limit 1", row1[1], when - 15*24*60*60, when + 15*24*60*60);
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
         printf("<td>%s</td><td>\n", calls[index].platform); 
         if(huyton_special && !strcmp(calls[index].tiploc_code, "HUYTJUN")) printf("Junction: ");
         if(calls[index].arrival[0])          printf("a: %s", show_time(calls[index].arrival)); // arrival
         if(calls[index].public_arrival[0])   printf("(%s)",  show_time(calls[index].public_arrival)); // public arrival
         if(calls[index].departure[0])        printf(" d: %s",show_time(calls[index].departure)); // dep
         if(calls[index].public_departure[0]) printf("(%s)",  show_time(calls[index].public_departure)); // public dep
         if(calls[index].pass[0])             printf("p: %s", show_time(calls[index].pass)); // pass
         printf("</td>");
                        
         // From
         sprintf(query, "SELECT tiploc_code, departure, public_departure FROM cif_schedule_locations where cif_schedule_id = %ld AND record_identity = 'LO'", calls[index].cif_schedule_id);
         printf("<td>");
         if(!db_query(query))
         {
            result1 = db_store_result();
            
            if((row1 = mysql_fetch_row(result1)))
            {
               printf("%s %s", location_name_link(row1[0], true, "full", when), show_time(row1[1]));
               if(row1[2][0]) printf("(%s)", show_time(row1[2]));
            }
            mysql_free_result(result1);
         }
         printf("</td>");

         // To
         sprintf(query, "SELECT tiploc_code, arrival, public_arrival FROM cif_schedule_locations where cif_schedule_id = %ld AND record_identity = 'LT'", calls[index].cif_schedule_id);
         printf("<td>");
         if(!db_query(query))
         {
            result1 = db_store_result();
            
            if((row1 = mysql_fetch_row(result1)))
            {
               printf("%s %s", location_name_link(row1[0], true, "full", when), show_time(row1[1]));
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

static void report_train_summary(const word index, const time_t when, const word ntrains)
// Despite its name, also used for SUMMARYU, DEPART, DEPARTU, PANEL, PANELU and MOBILE modes.
{
   enum statuses {NoReport, Activated, Moving, Cancelled, Arrived, Departed};
   MYSQL_RES * result0, * result1;
   MYSQL_ROW row0, row1;

   char query[1024];
   static word trains, rows, train, row, bus, shown;
   static word nlate, ncape, nbus, ndeduced, narrival;
   word status, mobile_sched, mobile_sched_unmung, mobile_act;
   char actual[16];
   word deviation, deduced, late;
   char train_details[256], train_time[16], destination[128], analysis_text[32], analysis_class[32], mobile_analysis[32];
   struct tm * broken;

   // Calculate start_date from when, deducting 24h if next_day and/or adding 24h if after 00:00.
   time_t start_date = when - (calls[index].next_day?(24*60*60):0);
   if(calls[index].sort_time >= 10000) start_date += (24*60*60);

   deviation = late = status = bus = deduced = 0;

   _log(PROC, "report_train_summary(%d, %ld, %d)", index, when, ntrains);

   // Initialise
   if(index >= MAX_CALLS)
   {
      trains = ntrains;
      rows = (trains + COLUMNS - 1) / COLUMNS;
      if(rows < 20) rows = 20;
      if(rows < 32 && (mode == PANEL || mode == PANELU)) rows = 32;
      train = row = 0;
      nlate = ncape = nbus = ndeduced = narrival = shown = 0;
      broken = localtime(&when);
      if(modef[mode] & 0x0002) // Update
      {
         printf("%02d%s%ld\n", broken->tm_mday, BUILD, schedules_key);
      }
      else if(modef[mode] & 0x0008) // Supports smart updates.
      {
         printf("\n<input type=\"hidden\" id=\"display_handle\" value=\"%02d%s%ld\">", broken->tm_mday, BUILD, schedules_key);
         printf("<table><tr>"); // Start of outer table
      }
      return;
   }

   // Before print
   if(row && (!(row % rows)))
   {
      if(mode == SUMMARY || mode == DEPART || mode == PANEL) printf("</table></td>\n"); // end of inner table, end of outer td
   }

   if((!(row % rows)) && row < trains)
   {
      if(mode == SUMMARY || mode == DEPART || mode == PANEL) printf("<td><table class=\"summ-table\"><tr class=\"summ-table-head\"><th colspan=2>&nbsp;</th><th>Report</th></tr>\n"); // start of outer td, start of inner table
   }
 
   //                     0             1                    2                  3              4              5                  6           
   sprintf(query, "SELECT train_status, schedule_start_date, schedule_end_date, signalling_id, CIF_train_uid, CIF_stp_indicator, update_id FROM cif_schedules WHERE id = %ld", calls[index].cif_schedule_id);

   if(!db_query(query))
   {
      result0 = db_store_result();

      if((row0 = mysql_fetch_row(result0)))
      {
         // vstp = (row0[6][0] == '0' && row0[6][1] == 0);
         bus = (row0[0][0] == 'B' || row0[0][0] == '5');

         // To
         sprintf(query, "SELECT tiploc_code FROM cif_schedule_locations where cif_schedule_id = %ld AND record_identity = 'LT'", calls[index].cif_schedule_id);
         if(!db_query(query))
         {
            result1 = db_store_result();
            
            if((row1 = mysql_fetch_row(result1)))
            {
               strcpy(destination, location_name(row1[0], true));
            }
            mysql_free_result(result1);
         }

         if(calls[index].terminates)
         {
            // From
            sprintf(query, "SELECT tiploc_code FROM cif_schedule_locations where cif_schedule_id = %ld AND record_identity = 'LO'", calls[index].cif_schedule_id);
            if(!db_query(query))
            {
               result1 = db_store_result();
            
               if((row1 = mysql_fetch_row(result1)))
               {
                  strcpy(destination, "From ");
                  strcat(destination, location_name(row1[0], true));
               }
               mysql_free_result(result1);
            }
         }

         _log(DEBUG, "Got destination = \"%s\"", destination);

         switch(mode)
         {
         case DEPART:
         case DEPARTU:
            if(calls[index].public_departure[0])
               strcpy(train_time, calls[index].public_departure);
            else
               strcpy(train_time, calls[index].departure);
            // Link and destination
            sprintf(train_details, "<a class=\"linkbutton-summary\" href=\"%strain/%ld/%s\">%s</a>", URL_BASE, calls[index].cif_schedule_id, show_date(start_date, false), destination);
            break;

         case PANEL:
         case PANELU:
            if(calls[index].public_departure[0])
               strcpy(train_time, calls[index].public_departure);
            else
               strcpy(train_time, calls[index].departure);
            // NO LINK, destination
            sprintf(train_details, "%s", destination);
            break;

         case SUMMARY:
         case SUMMARYU:
            if(calls[index].departure[0]) strcpy(train_time, calls[index].departure);
            else if(calls[index].arrival[0]) strcpy(train_time, calls[index].arrival);
            else if(calls[index].pass[0]) strcpy(train_time, calls[index].pass);
            else strcpy(train_time, "?????");
            if(train_time[4] == 'H') strcpy(train_time + 4, "&half;");
            // Link and time and destination
            sprintf(train_details, "<a class=\"linkbutton-summary\" href=\"%strain/%ld/%s\">%s</a>", URL_BASE, calls[index].cif_schedule_id, show_date(start_date, false), destination);
            break;

         default:
            sprintf(train_details, "XXXXX");
            if(calls[index].departure[0]) strcpy(train_time, calls[index].departure);
            else if(calls[index].arrival[0]) strcpy(train_time, calls[index].arrival);
            else if(calls[index].pass[0]) strcpy(train_time, calls[index].pass);
            else strcpy(train_time, "?????");
            if(train_time[4] == 'H') strcpy(train_time + 4, "&half;");
            break;
         }

         _log(DEBUG, "Got train time = \"%s\", train details = \"%s\"", train_time, train_details);

         status = NoReport;
         // TRUST
         // if(!bus)
         {
            char query[512], trust_id[16];
            MYSQL_RES * result2;
            MYSQL_ROW row2;
            broken = gmtime(&start_date);
            byte dom = broken->tm_mday;

            // Only accept activations where dom matches, and are +- 15 days (To eliminate last month's activation.)  YUK
            // ORDER BY created DESC means we get the last created one
            sprintf(query, "SELECT created, trust_id, deduced FROM trust_activation WHERE cif_schedule_id = %ld AND substring(trust_id FROM 9) = '%02d' AND created > %ld AND created < %ld ORDER BY created DESC", calls[index].cif_schedule_id, dom, when - 15*24*60*60, when + 15*24*60*60);
            if(!db_query(query))
            {
               result1 = db_store_result();
               if((row1 = mysql_fetch_row(result1)))
               {
                  status = Activated;
                  strcpy(trust_id, row1[1]);
                  deduced = (row1[2][0] != '0');
               }
               mysql_free_result(result1);
            }

            if(status)
            {
               // Look for cancellation
               sprintf(query, "SELECT created, reason, type, reinstate from trust_cancellation where trust_id='%s' AND created > %ld AND created < %ld order by created ", trust_id, when - 15*24*60*60, when + 15*24*60*60);                  
               if(!db_query(query))
               {                   
                  result2 = db_store_result();
                  while((row2 = mysql_fetch_row(result2)))
                  {
                     if(atoi(row2[3]))
                     {
                        status = Activated; // Reinstate.  Back to Activated.
                     }
                     else
                     {
                        status = Cancelled; // Cancelled
                     }
                  }
                  mysql_free_result(result2);
               }
            }

            if(status)
            {
               // Look for movements.
               sprintf(query, "SELECT event_type, loc_stanox, actual_timestamp, timetable_variation, variation_status, planned_timestamp from trust_movement where trust_id='%s' AND created > %ld AND created < %ld order by actual_timestamp, planned_timestamp, created", trust_id, when - 15*24*60*60, when + 15*24*60*60);
               if(!db_query(query))
               {
                  result1 = db_store_result();
                  while((row1 = mysql_fetch_row(result1)))
                  {
                     if(status == Activated || status == Moving)
                     {
                        status = Moving;
                        strcpy(actual, row1[2]);
                        deviation = atoi(row1[3]);
                        late = !strcasecmp("late", row1[4]);
                     }
                     if(status < Departed)
                     {
                        sprintf(query, "SELECT tiploc FROM corpus WHERE stanox = %s", row1[1]);
                        if(!db_query(query))
                        {
                           result2 = db_store_result();
                           if((row2 = mysql_fetch_row(result2)))
                           {
                              _log(DEBUG, "Looking for TIPLOC \"%s\" found movement at TIPLOC \"%s\".", calls[index].tiploc_code, row2[0]);
                              if(!strcasecmp(calls[index].tiploc_code, row2[0]))
                              {
                                 if(!strcasecmp("departure", row1[0]))
                                 {
                                    _log(DEBUG, "Hit - Departure.");
                                    // Got a departure report at our station
                                    // Check if it is about the right time, in case train calls twice.
                                    {
                                       char z[8];
                                       z[0] = train_time[0]; z[1] = train_time[1]; z[2] = '\0';
                                       word sched = atoi(z)*60;
                                       z[0] = train_time[2]; z[1] = train_time[3];
                                       sched += atoi(z);
                                       time_t planned_timestamp = atol(row1[5]);
                                       struct tm * broken = localtime(&planned_timestamp);
                                       word planned = broken->tm_hour * 60 + broken->tm_min;
                                       if(planned > sched - 8 && planned < sched + 8) // This might fail close to midnight!
                                       {
                                          // Near enough!
                                          status = Departed;
                                          strcpy(actual, row1[2]);
                                          deviation = atoi(row1[3]);
                                          late = !strcasecmp("late", row1[4]);
                                       }
                                    }
                                 }
                                 else if(status < Arrived)
                                 {
                                    _log(DEBUG, "Hit - Arrival.");
                                    // Got an arrival from our station AND haven't seen a departure yet
                                    char z[8];
                                    z[0] = train_time[0]; z[1] = train_time[1]; z[2] = '\0';
                                    word sched = atoi(z)*60;
                                    z[0] = train_time[2]; z[1] = train_time[3];
                                    sched += atoi(z);
                                    time_t planned_timestamp = atol(row1[5]);
                                    struct tm * broken = localtime(&planned_timestamp);
                                    word planned = broken->tm_hour * 60 + broken->tm_min;
                                    if(planned > sched - 8 && planned < sched + 8) // This might fail close to midnight!
                                    {
                                       // Near enough!
                                       status = Arrived;
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
         }
      }
      mysql_free_result(result0);
   }

   // Build analysis
   mobile_sched = atoi(calls[index].public_departure);
   if(!mobile_sched) mobile_sched = atoi(calls[index].public_arrival);
   if(!mobile_sched) mobile_sched = atoi(calls[index].departure);
   if(!mobile_sched) mobile_sched = atoi(calls[index].arrival);
   if(!mobile_sched) mobile_sched = atoi(calls[index].pass);
   mobile_sched_unmung = mobile_sched;
   // Convert to minutes past midnight
   mobile_sched = (mobile_sched / 100) * 60 + (mobile_sched % 100);
   mobile_act = mobile_sched;
   char row_class[32];
   switch(status)
   {
   case NoReport: 
      if(bus) 
      {
         strcpy(analysis_text, "(Bus)");
         strcpy(analysis_class, "");
         strcpy(mobile_analysis, "Bus");
         nbus++;
      }
      else 
      {
         strcpy(analysis_text, "&nbsp"); 
         strcpy(analysis_class, "");
         strcpy(mobile_analysis, "");
      }
      strcpy(row_class, "summ-table-idle");
      break;

   case Activated:
      strcpy(analysis_text, "Activated");
      strcpy(analysis_class, "");
      strcpy(mobile_analysis, "Act");
      strcpy(row_class, "summ-table-act");
      break;

   case Moving:
      strcpy(row_class, "summ-table-move");
      if(!deviation )        { sprintf(analysis_text, "Cur. On time"); strcpy(analysis_class, "summ-table-good"); }
      else if(deviation < 3) { sprintf(analysis_text, "Cur. %s %d%s",  show_expected_time(train_time, deviation, late), deviation, late?"L":"E"); strcpy(analysis_class, "summ-table-good"); }
      else if(deviation < 6) { sprintf(analysis_text, "Cur. %s %d%s", show_expected_time(train_time, deviation, late), deviation, late?"L":"E"); strcpy(analysis_class, "summ-table-minor"); }
      else                   { sprintf(analysis_text, "Cur. %s %d%s", show_expected_time(train_time, deviation, late), deviation, late?"L":"E"); strcpy(analysis_class, "summ-table-major"); }
      sprintf(mobile_analysis, "%d%s", deviation, late?"L":"E");
      if(deviation >= 3) nlate++;
      mobile_act = mobile_sched + (late?deviation:(-deviation));
      break;

   case Cancelled:
      strcpy(row_class, "summ-table-cape");
      sprintf(analysis_text, "Cancelled");
      strcpy(analysis_class, "small-table-crit");
      strcpy(mobile_analysis, "Can");
      ncape++;
      break;

   case Arrived:
   case Departed:
      strcpy(row_class, "summ-table-gone");
      if(!deviation )        { sprintf(analysis_text, "On time"); strcpy(analysis_class, "summ-table-good"); }
      else if(deviation < 3) { sprintf(analysis_text, "%s %d%s",  show_trust_time_nocolon(actual, true), deviation, late?"L":"E"); strcpy(analysis_class, "summ-table-good"); }
      else if(deviation < 6) { sprintf(analysis_text, "%s %d%s", show_trust_time_nocolon(actual, true), deviation, late?"L":"E"); strcpy(analysis_class, "summ-table-minor"); }
      else                   { sprintf(analysis_text, "%s %d%s", show_trust_time_nocolon(actual, true), deviation, late?"L":"E"); strcpy(analysis_class, "summ-table-major"); }
      sprintf(mobile_analysis, "%d%s", deviation, late?"L":"E");
      if(deviation >= 3) nlate++;
      mobile_act = mobile_sched + (late?deviation:(-deviation));
      break;

   }

   if(deduced) ndeduced++;
   if(status == Arrived) narrival++;

   // Mung mobile times
   if(mobile_sched < 240) mobile_sched += (24*60);
   if(mobile_act   < 240) mobile_act   += (24*60);

   _log(DEBUG, "Mobile times: sched %d, act %d, requested time = %d", mobile_sched, mobile_act, mobile_time);

   // Print it
   switch(mode)
   {
   case SUMMARYU:
   case DEPARTU:
   case PANELU:
      //printf("tr%d%d|%s|<td>%s</td><td>%s</td>%s", row/rows, row%rows, row_class, train_time, train_details, analysis );
      printf("tr%d%d|%s|%s|%s", row/rows, row%rows, row_class, analysis_class, analysis_text);
      if(deduced || (status == Arrived && !calls[index].terminates))
      {
         printf("&nbsp;<span class=\"symbols\">");
         if(deduced) printf("&loz;");
         if(status == Arrived && !calls[index].terminates) printf("&alpha;");
         printf("</span>");
      }
      printf("\n");
      break;

   case SUMMARY:
   case DEPART:
   case PANEL:
      printf("<tr id=\"tr%d%d\" class=\"%s\"><td>%s</td><td>%s</td><td id=\"tr%d%dr\" class=\"%s\">%s", row/rows, row%rows, row_class, train_time, train_details, row/rows, row%rows, analysis_class, analysis_text);
      
      // Symbols
      if(deduced || (status == Arrived && !calls[index].terminates))
      {
         printf("&nbsp;<span class=\"symbols\">");
         if(deduced) printf("&loz;");
         if(status == Arrived && !calls[index].terminates) printf("&alpha;");
         printf("</span>");
      }
      printf("</td></tr>\n");
      break;

   case MOBILE:
      if(shown < mobile_trains)
      {
         if(mobile_sched > mobile_time || mobile_act > mobile_time)
         {
            printf("%04d %s|%d|%s|%ld\n", mobile_sched_unmung, destination, status, mobile_analysis, calls[index].cif_schedule_id);
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
         if(mode == SUMMARY || mode == DEPART || mode == PANEL)
         {
            printf("<tr class=\"summ-table-idle\"><td>&nbsp;</td><td>&nbsp;</td><td>&nbsp;</td></tr>\n");
         }
      }
      if(mode == SUMMARY || mode == DEPART || mode == PANEL) printf("</table></td><td>");
      
      // Last column of outer table - Key
      if(modef[mode] & 0x0002) // Update
      {
         printf("tr%d9|summ-table-idle||%d trains.\n", COLUMNS, trains - nbus);
         printf("tr%d10|summ-table-idle|%s|%d not on time.\n", COLUMNS, nlate?"summ-table-minor":"", nlate);
         printf("tr%d11|summ-table-idle|%s|%d cancelled.\n", COLUMNS, ncape?"summ-table-cape":"", ncape);
         printf("tr%d12|summ-table-idle||%d buses.\n", COLUMNS, nbus);
         printf("tr%d13|summ-table-idle||%d activation deduced.\n", COLUMNS, ndeduced);
         printf("tr%d14|summ-table-idle||%d departure not reported.\n", COLUMNS, narrival);
         if(mode == PANELU)
         {
            printf("tr%d19|summ-table-idle||%s\n", COLUMNS, time_text(time(NULL), 1));
            display_status_panel(COLUMNS);
         }
      }
      else if(modef[mode] & 0x0008) // Updateable
      {
         printf("<table class=\"summ-table\">");
         printf("<tr class=\"summ-table-head\"><th>Key</th></tr>\n");
         printf("<tr class=\"summ-table-move\"><td>Train moving.</td></tr>");
         printf("<tr class=\"summ-table-act\"><td>Train activated.</td></tr>");
         printf("<tr class=\"summ-table-idle\"><td>&nbsp;</td></tr>");
         printf("<tr class=\"summ-table-idle\"><td>Deduced activation.&nbsp;&nbsp;<span class=\"symbols\">&loz;</span></td></tr>");
         printf("<tr class=\"summ-table-idle\"><td>Departure not reported.&nbsp;&nbsp;<span class=\"symbols\">&alpha;</span></td></tr>");
         printf("<tr class=\"summ-table-idle\"><td>&nbsp;</td></tr>");
         printf("<tr class=\"summ-table-idle\"><td>&nbsp;</td></tr>");
         printf("<tr class=\"summ-table-head\"><th>Statistics</th></tr>\n");
         printf("<tr id=\"tr%d9\" class=\"summ-table-idle\"><td id=\"tr%d9r\">%d trains.</td></tr>", COLUMNS, COLUMNS, trains - nbus);
         printf("<tr id=\"tr%d10\" class=\"summ-table-idle\"><td id=\"tr%d10r\"%s>%d not on time.</td></tr>", COLUMNS, COLUMNS, nlate?" class=\"summ-table-minor\"":"", nlate);
         printf("<tr id=\"tr%d11\" class=\"summ-table-idle\"><td id=\"tr%d11r\"%s>%d cancelled.</td></tr>", COLUMNS, COLUMNS, ncape?" class=\"summ-table-cape\"":"", ncape);
         printf("<tr id=\"tr%d12\" class=\"summ-table-idle\"><td id=\"tr%d12r\">%d buses.</td></tr>", COLUMNS, COLUMNS, nbus);
         printf("<tr id=\"tr%d13\" class=\"summ-table-idle\"><td id=\"tr%d13r\">%d activation deduced.</td></tr>", COLUMNS, COLUMNS, ndeduced);
         printf("<tr id=\"tr%d14\" class=\"summ-table-idle\"><td id=\"tr%d14r\">%d departure not reported.</td></tr>", COLUMNS, COLUMNS, narrival);
         if(mode != PANEL)
         {
            row = 14;
         }
         else
         {
            printf("<tr class=\"summ-table-idle\"><td>&nbsp;</td></tr>"); //15
            printf("<tr class=\"summ-table-idle\"><td>&nbsp;</td></tr>"); //16
            printf("<tr class=\"summ-table-head\"><th>System Status</th></tr>\n"); // 17
            printf("<tr class=\"summ-table-idle\"><td>Display updated:</td></tr>"); // 18
            printf("<tr id=\"tr%d19\" class=\"summ-table-idle\"><td id=\"tr%d19r\">&nbsp;</td></tr>", COLUMNS, COLUMNS); // 19
            printf("<tr class=\"summ-table-idle\"><td>Timetable updated:</td></tr>"); // 20
            printf("<tr id=\"tr%d21\" class=\"summ-table-idle\"><td id=\"tr%d21r\">&nbsp;</td></tr>", COLUMNS, COLUMNS); // 21
            printf("<tr class=\"summ-table-idle\"><td>VSTP updated:</td></tr>"); // 22
            printf("<tr id=\"tr%d23\" class=\"summ-table-idle\"><td id=\"tr%d23r\">&nbsp;</td></tr>", COLUMNS, COLUMNS); // 23
            printf("<tr class=\"summ-table-idle\"><td>TRUST updated:</td></tr>"); // 24
            printf("<tr id=\"tr%d25\" class=\"summ-table-idle\"><td id=\"tr%d25r\">&nbsp;</td></tr>", COLUMNS, COLUMNS); // 25
            printf("<tr id=\"tr%d26\" class=\"summ-table-idle\"><td id=\"tr%d26r\">&nbsp;</td></tr>", COLUMNS, COLUMNS); // 26
            printf("<tr class=\"summ-table-idle\"><td>TD updated:</td></tr>"); // 27
            printf("<tr id=\"tr%d28\" class=\"summ-table-idle\"><td id=\"tr%d28r\">&nbsp;</td></tr>", COLUMNS, COLUMNS); // 28
            printf("<tr id=\"tr%d29\" class=\"summ-table-idle\"><td id=\"tr%d29r\">&nbsp;</td></tr>", COLUMNS, COLUMNS); // 29
            printf("<tr class=\"summ-table-idle\"><td>Disc free space:</td></tr>"); // 30
            printf("<tr id=\"tr%d31\" class=\"summ-table-idle\"><td id=\"tr%d31r\">&nbsp;</td></tr>", COLUMNS, COLUMNS); // 31

            row = 31;
         }

         while((row++) % rows)
         {
            printf("<tr class=\"summ-table-idle\" id=\"tr%d%d\"><td>&nbsp;</td></tr>\n", COLUMNS, row);
         }
         printf("</table>");// End of inner table
            
         printf("</td></tr></table>\n"); // End of last column, end of outer table
      }
   }
   return;
}

static void train(void)
{
   char query[2048], zs[128];
   MYSQL_RES * result0, * result1;
   MYSQL_ROW row0, row1;
   char train[1024];

   // Decode parameters
   dword schedule_id = atol(parameters[1]);
   if(!schedule_id) 
   {
      display_control_panel("", 0);
      return;
   }

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
   // In here, when = 0 if no date supplied, NOT today

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
   //              25       26                                           31       32       33          34               35
   strcat(query, "runs_mo, runs_tu, runs_we, runs_th, runs_fr, runs_sa, runs_su, created, deleted, deduced_headcode, deduced_headcode_status");
   sprintf(zs, " FROM cif_schedules WHERE id=%ld", schedule_id);
   strcat(query, zs);

   if(!db_query(query))
   {
      result0 = db_store_result();
      if(!(row0 = mysql_fetch_row(result0)))
      {
         printf("<h3>Schedule %ld not found.</h3>\n", schedule_id);
         mysql_free_result(result0);
         return;
      }
      else
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
         printf("<tr bgcolor=\"#eeeeee\"><td>Deduced headcode</td><td>%s</td><td>", row0[34]);
         if(row0[35][0]) printf("(Status %s)", row0[35]);
         printf("</td></tr>");
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


         // printf("</td><td width=\"5%%\">&nbsp;</td><td style=\"vertical-align:top;\">\n"); // Outer table

         //                      0              1                2            3                4        5          6      7               8                9        10    11    12                       13                 14                   15 
         sprintf(query, "SELECT location_type, record_identity, tiploc_code, tiploc_instance, arrival, departure, pass, public_arrival, public_departure, platform, line, path, engineering_allowance, pathing_allowance, performance_allowance, location_type FROM cif_schedule_locations WHERE cif_schedule_id = %ld ORDER BY next_day,sort_time", schedule_id);

         if(!db_query(query))
         {
            printf("<table>");
            printf("<tr class=\"small-table\"><th>Location</th><th>Times WTT(Public)</th><th>Plat</th><th>Line</th><th>Allowances</th><th>&nbsp;</th>\n");
            result0 = db_store_result();
            while((row0 = mysql_fetch_row(result0)))
            {
               printf("<tr class=\"small-table\">\n");
               
               printf("<td>%s</td>", location_name_link(row0[2], false, "full", when));
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
               printf("<td>%s</td>\n", show_act(row0[15]));

               printf("</tr>\n");
            }
            printf("</table>\n");
         }
      }
      mysql_free_result(result0);

      {
         struct tm * broken;
         broken = localtime(&when);
         printf("<a class=\"linkbutton-cp\" href=\"%strain_text/%ld/%02d/%02d/%02d\" target=\"_blank\">Display in plain text</a>\n", URL_BASE, schedule_id, broken->tm_mday, broken->tm_mon + 1, broken->tm_year%100);
      }
      printf("</td><td width=\"5%%\">&nbsp;</td><td style=\"vertical-align:top;\">\n"); // Outer table

      // TRUST
      if(when)
      {
         struct tm * broken = gmtime(&when);
         printf("<table><tr>\n");
         printf("<td>Real Time Data For %s %02d/%02d/%02d</td>\n", days[broken->tm_wday % 7], broken->tm_mday, broken->tm_mon + 1, broken->tm_year % 100);
         printf("<td width = \"10%%\">&nbsp;</td>\n");
         printf("<td class=\"control-panel-row\"> Show real time data for date ");
         printf("<input type=\"text\" id=\"train_date\" size=\"8\" maxlength=\"8\" value=\"\" onkeydown=\"if(event.keyCode == 13) train_date_onclick(%ld); else ar_off();\">\n", schedule_id);
         printf(" <button id=\"search\" class=\"cp-button\" onclick=\"train_date_onclick(%ld);\">Show</button> </td>\n", schedule_id);
         printf("</tr></table>\n");
         printf("<table>");
         printf("<tr class=\"small-table\"><th>&nbsp;&nbsp;Received&nbsp;&nbsp;</th><th>Event</th><th>&nbsp;&nbsp;&nbsp;&nbsp;Source&nbsp;&nbsp;&nbsp;&nbsp;</th><th>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Location&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</th><th>P</th><th>Event Type</th><th>Planned Type</th><th>WTT</th><th>GBTT</th><th>Actual</th><th>&nbsp;&nbsp;Var&nbsp;&nbsp;</th><th>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Next Report (Run Time)&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</th><th>Flags</th></tr>\n");
         char query[512];
         MYSQL_RES * result2;
         MYSQL_ROW row2;

         // Calculate dom from when, deducting 24h if next_day and/or adding 24h if after 00:00.
         // time_t start_date = when - (next_day?(24*60*60):0);
         // if(sort_time < DAY_START) start_date += (24*60*60);
         // 
         // NO NO NO.  The link to get here MUST have the start date of the train, NOT the date at the point clicked.
         // So we DON'T need to fiddle here.
         time_t start_date = when;

         byte dom = broken->tm_mday;

         char trust_id[16];
         // Only accept activations where dom matches, and are +- 15 days (To eliminate last months activation.)  YUK
         sprintf(query, "SELECT created, trust_id, deduced FROM trust_activation WHERE cif_schedule_id = %ld AND substring(trust_id FROM 9) = '%02d' AND created > %ld AND created < %ld order by created", schedule_id, dom, start_date - 15*24*60*60, start_date + 15*24*60*60);
         if(!db_query(query))
         {
            result1 = db_store_result();
            while((row1 = mysql_fetch_row(result1)))
            {
               printf("<tr class=\"small-table-act\"><td>%s</td><td>Activated</td><td colspan=11>%sTrain ID = \"%s\"</td></tr>\n", time_text(atol(row1[0]), true), (atoi(row1[2]))?"[DEDUCED] ":"", row1[1]);
               sprintf(query, "SELECT created, reason, type, reinstate from trust_cancellation where trust_id='%s' AND created > %ld AND created < %ld order by created ", row1[1], start_date - 15*24*60*60, start_date + 15*24*60*60);
               strcpy(trust_id, row1[1]);
            }
            mysql_free_result(result1);
               
            if(!db_query(query))
            {
               result2 = db_store_result();
               while((row2 = mysql_fetch_row(result2)))
               {
                  if(atoi(row2[3]))
                  {
                     printf("<tr class=\"small-table-act\"><td>%s</td><td>Reinstated</td><td colspan=11>&nbsp;</td></tr>\n", time_text(atol(row2[0]), true));
                  }
                  else
                  {
                     printf("<tr class=\"small-table-cape\"><td>%s</td><td>Cancelled</td><td colspan=11>Reason %s %s</td></tr>\n", time_text(atol(row2[0]), true), show_cape_reason(row2[1]), row2[2]);
                  }
               }
               mysql_free_result(result2);
            }

            //                       0       1           2                   3         4           5                 6               7                   8                   9             10            11                12                  13                14   
            sprintf(query, "SELECT created, event_type, planned_event_type, platform, loc_stanox, actual_timestamp, gbtt_timestamp, planned_timestamp, timetable_variation, event_source, offroute_ind, train_terminated, variation_status, next_report_stanox, next_report_run_time from trust_movement where trust_id='%s' AND created > %ld AND created < %ld order by actual_timestamp, planned_timestamp, created", trust_id, start_date - 15*24*60*60, start_date + 15*24*60*60);
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
         printf("<input type=\"text\" id=\"train_date\" size=\"8\" maxlength=\"8\" value=\"\" onkeydown=\"if(event.keyCode == 13) train_date_onclick(%ld); else ar_off();\">\n", schedule_id);
         printf(" <button id=\"search\" class=\"cp-button\" onclick=\"train_date_onclick(%ld);\">Show</button> \n", schedule_id);
         printf("</td></tr></table>\n");
         printf("</p>\n");
      }
      printf("</table>\n");
      printf("</td></tr></table>\n"); // Outer table

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
   // In here when = 0 means no date supplied, NOT today.

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
   strcat(query, "runs_mo, runs_tu, runs_we, runs_th, runs_fr, runs_sa, runs_su, created, deleted, deduced_headcode, deduced_headcode_status");
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
         printf("         Deduced headcode: %s", row0[34]);
         if(row0[35][0]) printf(" (%s)", row0[35]);
         printf("\n");
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

         //                      0              1                2            3                4        5          6      7               8                9        10    11    12                       13                 14                   15  
         sprintf(query, "SELECT location_type, record_identity, tiploc_code, tiploc_instance, arrival, departure, pass, public_arrival, public_departure, platform, line, path, engineering_allowance, pathing_allowance, performance_allowance, location_type FROM cif_schedule_locations WHERE cif_schedule_id = %ld ORDER BY next_day,sort_time", schedule_id);

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
               printf("%s", show_act(row0[15]));

               printf("\n");
            }
         }
      }

      // TRUST
      if(when)
      {
         struct tm * broken = gmtime(&when);
         printf("\nReal Time Data For %s %02d/%02d/%02d:\n", days[broken->tm_wday % 7], broken->tm_mday, broken->tm_mon + 1, broken->tm_year % 100);
         printf("%-17s %c %-40s %2s %-12s %-12s %6s %6s %6s %3s %7s %s\n", "Received", ' ', "Location", "P", "Event", "Planned", "  WTT ", " GBTT ", "Actual", "Var", "", "Next Report (Run Time) and Flags");
         char query[512];
         MYSQL_RES * result2;
         MYSQL_ROW row2;

         // Calculate dom from when, deducting 24h if next_day and/or adding 24h if after 00:00.
         // time_t start_date = when - (next_day?(24*60*60):0);
         // if(sort_time < DAY_START) start_date += (24*60*60);
         // 
         // NO NO NO.  The link to get here MUST have the start date of the train, NOT the date at the point clicked.
         // So we DON'T need to fiddle here.
         time_t start_date = when;

         byte dom = broken->tm_mday;

         char trust_id[16];
         // Only accept activations where dom matches, and are +- 15 days (To eliminate last months activation.)  YUK
         sprintf(query, "SELECT created, trust_id, deduced FROM trust_activation WHERE cif_schedule_id = %ld AND substring(trust_id FROM 9) = '%02d' AND created > %ld AND created < %ld order by created", schedule_id, dom, start_date - 15*24*60*60, start_date + 15*24*60*60);
         if(!db_query(query))
         {
            result1 = db_store_result();
            while((row1 = mysql_fetch_row(result1)))
            {
               printf("%17s Activated %s Train ID = \"%s\"\n", time_text(atol(row1[0]), true), (atoi(row1[2]))?"[DEDUCED] ":"", row1[1]);
               sprintf(query, "SELECT created, reason, type, reinstate from trust_cancellation where trust_id='%s' AND created > %ld AND created < %ld order by created ", row1[1], start_date - 15*24*60*60, start_date + 15*24*60*60);
               strcpy(trust_id, row1[1]);
            }
            mysql_free_result(result1);
               
            if(!db_query(query))
            {
               result2 = db_store_result();
               while((row2 = mysql_fetch_row(result2)))
               {
                  if(atoi(row2[3]))
                  {
                     printf("%17s Reinstated\n", time_text(atol(row2[0]), true));
                  }
                  else
                  {
                     printf("%17s Cancelled - Reason %s %s\n", time_text(atol(row2[0]), true), show_cape_reason(row2[1]), row2[2]);
                  }
               }
               mysql_free_result(result2);
            }

            //                       0       1           2                   3         4           5                 6               7                   8                   9             10            11                12                  13                14   
            sprintf(query, "SELECT created, event_type, planned_event_type, platform, loc_stanox, actual_timestamp, gbtt_timestamp, planned_timestamp, timetable_variation, event_source, offroute_ind, train_terminated, variation_status, next_report_stanox, next_report_run_time from trust_movement where trust_id='%s' AND created > %ld AND created < %ld order by actual_timestamp, planned_timestamp, created", trust_id, start_date - 15*24*60*60, start_date + 15*24*60*60);
            if(!db_query(query))
            {
               result2 = db_store_result();
               while((row2 = mysql_fetch_row(result2)))
               {
                  printf("%17s ", time_text(atol(row2[0]), true));
                  printf("%c ", row2[9][0]);
                  printf("%-40s ", show_stanox(row2[4]));
                  printf("%2s ", row2[3]); // Platform
                  printf("%-12s %-12s ", row2[1], row2[2]);
                  printf("%-6s ", show_trust_time_text(row2[7], true));
                  printf("%-6s ", show_trust_time_text(row2[6], true));
                  printf("%-6s ", show_trust_time_text(row2[5], true));
                  printf("%3s %-7s ", row2[8], row2[12]);
                  if(row2[13][0])
                     printf("%s (%s) ", show_stanox(row2[13]), row2[14]);
                  printf("%s %s\n", (row2[10][0] == '0')?"":"Off Route", (row2[11][0] == '0')?"":"Terminated");
               }
               mysql_free_result(result2);
            }
            
         }
      }
      else
      {
      }
      printf("</pre>\n");
   }
}

static char * location_name_link(const char * const tiploc, const word use_cache, const char * const view, const time_t when)
{
   // Not re-entrant
   // Set use_cache to false if hit is not expected, to bypass search.  Cache will still be updated.
   static char result[256];

   char * name = location_name(tiploc, use_cache);

   sprintf(result, "<a class=\"linkbutton\" href=\"%s%s/%s/%s\">%s</a>", URL_BASE, view, tiploc, show_date(when, 0), name);

   return result;
}

static char * location_name_and_codes(const char * const tiploc)
{
   // Does not use the cache
   char query[256];
   static char result[1024];
   MYSQL_RES * result0;
   MYSQL_ROW row0;

   sprintf(query, "select fn, 3alpha from corpus where tiploc = '%s'", tiploc);
   db_query(query);
   result0 = db_store_result();
   if((row0 = mysql_fetch_row(result0)) && row0[0][0]) 
   {
      sprintf(result, "%s (%s %s)", row0[0], tiploc, row0[1]);
   }
   else
   {
      strcpy(result, tiploc);
   }
   mysql_free_result(result0);

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

   for(i=0; i<CACHE_SIZE && use_cache; i++)
   {
      if(!strcasecmp(tiploc, cache_key[i]))
      {
         _log(DEBUG, "Cache hit for \"%s\"", tiploc);
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
   mysql_free_result(result0);

   _log(DEBUG, "Cache miss for \"%s\" - Returning \"%s\"", tiploc, cache_val[next_cache]);
   return cache_val[next_cache];
}

static char * show_stanox(const char * const stanox)
{
   static char result[256];
   char  query[256];
   MYSQL_RES * result0;
   MYSQL_ROW row0;

   sprintf(query, "SELECT fn FROM corpus WHERE stanox = %s", stanox);
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

   sprintf(query, "SELECT tiploc FROM corpus WHERE stanox = %s", stanox);
   db_query(query);
   result0 = db_store_result();
   if((row0 = mysql_fetch_row(result0)) && row0[0][0]) 
   {
      return location_name_link(row0[0], true, "full", 0);
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

static char * show_trust_time_text(const char * const s, const word local)
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

   if((stamp%60) > 29) strcat(result, "H");

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
   if(scheduled[4] == '&') strcat(result, "&half;");
   return result;
}

static void display_status(void)
{
   _log(PROC, "display_status()");
   char q[256];
   MYSQL_RES * result;
   MYSQL_ROW row;
   word status; // 0 Green, 1 Yellow, 2 Red 3 Black
   char status_text[256], zs[256];

   printf("<h2>Openrail System Status</h2>\n");

   printf("<table>");

   // Timetable
   status = 3;
   strcpy(status_text, "Failed to read database.");
   sprintf(q, "SELECT time FROM updates_processed ORDER BY time DESC LIMIT 1");
   if(!db_query(q))
   {
      result = db_store_result();
      if((row = mysql_fetch_row(result)) && row[0][0]) 
      {
         time_t when = atol(row[0]);
         time_t age = now - when;
         if(age > 29*60*60) status = 2;
         else status = 0;
         strcpy(status_text, time_text(when, true));
      }
   }
   printf("<tr><td class=\"status-head\" colspan=2><br>Timetable feed</td></tr>");
   printf("<tr><td class=\"status-text\">Last update timestamp </td>");
   printf("<td class=\"status%d\"> %s </td>\n", status, status_text);
   printf("</tr>\n");

   // VSTP
   status = 3;
   strcpy(status_text, "Failed to read database.");
   sprintf(q, "SELECT last_vstp_processed FROM status");
   if(!db_query(q))
   {
      result = db_store_result();
      if((row = mysql_fetch_row(result)) && row[0][0]) 
      {
         time_t when = atol(row[0]);
         if(when)
         {
            time_t age = now - when;
            if(age > 2*60*60) status = 2;
            else if(age > 1*60*60) status = 1;
            else status = 0;
            strcpy(status_text, time_text(when, true));
            age += (30*60);
            age /= (60*60);
            if(age > 2)
            {
               sprintf(zs, " (%ld hours)", age);
               strcat(status_text, zs);
            }
         }
      }
   }
   printf("<tr><td class=\"status-head\" colspan=2><br>VSTP feed</td></tr>");
   printf("<tr><td class=\"status-text\">Last message processed </td>");
   printf("<td class=\"status%d\"> %s </td>\n", status, status_text);
   printf("</tr>\n");

   // Train movement feed
   status = 3;
   strcpy(status_text, "Failed to read database.");
   sprintf(q, "SELECT last_trust_actual FROM status");
   if(!db_query(q))
   {
      result = db_store_result();
      if((row = mysql_fetch_row(result)) && row[0][0]) 
      {
         time_t when = atol(row[0]);
         if(when)
         {
            time_t age = now - when;
            if(age > 10*60) status = 2;
            else if(age > 60) status = 1;
            else status = 0;
            strcpy(status_text, time_text(when, true));
            age += (30*60);
            age /= (60*60);
            if(age > 1)
            {
               sprintf(zs, " (%ld hours)", age);
               strcat(status_text, zs);
            }
         }
      }
   }
   printf("<tr><td class=\"status-head\" colspan=2><br>Train movement feed</td></tr>");
   printf("<tr><td class=\"status-text\">Most recent timestamp </td>");
   printf("<td class=\"status%d\"> %s </td>\n", status, status_text);
   printf("</tr>\n");

   status = 3;
   strcpy(status_text, "Failed to read database.");
   sprintf(q, "SELECT last_trust_processed FROM status");
   if(!db_query(q))
   {
      result = db_store_result();
      if((row = mysql_fetch_row(result)) && row[0][0]) 
      {
         time_t when = atol(row[0]);
         if(when)
         {
            time_t age = now - when;
            if(age > 256) status = 2;
            else if(age > 16) status = 1;
            else status = 0;
            strcpy(status_text, time_text(when, true));
            age += (30*60);
            age /= (60*60);
            if(age > 1)
            {
               sprintf(zs, " (%ld hours)", age);
               strcat(status_text, zs);
            }
         }
      }
      mysql_free_result(result);
   }
   printf("<tr><td class=\"status-text\">Last message processed </td>");
   printf("<td class=\"status%d\"> %s </td>\n", status, status_text);
   printf("</tr>\n");

   status = 3;
   strcpy(status_text, "Failed to read database.");

   sprintf(q, "SELECT time, count from message_count WHERE application = 'trustdb' and time >= %ld ORDER BY time", now - MESSAGE_RATE_PERIOD);
   if(!db_query(q))
   {
      strcpy(status_text, "Unknown.");
      status = 2;
      result = db_store_result();
      if((row = mysql_fetch_row(result))) 
      {
         time_t start = atol(row[0]);
         dword count = 0;
         time_t end = 0;
         while((row = mysql_fetch_row(result)))
         {
            end = atol(row[0]);
            count += atol(row[1]);
         }

         if(end > start)
         {
            dword rate = count * 60 / (end - start);
            if(rate < 10) status = 2;
            else if(rate < 100) status = 1;
            else status = 0;
            sprintf(status_text, "%ld per minute.", rate);
         }
      }
   }
   printf("<tr><td class=\"status-text\">Message process rate </td>");
   printf("<td class=\"status%d\"> %s </td>\n", status, status_text);
   printf("</tr>\n");

   // TD feed
   status = 3;
   strcpy(status_text, "Failed to read database.");
   sprintf(q, "SELECT last_timestamp FROM td_status ORDER BY last_timestamp DESC");
   if(!db_query(q))
   {
      result = db_store_result();
      if((row = mysql_fetch_row(result)) && row[0][0]) 
      {
         time_t when = atol(row[0]);
         if(when)
         {
            time_t age = now - when;
            if(age > 64) status = 2;
            else status = 0;
            strcpy(status_text, time_text(when, true));
            age += (30*60);
            age /= (60*60);
            if(age > 1)
            {
               sprintf(zs, " (%ld hours)", age);
               strcat(status_text, zs);
            }
         }
      }
   }
   printf("<tr><td class=\"status-head\" colspan=2><br>Train describer feed</td></tr>");
   printf("<tr><td class=\"status-text\">Most recent timestamp </td>");
   printf("<td class=\"status%d\"> %s </td>\n", status, status_text);
   printf("</tr>\n");

   status = 3;
   strcpy(status_text, "Failed to read database.");
   sprintf(q, "SELECT last_td_processed FROM status");
   if(!db_query(q))
   {
      result = db_store_result();
      if((row = mysql_fetch_row(result)) && row[0][0]) 
      {
         time_t when = atol(row[0]);
         if(when)
         {
            time_t age = now - when;
            if(age > 64) status = 2;
            else status = 0;
            strcpy(status_text, time_text(when, true));
            age += (30*60);
            age /= (60*60);
            if(age > 1)
            {
               sprintf(zs, " (%ld hours)", age);
               strcat(status_text, zs);
            }
         }
      }
   }
   printf("<tr><td class=\"status-text\">Last message processed </td>");
   printf("<td class=\"status%d\"> %s </td>\n", status, status_text);
   printf("</tr>\n");

   status = 3;
   strcpy(status_text, "Failed to read database.");

   sprintf(q, "SELECT time, count from message_count WHERE application = 'tddb' and time >= %ld ORDER BY time", now - MESSAGE_RATE_PERIOD);
   if(!db_query(q))
   {
      strcpy(status_text, "Unknown.");
      status = 2;
      result = db_store_result();
      if((row = mysql_fetch_row(result))) 
      {
         time_t start = atol(row[0]);
         dword count = 0;
         time_t end = 0;
         while((row = mysql_fetch_row(result)))
         {
            end = atol(row[0]);
            count += atol(row[1]);
         }

         if(end > start)
         {
            dword rate = count * 60 / (end - start);
            if(rate < 10) status = 2;
            else if(rate < 100) status = 1;
            else status = 0;
            sprintf(status_text, "%ld per minute.", rate);
         }
      }
   }
   printf("<tr><td class=\"status-text\">Message process rate </td>");
   printf("<td class=\"status%d\"> %s </td>\n", status, status_text);
   printf("</tr>\n");

   status = 3;
   strcpy(status_text, "Failed to read database.");

   sprintf(q, "SELECT time, count from message_count WHERE application = 'tddbrel' and time >= %ld ORDER BY time", now - MESSAGE_RATE_PERIOD);
   if(!db_query(q))
   {
      strcpy(status_text, "Unknown.");
      status = 2;
      result = db_store_result();
      if((row = mysql_fetch_row(result))) 
      {
         time_t start = atol(row[0]);
         dword count = 0;
         time_t end = 0;
         while((row = mysql_fetch_row(result)))
         {
            end = atol(row[0]);
            count += atol(row[1]);
         }

         if(end > start)
         {
            dword rate = count * 60 / (end - start);
            if(rate < 2) status = 2;
            else status = 0;
            sprintf(status_text, "%ld per minute.", rate);
         }
      }
   }
   printf("<tr><td class=\"status-text\">Relevant message process rate </td>");
   printf("<td class=\"status%d\"> %s </td>\n", status, status_text);
   printf("</tr>\n");

   // Disc space
   status = 3;
   strcpy(status_text, "Failed to read disc data.");
   {
      struct statfs fstatus;
      if(!statfs("/tmp", &fstatus))
      {
         qword free = fstatus.f_bavail * fstatus.f_bsize;
         qword total = fstatus.f_blocks * fstatus.f_bsize;
         qword perc = total?(free*100/total):0;
         if(perc < 5) status = 2;
         else if (perc < 10) status = 1;
         else status = 0;
         sprintf(status_text, "%s bytes (%lld%%)", commas_q(free), perc);
      }
   }
   printf("<tr><td class=\"status-head\" colspan=2><br>Disc</td></tr>");
   printf("<tr><td class=\"status-text\">Free space </td>");
   printf("<td class=\"status%d\"> %s</td>\n", status, status_text);
   printf("</tr>\n");

   printf("</table>\n");
}

static void display_status_panel(const word column)
{
   _log(PROC, "display_status_panel()");
   char q[256];
   MYSQL_RES * result;
   MYSQL_ROW row;
   word status; // 0 Green, 1 Yellow, 2 Red 3 Black
   char status_text[256], zs[256];

   // Timetable
   status = 3;
   strcpy(status_text, "DB error.");
   sprintf(q, "SELECT time FROM updates_processed ORDER BY time DESC LIMIT 1");
   if(!db_query(q))
   {
      result = db_store_result();
      if((row = mysql_fetch_row(result)) && row[0][0]) 
      {
         time_t when = atol(row[0]);
         time_t age = now - when;
         if(age > 29*60*60) status = 2;
         else status = 0;
         strcpy(status_text, time_text(when, true));
      }
   }
   printf("tr%d21|status%d||%s\n", column, status, status_text);

   // VSTP
   status = 3;
   strcpy(status_text, "DB error.");
   sprintf(q, "SELECT last_vstp_processed FROM status");
   if(!db_query(q))
   {
      result = db_store_result();
      if((row = mysql_fetch_row(result)) && row[0][0]) 
      {
         time_t when = atol(row[0]);
         if(when)
         {
            time_t age = now - when;
            if(age > 2*60*60) status = 2;
            else if(age > 1*60*60) status = 1;
            else status = 0;
            strcpy(status_text, time_text(when, true));
            age += (30*60);
            age /= (60*60);
            if(age > 2)
            {
               sprintf(zs, " (%ldh)", age);
               strcat(status_text, zs);
            }
         }
      }
   }
   printf("tr%d23|status%d||%s\n", column, status, status_text);

   // Train movement feed
   status = 3;
   strcpy(status_text, "DB error.");
   sprintf(q, "SELECT last_trust_actual FROM status");
   if(!db_query(q))
   {
      result = db_store_result();
      if((row = mysql_fetch_row(result)) && row[0][0]) 
      {
         time_t when = atol(row[0]);
         if(when)
         {
            time_t age = now - when;
            if(age > 10*60) status = 2;
            else if(age > 60) status = 1;
            else status = 0;
            strcpy(status_text, time_text(when, true));
            age += (30*60);
            age /= (60*60);
            if(age > 1)
            {
               sprintf(zs, " (%ldh)", age);
               strcat(status_text, zs);
            }
         }
      }
   }
   printf("tr%d25|status%d||%s\n", column, status, status_text);

   status = 3;
   strcpy(status_text, "DB Error.");

   sprintf(q, "SELECT time, count from message_count WHERE application = 'trustdb' and time >= %ld ORDER BY time", now - MESSAGE_RATE_PERIOD);
   if(!db_query(q))
   {
      strcpy(status_text, "Unknown.");
      status = 2;
      result = db_store_result();
      if((row = mysql_fetch_row(result))) 
      {
         time_t start = atol(row[0]);
         dword count = 0;
         time_t end = 0;
         while((row = mysql_fetch_row(result)))
         {
            end = atol(row[0]);
            count += atol(row[1]);
         }

         if(end > start)
         {
            dword rate = count * 60 / (end - start);
            if(rate < 10) status = 2;
            else if(rate < 100) status = 1;
            else status = 0;
            sprintf(status_text, "(%ld per minute.)", rate);
         }
      }
   }
   printf("tr%d26|status%d||%s\n", column, status, status_text);

   // TD feed
   status = 3;
   strcpy(status_text, "DB error.");
   sprintf(q, "SELECT last_timestamp FROM td_status ORDER BY last_timestamp DESC");
   if(!db_query(q))
   {
      result = db_store_result();
      if((row = mysql_fetch_row(result)) && row[0][0]) 
      {
         time_t when = atol(row[0]);
         if(when)
         {
            time_t age = now - when;
            if(age > 64) status = 2;
            else status = 0;
            strcpy(status_text, time_text(when, true));
            age += (30*60);
            age /= (60*60);
            if(age > 1)
            {
               sprintf(zs, " (%ldh)", age);
               strcat(status_text, zs);
            }
         }
      }
   }
   printf("tr%d28|status%d||%s\n", column, status, status_text);

   status = 3;
   strcpy(status_text, "DB Error.");

   sprintf(q, "SELECT time, count from message_count WHERE application = 'tddb' and time >= %ld ORDER BY time", now - MESSAGE_RATE_PERIOD);
   if(!db_query(q))
   {
      strcpy(status_text, "Unknown.");
      status = 2;
      result = db_store_result();
      if((row = mysql_fetch_row(result))) 
      {
         time_t start = atol(row[0]);
         dword count = 0;
         time_t end = 0;
         while((row = mysql_fetch_row(result)))
         {
            end = atol(row[0]);
            count += atol(row[1]);
         }

         if(end > start)
         {
            dword rate = count * 60 / (end - start);
            if(rate < 10) status = 2;
            else if(rate < 100) status = 1;
            else status = 0;
            sprintf(status_text, "(%ld per minute.)", rate);
         }
      }
   }
   printf("tr%d29|status%d||%s\n", column, status, status_text);

   // Disc space
   status = 3;
   strcpy(status_text, "Error.");
   {
      struct statfs fstatus;
      if(!statfs("/tmp", &fstatus))
      {
         qword free = fstatus.f_bavail * fstatus.f_bsize;
         qword total = fstatus.f_blocks * fstatus.f_bsize;
         qword perc = total?(free*100/total):0;
         if(perc < 5) status = 2;
         else if (perc < 10) status = 1;
         else status = 0;
         sprintf(status_text, "%s B (%lld%%)", commas_q(free), perc);
      }
   }
   printf("tr%d31|status%d||%s\n", column, status, status_text);
}

static void mobilef(void)
{
   char location[256];
   char query[1024];
   MYSQL_RES * result;
   MYSQL_ROW row;
   word hit = false;

   if(parameters[1][0])
   {
      db_real_escape_string(location, parameters[1], strlen(parameters[1]));
      // If auto-spell created it, it may have a spurious trailing space.
      while(strlen(location) && location[strlen(location) - 1] == ' ') location[strlen(location) - 1] = '\0';
      {
         // Decode 3alpha and search for friendly name match
         // Process location NLC to TIPLOC
         if(strlen(location) == 3)
         {
            sprintf(query, "SELECT tiploc,fn FROM corpus WHERE 3alpha = '%s'", location);
            if(!db_query(query))
            {
               result = db_store_result();
               while((row = mysql_fetch_row(result)) && row[0][0]) 
               {
                  hit = true;
                  printf("%s|%s\n", row[0], row[1]);
                  _log(DEBUG, "mobilef() found \"%s\" \"%s\" from 3alpha.", row[0], row[1]);
               }
               mysql_free_result(result);
            }
         }
         if(strlen(location) < 8)
         {
            // Try TIPLOC
            sprintf(query, "SELECT tiploc, fn FROM corpus WHERE tiploc = '%s'", location);
            if(!db_query(query))
            {
               result = db_store_result();
               while((row = mysql_fetch_row(result)) && row[0][0]) 
               {
                  hit = true;
                  printf("%s|%s\n", row[0], row[1]);
                  _log(DEBUG, "mobilef() found \"%s\" \"%s\" from TIPLOC.", row[0], row[1]);
               }
               mysql_free_result(result);
            }
         }

         if(!hit)
         {
            // Try fn
            sprintf(query, "SELECT tiploc, fn FROM corpus WHERE fn like '%%%s%%' AND tiploc != '' AND 3alpha != '' ORDER BY fn", location);
            if(!db_query(query))
            {
               result = db_store_result();
               while((row = mysql_fetch_row(result)) && row[0][0]) 
               {
                  printf("%s|%s\n", row[0], row[1]);
                  _log(DEBUG, "mobilef() found \"%s\" \"%s\" from fn.", row[0], row[1]);
               }
               mysql_free_result(result);
            }
         }
      }
   }
}

static const char * const show_cape_reason(const char * const code)
{
   // Decode a cancellation reason using a list in the database.
   // Note:  DB errors (table not present, code not found, etc.) are handled silently.
   // Returned string is in the format 'AA [Reason]' if found, or 'AA' if not found.
   static char reason[128];
   char query[128];
   MYSQL_RES * result;
   MYSQL_ROW row;

   strcpy(reason, code);

   if(strlen(code) < 2) return reason;

   sprintf(query, "SELECT reason FROM cape_reasons WHERE code = '%s'", code);
   if(!db_query(query))
   {
      result = db_store_result();
      if((row = mysql_fetch_row(result)) && row[0][0])
      {
         sprintf(reason, "%s [%s]", code, row[0]);
      }
      mysql_free_result(result);
   }
   return reason;
}

static word get_sort_time(const char const * buffer)
{
   word result;
   char zs[8];

   zs[0]=buffer[0];
   zs[1]=buffer[1];
   zs[2]='\0';
   result = atoi(zs)*4*60;
   zs[0]=buffer[2];
   zs[1]=buffer[3];
   result += 4*atoi(zs);
   if(buffer[4] == 'H') result += 2;

   return result;
}
