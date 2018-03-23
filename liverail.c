/*
    Copyright (C) 2013, 2014, 2015, 2016, 2017 Phil Wieland

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

#define NAME "liverail"

#ifndef RELEASE_BUILD
#define BUILD "YC24p"
#else
#define BUILD RELEASE_BUILD
#endif

static void depsheet(void);
static void display_choice(MYSQL_RES * result0, const char * const view, const time_t when);
static void display_control_panel(const char * const location, const time_t when);
static void display_help_text(void);
static void report_train(const word index, const time_t when);
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
static char * show_act_text(const char * const input);
static char * show_trust_time(const char * const ms, const word local);
static char * show_trust_time_nocolon(const char * const ms, const word local);
static char * show_expected_time(const char * const scheduled, const word deviation, const word late);
static void display_status(void);
static void display_status_panel(const word column);
static void display_jiancha(void);
static void mobilef(void);
static const char * const show_cape_reason(const char * const code);
static word get_sort_time(const char const * buffer);

#define COLUMNS_NORMAL 5
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
   "ZZLight Locomotive",
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

#define ACTIVITIES 25
static const char * activities[ACTIVITIES] = {
   "A Stops or shunts for other trains to pass",
   "AEAttach/detach assisting locomotive",
   "BLStops for banking locomotive",
   "C Stops to change trainmen",
   "D Stops to set down passengers",
   "-DStops to detach vehicles",
   "E Stops for examination",
   "L Stops to change locomotives",
   "N Stop not advertised",
   "OPStops for other operating reasons",
   "ORTRain locomotive on rear",
   "PRPropelling between points shown",
   "R Stops when required",
   "RMReversing movement or driver changes ends",
   "RRStops for locomotive to run round",
   "S Stops for railway personnel only",
   "T Stops to take up and set down passengers",
   "-TStops to attach and detach vehicles",
   "TBTrain begins",
   "TFTrain finishes",
   "TWStops (or at pass) for tablet, staff or token",
   "U Stops to take up passengers",
   "-UStops to attach vehicles",
   "W Stops for watering of coaches",
   "X Passes another train at crossing point on single line",
};
static const char * activities_text[ACTIVITIES] = {
   "A Shunts for other trains to pass",
   "AEAttach/detach assisting locomotive",
   "BLStops for banking locomotive",
   "C Stops to change trainmen",
   "D Stops to set down passengers",
   "-DStops to detach vehicles",
   "E Stops for examination",
   "L Stops to change locomotives",
   "N Stop not advertised",
   "OPStops for other operating reasons",
   "ORTRain locomotive on rear",
   "PRPropelling between points shown",
   "R Stops when required",
   "RMRM",
   "RRStops for locomotive to run round",
   "S Stops for railway personnel only",
   "T Take up and set down passengers",
   "-TStops to attach and detach vehicles",
   "TBTrain begins",
   "TFTrain finishes",
   "TWTablet, staff, or token exchange",
   "U Stops to take up passengers",
   "-UStops to attach vehicles",
   "W Stops for watering of coaches",
   "X Passes another train at crossing point on single line",
};

static char * variation_status[4] = {"Early", "On time", "Late", "Off route"};

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
enum modes        { FULL, FREIGHT, SUMMARY, DEPART, PANEL, PANELU, SUMMARYU, DEPARTU, MOBILE, MOBILEF, TRAIN  , TRAINT, STATUS, MENUONLY, JIANCHA, MODES} mode;
// 0x0001 HTML header and footer
// 0x0002 Update
// 0x0004 Sort using: 0 = sort_time database field. 1 = Prefer departure time
// 0x0008 Updateable - Supports smart updates.
word modef[MODES] = {0x0001,0x0001,0x000d,0x000d, 0x000d, 0x0006, 0x0006  , 0x0006 , 0x0004, 0x0000, 0x0001 , 0x0001, 0x0001, 0x0001, 0x0000};

static word mobile_trains, mobile_time;

#define MAX_CALLS 4096
static struct call_details
   {
      dword garner_schedule_id;
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
   else if(!strcasecmp(parameters[0], "frt")) mode = FREIGHT;
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
   else if(!strcasecmp(parameters[0], "jiancha")) mode = JIANCHA;
   _log(DEBUG, "Display mode  = %d", mode);

   if(modef[mode] & 0x0001)
   {
      printf("Content-Type: text/html; charset=iso-8859-1\n\n");
      printf("<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\" \"http://www.w3.org/TR/html4/loose.dtd\">\n");
      printf("<html xmlns=\"http://www.w3.org/1999/xhtml\" lang=\"en\" xml:lang=\"en\">\n");
      printf("<head>\n");
      printf("<title>%s %s</title>\n", NAME, BUILD);
      printf("<link rel=\"stylesheet\" type=\"text/css\" href=\"/auxy/liverail.css\">\n");
      printf("<script type=\"text/javascript\" src=\"/auxy/liverail.js\"></script>\n");
      printf("</head>\n");
      if(mode == PANEL)
         printf("<body class=\"panel-body\" onload=\"startup();\">\n");
      else
         printf("<body onload=\"startup();\">\n");
   }
   else
   {
      printf("Content-Type: text/plain\nCache-Control: no-cache\n\n");
   }
   // Initialise location name cache
   location_name(NULL, false);

   // Initialise database
   db_init(conf[conf_db_server], conf[conf_db_user], conf[conf_db_password], conf[conf_db_name]);

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
   case FREIGHT:
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
      display_help_text();
      break;

   case STATUS:
      display_control_panel("", 0);
      display_status();
      break;

   case JIANCHA:
      display_jiancha();
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
      time_t now = time(NULL);
      elapsed = elapsed * 1000 + ha_clock.tv_usec / 1000;
      elapsed -= start_time;
      char banner[1024];
      {
         banner[0] = '\0';
         MYSQL_RES * result0;
         MYSQL_ROW row0;
         if(!db_query("SELECT banner, expires from banners WHERE type = 'webpage'"))
         {
            result0 = db_store_result();
            if((row0 = mysql_fetch_row(result0)) && row0[0][0])
            {
               time_t expires = atol(row0[1]);
               if(expires == 0 || expires > now)
               {
                  sprintf(banner, "<span style=\"background-color:yellow;\">%s</span><br><br>", row0[0]);
               }
            }
            mysql_free_result(result0);
         }
      }
      if(modef[mode] & 0x0002)
      {
         printf("%sUpdated at %s by %s %s at %s.  Elapsed time %s ms.\n", banner, time_text(now, 1), NAME, BUILD, host, commas_q(elapsed));
      }
      else 
      {
         if(mode == PANEL)
         {
            printf("<p id=\"bottom-line\">Completed at %s by %s %s at %s.  Elapsed time %s ms.</p>\n", time_text(now, 1), NAME, BUILD, host, commas_q(elapsed));
         }
         else
         {
            printf("<p><div id=\"bottom-line\">%sCompleted at %s by %s %s at %s.  Elapsed time %s ms.</div>\n", banner, time_text(now, 1), NAME, BUILD, host, commas_q(elapsed));
            printf("<br><div class=\"licence\">Contains Information of Network Rail Infrastructure Limited licensed under the following licence: <a href=\"http://www.networkrail.co.uk/data-feeds/terms-and-conditions\"> Click Here.</a><br>");
            printf("These pages are provided strictly for non-commercial, non-safety critical purposes; no responsibility is accepted for any inaccurate or out of date information.");
            printf("</div></p>");
         }
         printf("</body></html>\n\n");
      }
   }
   exit(0);
}

static void display_choice(MYSQL_RES * result0, const char * const view, const time_t when)
{
   MYSQL_ROW row0;
   
   printf("<h2>Select desired location</h2>\n");

   printf("<table>");
   printf("<tr class=\"small-table\"><th>TIPLOC</th><th>3-alpha</th><th>Location</th></tr>\n");

   while((row0 = mysql_fetch_row(result0)) ) 
   {
      printf("<tr class=\"small-table\"><td>%s</td><td>%s</td><td>%s</td></tr>\n", row0[0],  row0[1], location_name_link(row0[0], false, view, when));
   }
   printf("</table>");
}

static void display_control_panel(const char * const location, const time_t when)
{
   printf("<table><tr><td class=\"control-panel-row\">\n");
   
   printf("&nbsp;Show trains at <input type=\"text\" class=\"control-panel-box\" id=\"search_loc\" size=\"26\" maxlength=\"64\" placeholder=\"Location name / TIPLOC / 3-alpha\" value=\"%s\" onkeydown=\"if(event.keyCode == 13) search_onclick(); else ar_off();\">\n", location);

   time_t x = now - (DAY_START * 15);
   struct tm * broken = localtime(&x);
   word d = broken->tm_mday;
   word m = broken->tm_mon;
   word y = broken->tm_year;
   broken = localtime(&when);
   printf("on <input type=\"text\" class=\"control-panel-box\" id=\"search_date\" size=\"8\" maxlength=\"8\" placeholder=\"dd/mm/yy\" value=\"");
   if(!when || (d == broken->tm_mday && m == broken->tm_mon && y == broken->tm_year))
   {
   }
   else
   {
      printf("%02d/%02d/%02d", broken->tm_mday, broken->tm_mon + 1, broken->tm_year % 100);
   }
   printf("\" onkeydown=\"if(event.keyCode == 13) search_onclick(); else ar_off();\">\n");

   printf(" <button id=\"search\" class=\"cp-button\" onclick=\"search_onclick();\">All</button>\n");
   printf(" <button id=\"search\" class=\"cp-button\" onclick=\"freight_onclick();\">Freight</button>\n");
   printf(" <button id=\"search\" class=\"cp-button\" onclick=\"summary_onclick();\">Summary</button>\n");
   printf(" <button id=\"search\" class=\"cp-button\" onclick=\"depart_onclick();\">Departures</button>\n");

   printf("&nbsp;</td><td width=\"4%%\"></td><td class=\"control-panel-row\">&nbsp;&nbsp;<button id=\"as_rq\" class=\"cp-button\" onclick=\"as_rq_onclick();\">Advanced Search</button>");
   printf("&nbsp;&nbsp;<button id=\"status\" class=\"cp-button\" onclick=\"status_onclick();\">Status</button>&nbsp;\n");

   printf("&nbsp;</td><td width=\"4%%\"></td><td class=\"control-panel-row\">&nbsp;Auto-refresh&nbsp;<input type=\"checkbox\" id=\"ar\" onclick=\"ar_onclick();\"%s>&nbsp;\n", refresh?" checked":"");
   
   printf("</td><td id=\"progress\" class=\"control-panel-row\" width=\"1%%\" valign=\"top\">&nbsp;");

   printf("</td></tr></table>\n");
}

static void display_help_text(void)
{
   printf("<div class=\"help-text-box\">");
   printf("<p><b>Welcome to the Open Rail real time railway data pages.</b></p>");

   printf("<p>Use the menu bar above to show details of the trains at a particular location.</p> \
<ul><li>In the location box, put a TIPLOC or a National Rail three character code or just a part of the location name.  If more than one location matches you will be given a list to choose from.</li> \
<li>Leave the date box blank to see today's trains.  If you miss the year, this year will be assumed.</li>\
<li>Then click one of the buttons:</li>\
<li><button class=\"cp-button\">All</button> gives a detailed list of all trains.</li>\
<li><button class=\"cp-button\">Freight</button> only shows freight trains.</li>\
<li><button class=\"cp-button\">Summary</button> provides a summary screen showing all trains.</li>\
<li><button class=\"cp-button\">Departures</button> provides a summary screen showing only passenger departures.</li></ul></p>");
  
   printf("<p>To find live information for a specific train, find it at a location using the pages described above, and then click on the train for more details.</p>");

   printf("<p>Click on the <button class=\"cp-button\">Advanced Search</button> button to access a number of more sophisticated database queries.</p>");

   printf("</div>\n");
}

static void depsheet(void)
{
   word cif_schedule_count;

   MYSQL_RES * result0;
   MYSQL_ROW row0;
   char query[4096], zs[256];

   time_t when;

   qword start_us = time_us();

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
            sprintf(query, "SELECT tiploc, 3alpha FROM corpus WHERE fn like '%%%s%%' and tiploc != '' order by fn", location);
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
                  case FREIGHT: display_choice(result0, "frt",  when); break;
                  default:      display_choice(result0, "full", when); break;
                  }
                  mysql_free_result(result0);
                  return;
               }
               else if(found == 0)
               {
                  // No match found.
                  if((modef[mode] & 0x0002))
                  {
                     // Update - spoof an empty update
                     printf("---\n");
                  }
                  else
                  {
                     display_control_panel("", when);
                     printf("<h2>No matches found for location \"%s\".</h2>\n", location);
                     printf("<input type=\"hidden\" id=\"display_handle\" value=\"---\">\n");
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
      // Set up default locations
      switch(mode)
      {   
      case FULL:
      case FREIGHT:
      case DEPART:
      case PANEL:
      case DEPARTU:
      case PANELU:
      case MOBILE:
      case SUMMARY:
      case SUMMARYU:
         strcpy(location, "HUYTON");
         break;

      default:
         strcpy(location, "");
      }
   }
   
   _log(DEBUG, "%10s us ] Determined location \"%s\".", commas_q(time_us() - start_us), location);

   struct tm broken = *localtime(&when);

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
   case FREIGHT:
   case SUMMARY:
   case DEPART:
      display_control_panel(location, when);
      printf("<h2>");
      if(mode == DEPART) printf ("Passenger departures from ");
      else if(mode == FREIGHT) printf("Freight services at ");
      else printf("Services at ");
      printf("%s on %s %02d/%02d/%02d</h2>", location_name_and_codes(location), days[broken.tm_wday % 7], broken.tm_mday, broken.tm_mon + 1, broken.tm_year % 100);
      break;

   case PANEL:
      printf("<div style=\"display:none;\">");
      display_control_panel(location, when);
      printf("</div>");
      break;
   default:
      break;
   }
   
   switch(mode)
   {
   case FULL:
   case FREIGHT:
   case SUMMARY:
   case DEPART:
      if(when < now - 2*24*60*60)
      {
         if(!db_query("SELECT MIN(created) FROM trust_activation"))
         {
            result0 = db_store_result();
            if((row0 = mysql_fetch_row(result0)))
            {
               if(atol(row0[0]) + 24*60*60 > when)
               {
                  printf("<p>The database no longer contains records for the requested date.</p>");
                  if(modef[mode] & 0x0008) // Supports smart updates.
                  {
                     printf("\n<input type=\"hidden\" id=\"display_handle\" value=\"AROFF\">");
                  }
                  return;
               }
            }
         }
      }
      else
      {
         if(when > now + 12*7*24*60*60)
         {
            printf("<p>The requested date is more than twelve weeks in the future - All information should be considered provisional.</p>");
         }
      }             
      break;
      
   default:
      break;
   }
   //                    0                          1                      2                             3         4                5             6           7             8            9            10      11         12
   strcpy(query, "SELECT cif_schedules.id, cif_schedules.CIF_train_uid, cif_schedules.CIF_stp_indicator, next_day, sort_time, record_identity, arrival, public_arrival, departure, public_departure, pass, platform, tiploc_code");
   strcat(query, " FROM cif_schedules INNER JOIN cif_schedule_locations");
   strcat(query, " ON cif_schedules.id = cif_schedule_locations.cif_schedule_id");
      sprintf(zs, " WHERE (cif_schedule_locations.tiploc_code = '%s')", location);
      strcat(query, zs);
   
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
   
   sprintf(zs, " AND ((((%s) AND (schedule_start_date <= %ld) AND (schedule_end_date >= %ld) AND (NOT next_day)) AND (sort_time >= %d))",  days_runs
[day],  when + 12*60*60, when - 12*60*60, DAY_START);
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

   if(mode == FREIGHT)
   {
      // Freight only
      strcat(query, " AND (train_status = 'F' OR train_status = '2' OR train_status = '3')");
   }

   word index, i;
   
   // 1. Collect a list of visits
   _log(DEBUG, "Step 1:  Collect a list of visits.");
   call_count = 0;
   
   _log(DEBUG, "%10s us ] Prepared Step 1 initial query.", commas_q(time_us() - start_us));
   if(!db_query(query))
   {
      _log(DEBUG, "%10s us ] Completed Step 1 initial query.", commas_q(time_us() - start_us));

      result0 = db_store_result();
      while((row0 = mysql_fetch_row(result0)))
      {
         if(call_count >= MAX_CALLS)
         {
            printf("<p>Error: MAX_CALLS exceeded.</p>");
            return;
         }

         // Insert in array
         calls[call_count].garner_schedule_id        = atol(row0[0]);
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
            word prev = calls[call_count].sort_time;
            if(calls[call_count].public_departure[0]) 
            {
               calls[call_count].sort_time = get_sort_time(calls[call_count].public_departure);
               // If we have moved across midnight, amend next_day
               // NOTE sort_time is set by cifdb to WTT arrival whenever possible.  next_day is set by cifdb to match this.
               //      next_day is NOT in the download.
               if(calls[call_count].sort_time < prev - 256 && calls[call_count].next_day == 0) calls[call_count].next_day = 1;
            }
            else if(calls[call_count].departure[0])
            {
               calls[call_count].sort_time = get_sort_time(calls[call_count].departure);
               // If we have moved across midnight, amend next_day
               if(calls[call_count].sort_time < prev - 256 && calls[call_count].next_day == 0) calls[call_count].next_day = 1;
            }
            else if(calls[call_count].pass[0])        calls[call_count].sort_time = get_sort_time(calls[call_count].pass);
         }
         call_count++;
      }
      mysql_free_result(result0);
   }
   _log(DEBUG, "%10s us ] Completed Step 1.", commas_q(time_us() - start_us));
   
   // 2. Cancel any which are overriden
   for(index = 0; index < call_count; index++)
   {
      if(calls[index].valid && calls[index].cif_stp_indicator == 'O')
      {
         for(i = 0; i < call_count; i++)
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
   _log(DEBUG, "%10s us ] Completed Step 2.", commas_q(time_us() - start_us));
   
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
                  _log(DEBUG, "Schedule %ld (%s) cancelled by schedule %s.", calls[index].garner_schedule_id, calls[index].cif_train_uid, row0[0]);
               }
               else 
               {
                  // Overlay
                  // We will come here with an overlay we already know about OR one which *doesn't come here*
                  // In either case we invalidate this schedule.  If the overlay comes here it will already be in the list, somewhere.
                  _log(DEBUG, "Examining overlay.");
                  dword overlay_id = atol(row0[0]);
                  if(overlay_id != calls[index].garner_schedule_id || calls[index].cif_stp_indicator == 'N' || calls[index].cif_stp_indicator == 'P')
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

   _log(DEBUG, "%10s us ] Completed Step 3.", commas_q(time_us() - start_us));

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
         if(calls[j].valid) schedules_key += calls[j].garner_schedule_id; // May wrap.
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
   _log(DEBUG, "%10s us ] Completed Step 5.", commas_q(time_us() - start_us));

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
            _log(DEBUG, "%10s us ] Calling report_train_summary( [%d] )", commas_q(time_us() - start_us), index);
            report_train_summary(call_sequence[index], when, cif_schedule_count);
         }
      }

      // If no trains at all, tail won't get printed.
      if(!cif_schedule_count)
      {
         if(mode == SUMMARY || mode == DEPART || mode == PANEL) printf("</tr></table>\n");
      }
      break;

   case FULL:
   case FREIGHT:
      printf("<table>\n");
      printf("<tr class=\"small-table\"><th colspan=\"8\">Information From Schedule Database</th><th>Information From TRUST Feed</tr>\n");
      printf("<tr class=\"small-table\"><th>Detail</th><th>Type</th><th>ID</th><th>CIF UID</th><th>P</th><th>Times WTT(Public)</th><th>From</th><th>To</th><th>Latest Live Data</th></tr>\n");
      cif_schedule_count = 0;
      for(index = 0; index < call_count; index++)
      {
         // printf("%s<br>\n", row0[0]);
         if(calls[call_sequence[index]].valid)
         {
            cif_schedule_count++;
            report_train(call_sequence[index], when);
         }
      }
      
      printf("</table>\n");
      break;

   default:
      break;
   }
   _log(DEBUG, "%10s us ] End.", commas_q(time_us() - start_us));
}

static void report_train(const word index, const time_t when)
{
   // One line describing the specified train in a FULL or FREIGHT report.
   MYSQL_RES * result0, * result1;
   MYSQL_ROW row0, row1;

   char query[1024];
   word vstp;

   // Calculate start_date from when, deducting 24h if next_day and/or adding 24h if after 00:00.
   time_t start_date = when - (calls[index].next_day?(24*60*60):0);
   // N.B. sort_time has been munged by now so that ones before DAY_START have had 10000 added
   if(calls[index].sort_time >= 10000) start_date += (24*60*60);

   _log(DEBUG, "calls[index].sort_time = %d, DAY_START = %d, start_date = %s", calls[index].sort_time, DAY_START, show_date(start_date, false));
   //                     0             1                    2                  3              4              5                  6          7                8
   sprintf(query, "SELECT train_status, schedule_start_date, schedule_end_date, signalling_id, CIF_train_uid, CIF_stp_indicator, update_id, deduced_headcode,deduced_headcode_status FROM cif_schedules WHERE id = %u", calls[index].garner_schedule_id);

   if(!db_query(query))
   {
      result0 = db_store_result();

      if((row0 = mysql_fetch_row(result0)))
      {
         // printf("<tr class=\"small-table\" onclick=train_onclick('%u/%s')>", calls[index].garner_schedule_id, show_date(start_date, false));
         printf("<tr class=\"small-table\">");
         vstp = (row0[6][0] == '0' && row0[6][1] == 0);

         // Link
         printf("<td><a class=\"linkbutton\" href=\"%strain/%u/%s\">Details</a></td>\n", URL_BASE, calls[index].garner_schedule_id, show_date(start_date, false));
         
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
         if(row0[3][0])
         {
            printf("<td>%s", row0[3]); // Headcode
         }
         else if(row0[7][0])
         {
            printf("<td>%s %s", row0[7], row0[8]); // Deduced headcode and status
         }
         else
         {
            printf("<td class=\"small-table-freight\">%u", calls[index].garner_schedule_id); // Garner ID
         }
         printf("</td>");

         // CIF UID and CIF STP indicator
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

         // Arrival and departure        
         printf("<td>%s</td><td>\n", calls[index].platform); 
         if(calls[index].arrival[0])          printf("a. %s", show_time(calls[index].arrival)); // arrival
         if(calls[index].public_arrival[0])   printf("(%s)",  show_time(calls[index].public_arrival)); // public arrival
         if(calls[index].departure[0])        printf(" d. %s",show_time(calls[index].departure)); // dep
         if(calls[index].public_departure[0]) printf("(%s)",  show_time(calls[index].public_departure)); // public dep
         if(calls[index].pass[0])             printf("p. %s", show_time(calls[index].pass)); // pass
         printf("</td>");
                        
         // From
         sprintf(query, "SELECT tiploc_code, departure, public_departure FROM cif_schedule_locations where cif_schedule_id = %u AND record_identity = 'LO'", calls[index].garner_schedule_id);
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
         sprintf(query, "SELECT tiploc_code, arrival, public_arrival FROM cif_schedule_locations where cif_schedule_id = %u AND record_identity = 'LT'", calls[index].garner_schedule_id);
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
            sprintf(query, "SELECT created, trust_id, deduced FROM trust_activation WHERE cif_schedule_id = %u AND substring(trust_id FROM 9) = '%02d' AND created > %ld AND created < %ld order by deduced", calls[index].garner_schedule_id, dom, when - 15*24*60*60, when + 15*24*60*60);
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
                  sprintf(query, "SELECT created, reason, type, reinstate, loc_stanox from trust_cancellation where trust_id='%s' AND created > %ld AND created < %ld order by created ", row1[1], when - 15*24*60*60, when + 15*24*60*60);
                  if(!db_query(query))
                  {
                     result2 = db_store_result();
                     while((row2 = mysql_fetch_row(result2)))
                     {
                        if(atoi(row2[3]) == 1)
                        {
                           sprintf(report, "%s Reinstated at %s", time_text(atol(row2[0]), true), show_stanox(row2[4]));
                           strcpy(class, "small-table-act");
                        }
                        else
                        {
                           sprintf(report, "%s Cancelled at %s %s %s", time_text(atol(row2[0]), true), show_stanox(row2[4]), show_cape_reason(row2[1]), row2[2]);
                           strcpy(class, "small-table-cape");
                        }
                     }
                     mysql_free_result(result2);
                  }
                  //                       0        1           2            3                   4               5
                  sprintf(query, "SELECT created, platform, loc_stanox, actual_timestamp, timetable_variation, flags from trust_movement where trust_id='%s' AND created > %ld AND created < %ld order by actual_timestamp desc, planned_timestamp desc, created desc limit 1", row1[1], when - 15*24*60*60, when + 15*24*60*60);
                  if(!db_query(query))
                  {
                     result2 = db_store_result();
                     while((row2 = mysql_fetch_row(result2)))
                     {
		        word flags = atoi(row2[5]);
                        // Abandon the report so far and just show the last movement.
			strcpy(report, show_trust_time(row2[3], true));
			sprintf(zs1, " %s ", (flags & 0x0002) ? "Arr." : "Dep.");
                        strcat(report, zs1);
                        strcat(report, show_stanox(row2[2]));
                     
                        if(row2[1][0])
                        {
                           if(row2[1][0] == ' ')
                           {
                              sprintf(zs1, " P%s", row2[1] + 1);
                           }
                           else
                           {
                              sprintf(zs1, " P%s", row2[1]);
                           }
                           strcat(report, zs1);
                        }
                        word variation = atoi(row2[4]);
                        sprintf(zs1, " %s %s", variation?row2[4]:"", variation_status[(flags & 0x0018) >> 3]);
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
         printf("</tr>\n");

      }
   
      mysql_free_result(result0);
   }

   return;
}

static void report_train_summary(const word index, const time_t when, const word ntrains)
// Despite its name, also used for SUMMARYU, DEPART, DEPARTU, PANEL, PANELU and MOBILE modes.
{
   enum statuses {NoReport, Activated, Moving, Cancelled, Arrived, Departed, DepartedDeduced};
   MYSQL_RES * result0, * result1;
   MYSQL_ROW row0, row1;

   char query[1024];
   static word trains, rows, train, row, bus, shown;
   static word nlate, ncape, nbus, ndeduced, narrival;
   word status, mobile_sched, mobile_sched_unmung, mobile_act;
   char actual[16], deduced_actual[8];
   word deviation, deduced, late, off_route;
   char train_details[256], train_time[16], train_time_prefix[4], destination[128], analysis_text[32], analysis_class[32], mobile_analysis[32], headcode[16];
   struct tm * broken;

   // Calculate start_date from when, deducting 24h if next_day and/or adding 24h if after 00:00.
   time_t start_date = when - (calls[index].next_day?(24*60*60):0);
   if(calls[index].sort_time >= 10000) start_date += (24*60*60);

   deviation = late = status = bus = deduced = off_route = 0;
   strcpy(headcode, "");

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
         printf("%x%s%x\n", broken->tm_mday, BUILD, schedules_key);
      }
      else if(modef[mode] & 0x0008) // Supports smart updates.
      {
         printf("\n<input type=\"hidden\" id=\"display_handle\" value=\"%x%s%x\">", broken->tm_mday, BUILD, schedules_key);
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
      if(mode == SUMMARY || mode == DEPART) printf("<td><table class=\"summ-table\"><tr class=\"summ-table-head\"><th colspan=3>&nbsp;</th><th>Report</th></tr>\n"); // start of outer td, start of inner table
      if(mode == PANEL)                     printf("<td><table class=\"summ-table\"><tr class=\"summ-table-head\"><th colspan=3>&nbsp;</th><th>Report</th></tr>\n"); // start of outer td, start of inner table
   }
 
   //                     0             1                    2                  3              4              5                  6          7
   sprintf(query, "SELECT train_status, schedule_start_date, schedule_end_date, signalling_id, CIF_train_uid, CIF_stp_indicator, update_id, deduced_headcode FROM cif_schedules WHERE id = %u", calls[index].garner_schedule_id);

   if(!db_query(query))
   {
      result0 = db_store_result();

      if((row0 = mysql_fetch_row(result0)))
      {
         // vstp = (row0[6][0] == '0' && row0[6][1] == 0);
         bus = (row0[0][0] == 'B' || row0[0][0] == '5');

         // To
         sprintf(query, "SELECT tiploc_code FROM cif_schedule_locations where cif_schedule_id = %u AND record_identity = 'LT'", calls[index].garner_schedule_id);
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
            sprintf(query, "SELECT tiploc_code FROM cif_schedule_locations where cif_schedule_id = %u AND record_identity = 'LO'", calls[index].garner_schedule_id);
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

         if(row0[3][0]) strcpy(headcode, row0[3]);
         else if(row0[7][0]) strcpy(headcode, row0[7]);

         switch(mode)
         {
         case DEPART:
         case DEPARTU:
            if(calls[index].public_departure[0])
               strcpy(train_time, calls[index].public_departure);
            else
               strcpy(train_time, calls[index].departure);
            // Link and destination
            sprintf(train_details, "<a class=\"linkbutton-summary\" href=\"%strain/%u/%s\">%s</a>", URL_BASE, calls[index].garner_schedule_id, show_date(start_date, false), destination);
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
            if(calls[index].departure[0])    { strcpy(train_time, calls[index].departure); strcpy(train_time_prefix,"d."); }
            else if(calls[index].arrival[0]) { strcpy(train_time, calls[index].arrival);   strcpy(train_time_prefix,"a."); }
            else if(calls[index].pass[0])    { strcpy(train_time, calls[index].pass);      strcpy(train_time_prefix,"p."); }
            else { strcpy(train_time, "?????");strcpy(train_time_prefix,"??"); }
            if(train_time[4] == 'H') strcpy(train_time + 4, "&half;");
            // Link and time and destination
            sprintf(train_details, "<a class=\"linkbutton-summary\" href=\"%strain/%u/%s\">%s</a>", URL_BASE, calls[index].garner_schedule_id, show_date(start_date, false), destination);
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
            sprintf(query, "SELECT created, trust_id, deduced FROM trust_activation WHERE cif_schedule_id = %u AND substring(trust_id FROM 9) = '%02d' AND created > %ld AND created < %ld ORDER BY created DESC", calls[index].garner_schedule_id, dom, when - 15*24*60*60, when + 15*24*60*60);
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
               // Look for movements.     0            1               2                   3                  4  
               sprintf(query, "SELECT loc_stanox, actual_timestamp, timetable_variation, planned_timestamp, flags from trust_movement where trust_id='%s' AND created > %ld AND created < %ld order by actual_timestamp, planned_timestamp, created", trust_id, when - 15*24*60*60, when + 15*24*60*60);
               if(!db_query(query))
               {
                  result1 = db_store_result();
                  while((row1 = mysql_fetch_row(result1)))
                  {
                     word flags = atoi(row1[4]);
                     if(status == Activated || status == Moving)
                     {
                        status = Moving;
                        off_route = (flags & 0x0020);
                        strcpy(actual, row1[1]);
                        deviation = atoi(row1[2]);
                        late = ((flags & 0x0018) == 0x0010);
                     }
                     if(status < Departed)
                     {
                        sprintf(query, "SELECT tiploc FROM corpus WHERE stanox = %s", row1[0]);
                        if(!db_query(query))
                        {
                           result2 = db_store_result();
                           if((row2 = mysql_fetch_row(result2)))
                           {
                              _log(DEBUG, "Looking for TIPLOC \"%s\" found movement at TIPLOC \"%s\".", calls[index].tiploc_code, row2[0]);
                              if(!strcasecmp(calls[index].tiploc_code, row2[0]))
                              {
                                 if((flags & 0x0003) == 0x0001)
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
                                       time_t planned_timestamp = atol(row1[3]);
                                       struct tm * broken = localtime(&planned_timestamp);
                                       word planned = broken->tm_hour * 60 + broken->tm_min;
                                       if(planned > sched - 8 && planned < sched + 8) // This might fail close to midnight!
                                       {
                                          // Near enough!
                                          status = Departed;
                                          strcpy(actual, row1[1]);
                                          deviation = atoi(row1[2]);
                                          late = ((flags & 0x0018) == 0x0010);
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
                                    time_t planned_timestamp = atol(row1[3]);
                                    struct tm * broken = localtime(&planned_timestamp);
                                    word planned = broken->tm_hour * 60 + broken->tm_min;
                                    if(planned > sched - 8 && planned < sched + 8) // This might fail close to midnight!
                                    {
                                       // Near enough!
                                       status = Arrived;
                                       strcpy(actual, row1[1]);
                                       deviation = atoi(row1[2]);
                                       late = ((flags & 0x0018) == 0x0010);
                                    }
                                 }
                              }
                              // Check for "gone"
                              if(status < Departed)
                              {
                                 char z[8];
                                 z[0] = train_time[0]; z[1] = train_time[1]; z[2] = '\0';
                                 word sched = atoi(z)*60;
                                 z[0] = train_time[2]; z[1] = train_time[3];
                                 sched += atoi(z);
                                 time_t planned_timestamp = atol(row1[3]);
                                 struct tm * broken = localtime(&planned_timestamp);
                                 word planned = broken->tm_hour * 60 + broken->tm_min;
                                 if(planned > sched + 2)
                                 {
                                    status = DepartedDeduced;
                                    strcpy(actual, row1[1]);
                                    deviation = atoi(row1[2]);
                                    late = ((flags & 0x0018) == 0x0010);
                                    sched += (60*24) + (late?deviation:(-deviation));
                                    sched %= (60*24);
                                    sprintf(deduced_actual, "%02d%02d", sched/60, sched%60);
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
      if(off_route)          { sprintf(analysis_text, "Off route"); strcpy(analysis_class, "summ-table-crit"); }
      else if(!deviation )   { sprintf(analysis_text, "Exp. On time"); strcpy(analysis_class, "summ-table-good"); }
      else if(deviation < 3) { sprintf(analysis_text, "Exp. %s %d%s",  show_expected_time(train_time, deviation, late), deviation, late?"L":"E"); strcpy(analysis_class, "summ-table-good"); }
      else if(deviation < 6) { sprintf(analysis_text, "Exp. %s %d%s", show_expected_time(train_time, deviation, late), deviation, late?"L":"E"); strcpy(analysis_class, "summ-table-minor"); }
      else                   { sprintf(analysis_text, "Exp. %s %d%s", show_expected_time(train_time, deviation, late), deviation, late?"L":"E"); strcpy(analysis_class, "summ-table-major"); }
      sprintf(mobile_analysis, "%d%s", deviation, late?"L":"E");
      if(deviation >= 3) nlate++;
      mobile_act = mobile_sched + (late?deviation:(-deviation));
      break;

   case Cancelled:
      strcpy(row_class, "summ-table-cape");
      sprintf(analysis_text, "Cancelled");
      strcpy(analysis_class, "summ-table-crit");
      strcpy(mobile_analysis, "Can");
      ncape++;
      break;

   case Arrived:
      if(calls[index].terminates)
         strcpy(row_class, "summ-table-gone");
      else
         strcpy(row_class, "summ-table-move");
      if(!deviation )        { sprintf(analysis_text, "On time"); strcpy(analysis_class, "summ-table-good"); }
      else if(deviation < 3) { sprintf(analysis_text, "%s %d%s",  show_trust_time_nocolon(actual, true), deviation, late?"L":"E"); strcpy(analysis_class, "summ-table-good"); }
      else if(deviation < 6) { sprintf(analysis_text, "%s %d%s", show_trust_time_nocolon(actual, true), deviation, late?"L":"E"); strcpy(analysis_class, "summ-table-minor"); }
      else                   { sprintf(analysis_text, "%s %d%s", show_trust_time_nocolon(actual, true), deviation, late?"L":"E"); strcpy(analysis_class, "summ-table-major"); }
      sprintf(mobile_analysis, "%d%s", deviation, late?"L":"E");
      if(deviation >= 3) nlate++;
      mobile_act = mobile_sched + (late?deviation:(-deviation));
      break;

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

    case DepartedDeduced:
      strcpy(row_class, "summ-table-gone");
      if(!deviation )        { sprintf(analysis_text, "On time"); strcpy(analysis_class, "summ-table-good"); }
      else if(deviation < 3) { sprintf(analysis_text, "%s %d%s", deduced_actual, deviation, late?"L":"E"); strcpy(analysis_class, "summ-table-good"); }
      else if(deviation < 6) { sprintf(analysis_text, "%s %d%s", deduced_actual, deviation, late?"L":"E"); strcpy(analysis_class, "summ-table-minor"); }
      else                   { sprintf(analysis_text, "%s %d%s", deduced_actual, deviation, late?"L":"E"); strcpy(analysis_class, "summ-table-major"); }
      sprintf(mobile_analysis, "%d%s", deviation, late?"L":"E");
      if(deviation >= 3) nlate++;
      mobile_act = mobile_sched + (late?deviation:(-deviation));
      break;

    }

   if(deduced) ndeduced++;
   if((status == Arrived && !calls[index].terminates) || status == DepartedDeduced) narrival++;

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
      if(deduced || (status == Arrived && !calls[index].terminates) || status == DepartedDeduced)
      {
         printf("&nbsp;<span class=\"symbols\">");
         if(deduced) printf("&loz;");
         if(status == Arrived && !calls[index].terminates) printf("&alpha;");
         if(status == DepartedDeduced) printf("&epsilon;");
         printf("</span>");
      }
      printf("\n");
      break;

   case SUMMARY:
      printf("<tr id=\"tr%d%d\" class=\"%s\"><td>%s</td><td>%s %s</td><td>%s</td><td id=\"tr%d%dr\" class=\"%s\">%s", row/rows, row%rows, row_class, headcode, train_time_prefix, train_time, train_details, row/rows, row%rows, analysis_class, analysis_text);
      
      // Symbols
      if(deduced || (status == Arrived && !calls[index].terminates) || status == DepartedDeduced)
      {
         printf("&nbsp;<span class=\"symbols\">");
         if(deduced) printf("&loz;");
         if(status == Arrived && !calls[index].terminates) printf("&alpha;");
         if(status == DepartedDeduced) printf("&epsilon;");
         printf("</span>");
      }
      printf("</td></tr>\n");
      break;

   case DEPART:
      printf("<tr id=\"tr%d%d\" class=\"%s\"><td>%s</td><td>%s</td><td>%s</td><td id=\"tr%d%dr\" class=\"%s\">%s", row/rows, row%rows, row_class, headcode, train_time, train_details, row/rows, row%rows, analysis_class, analysis_text);
      
      // Symbols
      if(deduced || (status == Arrived && !calls[index].terminates) || status == DepartedDeduced)
      {
         printf("&nbsp;<span class=\"symbols\">");
         if(deduced) printf("&loz;");
         if(status == Arrived && !calls[index].terminates) printf("&alpha;");
         if(status == DepartedDeduced) printf("&epsilon;");
         printf("</span>");
      }
      printf("</td></tr>\n");
      break;

   case PANEL:
      printf("<tr id=\"tr%d%d\" class=\"%s\"><td>%s</td><td>%s</td><td>%s</td><td id=\"tr%d%dr\" class=\"%s\">%s", row/rows, row%rows, row_class, headcode, train_time, train_details, row/rows, row%rows, analysis_class, analysis_text);
      
      // Symbols
      if(deduced || (status == Arrived && !calls[index].terminates) || status == DepartedDeduced)
      {
         printf("&nbsp;<span class=\"symbols\">");
         if(deduced) printf("&loz;");
         if(status == Arrived && !calls[index].terminates) printf("&alpha;");
         if(status == DepartedDeduced) printf("&epsilon;");
         printf("</span>");
      }
      printf("</td></tr>\n");
      break;

   case MOBILE:
      if(shown < mobile_trains)
      {
         if(mobile_sched > mobile_time || mobile_act > mobile_time)
         {
            printf("%04d %s|%d|%s|%u\n", mobile_sched_unmung, destination, status, mobile_analysis, calls[index].garner_schedule_id);
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
         if(mode == SUMMARY || mode == DEPART)
         {
            printf("<tr class=\"summ-table-idle\"><td>&nbsp;</td><td>&nbsp;</td><td>&nbsp;</td><td>&nbsp;</td></tr>\n");
         }
         if(mode == PANEL)
         {
            printf("<tr class=\"summ-table-idle\"><td>&nbsp;</td><td>&nbsp;</td><td>&nbsp;</td><td>&nbsp;</td></tr>\n");
         }
      }
      if(mode == SUMMARY || mode == DEPART || mode == PANEL) printf("</table></td><td>");
      
      // Last column of outer table - "Key"
      if(modef[mode] & 0x0002) // Update
      {
         printf("tr%d9|summ-table-idle||%d trains.\n", COLUMNS, trains - nbus);
         printf("tr%d10|summ-table-idle|%s|%d not on time.\n", COLUMNS, nlate?"summ-table-minor":"", nlate);
         printf("tr%d11|summ-table-idle|%s|%d cancelled.\n", COLUMNS, ncape?"summ-table-crit":"", ncape);
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
         printf("<tr class=\"summ-table-gone\"><td>Train has passed.</td></tr>");
         printf("<tr class=\"summ-table-idle\"><td>&nbsp;</td></tr>");
         printf("<tr class=\"summ-table-idle\"><td>Deduced activation.&nbsp;&nbsp;<span class=\"symbols\">&loz;</span></td></tr>");
         printf("<tr class=\"summ-table-idle\"><td>Departure not reported.&nbsp;&nbsp;<span class=\"symbols\">&alpha;</span></td></tr>");
         printf("<tr class=\"summ-table-idle\"><td>Time estimated.&nbsp;&nbsp;<span class=\"symbols\">&epsilon;</span></td></tr>");
         printf("<tr class=\"summ-table-idle\"><td>&nbsp;</td></tr>");
         printf("<tr class=\"summ-table-head\"><th>Statistics</th></tr>\n");
         printf("<tr id=\"tr%d9\" class=\"summ-table-idle\"><td id=\"tr%d9r\">%d trains.</td></tr>", COLUMNS, COLUMNS, trains - nbus);
         printf("<tr id=\"tr%d10\" class=\"summ-table-idle\"><td id=\"tr%d10r\"%s>%d not on time.</td></tr>", COLUMNS, COLUMNS, nlate?" class=\"summ-table-minor\"":"", nlate);
         printf("<tr id=\"tr%d11\" class=\"summ-table-idle\"><td id=\"tr%d11r\"%s>%d cancelled.</td></tr>", COLUMNS, COLUMNS, ncape?" class=\"summ-table-crit\"":"", ncape);
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
   char headcode[8];
   char uid[8];

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

   sprintf(query, "SELECT tiploc_code, departure FROM cif_schedule_locations WHERE record_identity = 'LO' AND cif_schedule_id = %u", schedule_id);
   if(!db_query(query))
   {
      result0 = db_store_result();
      if((row0 = mysql_fetch_row(result0)))
      {
         sprintf(train, "%s %s to ", show_time(row0[1]), location_name(row0[0], true));
      }
      mysql_free_result(result0);
   }
   sprintf(query, "SELECT tiploc_code FROM cif_schedule_locations WHERE record_identity = 'LT' AND cif_schedule_id = %u", schedule_id);
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
   sprintf(zs, " FROM cif_schedules WHERE id = %u", schedule_id);
   strcat(query, zs);

   if(!db_query(query))
   {
      result0 = db_store_result();
      if(!(row0 = mysql_fetch_row(result0)))
      {
         printf("<h3>Schedule %u not found.</h3>\n", schedule_id);
         mysql_free_result(result0);
         return;
      }
      else
      {
         strcpy(headcode, row0[8]);
         if(!headcode[0]) strcpy(headcode, row0[34]);
         printf("<h2>Train %u (%s) %s %s</h2>\n", schedule_id, show_spaces(row0[3]), headcode, train);
         strcpy(uid, row0[3]);
         printf("<table><tr><td style=\"vertical-align:top;\">"); // Outer table
         printf("<table class=\"train-table\">");
         printf("<tr><td>Update ID</td><td>%s</td><td>", row0[0]);
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
                  printf(" %s", time_text(atol(row1[0]), true));
               }
               mysql_free_result(result1);
            }
         }
         printf("</td></tr>\n");

         printf("<tr><td>Created</td><td></td><td>%s</td></tr>", time_text(atol(row0[32]), true));

         if(strtoul(row0[33], NULL, 10) < 0xffffffff)
         {
            strcpy(zs, time_text(atol(row0[33]), true));
         }
         else
         {
            zs[0] = '\0';
         }
         printf("<tr><td>Deleted</td><td></td><td>%s</td></tr>", zs);

         printf("<tr><td>Bank holiday running</td><td>%s</td><td>", row0[1]);
         switch(row0[1][0])
         {
         case 'X' : printf("Not on bank holidays."); break;
         case 'G': printf("Not on Glasgow bank holidays."); break;
         default: printf("%s", row0[1]);
         }
         printf("</td></tr>\n");

         printf("<tr><td>STP Indicator</td><td>%s</td>", row0[2]);
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

         printf("<tr><td>Train UID</td><td>%s</td><td></td></tr>\n", show_spaces(row0[3]));

         // printf("<tr><td>Applicable Timetable</td><td>%s</td>", row0[4]);
         printf("<tr><td>ATOC code</td><td>%s</td><td></td></tr>", row0[5]);
         //printf("<tr><td>Traction Class</td><td>%s</td>", row0[6]);
         printf("<tr><td>UIC code</td><td>%s</td><td></td></tr>", row0[6]);
         printf("<tr><td>Days runs</td><td colspan=2>");
         if(row0[25][0] == '1') printf("Mo ");
         if(row0[26][0] == '1') printf("Tu ");
         if(row0[27][0] == '1') printf("We ");
         if(row0[28][0] == '1') printf("Th ");
         if(row0[29][0] == '1') printf("Fr ");
         if(row0[30][0] == '1') printf("Sa ");
         if(row0[31][0] == '1') printf("Su ");
         printf("</td></tr>\n");
         printf("<tr><td>Schedule Dates</td><td colspan=2>%s", date_text(atol(row0[23]), 0));
         printf(" - %s</td></tr>", date_text(atol(row0[7]), 0));
         printf("<tr><td>Signalling ID</td><td>%s</td><td></td></tr>", row0[8]);
         printf("<tr><td>Deduced headcode</td><td>%s</td><td>", row0[34]);
         if(row0[35][0]) printf("(Status %s)", row0[35]);
         printf("</td></tr>");
         printf("<tr><td>Train category</td>");
         word i;
         for(i=0; i < CATEGORIES && (categories[i][0]!=row0[9][0] || categories[i][1]!=row0[9][1]) ; i++);
         if(i < CATEGORIES)
            printf("<td>%s</td><td>%s</td></tr>", row0[9], categories[i]+2);
         else if(row0[9][0])
            printf("<td>%s</td><td>Unrecognised</td></tr>", row0[9]);
         else
            printf("<td>%s</td><td></td></tr>", row0[9]);

         printf("<tr><td>TOC Headcode</td><td>%s</td><td></td></tr>\n", row0[10]);
         // "Not used" printf("<tr><td>CIF course indicator</td><td>%s</td>", row0[13]);
         printf("<tr><td>Train service code</td><td>%s</td><td></td></tr>\n", row0[11]);
         printf("<tr><td>Business Sector</td><td>%s</td><td></td></tr>\n", row0[12]);

         printf("<tr><td>Power type</td><td>%s</td><td>\n", row0[13]);
         sprintf(zs, "%-3s", row0[13]);
         for(i=0; i < POWER_TYPES && strncmp(power_types[i], zs, 3) ; i++);
         if(i < POWER_TYPES)
            printf("%s", power_types[i]+3);
         else if(row0[9][0])
            printf("Unrecognised");
         printf("</td></tr>");

           printf("<tr><td>Timing Load</td><td>%s</td><td>", row0[14]);
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
         printf("<tr><td>Speed</td>");
         if(row0[15][0])
            printf("<td>%s</td><td>mph</td>", row0[15]);
         else
            printf("<td>&nbsp;</td><td>&nbsp;</td>");
         printf("<tr><td>Operating Characteristics</td><td>%s</td><td>", row0[16]);
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

         printf("<tr><td>Seating Class</td><td>%s</td><td>", row0[17]);
         switch(row0[17][0])
         {
         case 0:   printf("&nbsp;"); break;
         case 'B': printf("First and Standard."); break;
         case ' ': printf("First and Standard."); break;
         case 'S': printf("Standard only."); break;
         default:  printf("Unrecognised"); break;
         }
         printf("</td></tr>\n");

         printf("<tr><td>Sleepers</td><td>%s</td><td>", row0[18]);
         switch(row0[18][0])
         {
         case 0:   printf("&nbsp;"); break;
         case 'B': printf("First and Standard."); break;
         case 'F': printf("First only."); break;
         case 'S': printf("Standard only."); break;
         default:  printf("Unrecognised"); break;
         }
         printf("</td></tr>\n");

         printf("<tr><td>Reservations</td><td>%s</td><td>", row0[19]);
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
         printf("<tr><td>Connection Indicator</td><td></td><td>%s</td></tr>\n", row0[20]);
         printf("<tr><td>Catering Code</td><td></td><td>%s</td></tr>\n", row0[21]);
         printf("<tr><td>Service Branding</td><td></td><td>%s</td></tr>\n", row0[22]);
         printf("<tr><td>Train Status</td><td>%s</td>", row0[24]);
         switch(row0[24][0])
         {
         case 'B': printf("<td>Bus</td>");          break;
         case 'F': printf("<td>Freight</td>");      break;
         case 'P': printf("<td>Passenger</td>");    break;
         case 'S': printf("<td>Ship</td>");    break;
         case 'T': printf("<td>Trip</td>");         break;
         case '1': printf("<td>STP passenger</td>");break;
         case '2': printf("<td>STP freight</td>");  break;
         case '3': printf("<td>STP trip</td>");     break;
         case '4': printf("<td>STP ship</td>");     break;
         case '5': printf("<td>STP bus</td>");      break;
         default:  printf("<td>Unrecognised</td>"); break;
         }
         printf("</tr></table>\n");


         // printf("</td><td width=\"5%%\">&nbsp;</td><td style=\"vertical-align:top;\">\n"); // Outer table

         //                      0              1                2            3                4        5          6      7               8                9        10    11    12                       13                 14                   15 
         sprintf(query, "SELECT location_type, record_identity, tiploc_code, tiploc_instance, arrival, departure, pass, public_arrival, public_departure, platform, line, path, engineering_allowance, pathing_allowance, performance_allowance, location_type FROM cif_schedule_locations WHERE cif_schedule_id = %u ORDER BY next_day,sort_time", schedule_id);

         if(!db_query(query))
         {
            printf("<table class=\"sched-table\">");
            printf("<tr><th>Location</th><th>Times WTT(Public)</th><th>Plat</th><th>Path-Line</th><th>Allowances</th><th>Activities</th>\n");
            result0 = db_store_result();
            while((row0 = mysql_fetch_row(result0)))
            {
               printf("<tr>");
               
               printf("<td>%s", location_name_link(row0[2], false, "sum", when));
               if(row0[3] && row0[3][0] && row0[3][0] != ' ')
               {
                  printf(" [%s]", row0[3]);
               }
               printf("</td><td>");
               if(row0[4][0]) printf("a. %s", show_time(row0[4])); // arrival
               if(row0[7][0]) printf("(%s)",  show_time(row0[7])); // public arrival
               if(row0[5][0]) printf(" d. %s",show_time(row0[5])); // dep
               if(row0[8][0]) printf("(%s)",  show_time(row0[8])); // public dep
               if(row0[6][0]) printf("p. %s", show_time(row0[6])); // pass
               printf("</td>\n");

               printf("<td>%s</td>", row0[9]); // platform
               if(row0[11][0] || row0[10][0])
                  printf("<td>%s - %s</td>", row0[11], row0[10]); // path line
               else
                  printf("<td></td>");

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

      // Association(s)         0         1         2         3              4                5                 6               7           8            9           10          11                     12                   13           14
      sprintf(query, "SELECT update_id, created, deleted, main_train_uid, assoc_train_uid, assoc_start_date, assoc_end_date, assoc_days, category, date_indicator, location, base_location_suffix, assoc_location_suffix, diagram_type, CIF_stp_indicator FROM cif_associations WHERE main_train_uid = '%s' OR assoc_train_uid = '%s'", uid, uid);

      if(!db_query(query))
      {
         word header = false;
         char deleted[32], assoc[32], this_uid[32];
         result0 = db_store_result();
         while((row0 = mysql_fetch_row(result0)))
         {
            if(!header)
            {
               header = true;
               printf("<h3>Associations:</h3>\n");
               printf("<table class=\"train-table-small\">");
               printf("<tr><th>Update id</th><th>Created</th><th>Deleted</th><th>UID</th><th>Assoc UID</th>");
               printf("<th>Runs on</th><th>From</th><th>To</th>");
               printf("<th>Category</th><th>Type</th><th>Location</th><th>STP</th>");
               printf("</tr>\n");
            }
            if(strtoul(row0[2], NULL, 10) < 0xffffffff)
            {
               strcpy(deleted, time_text(atol(row0[2]), true));
            }
            else
            {
               deleted[0] = '\0';
            }
            if(!strcmp(row0[3], uid)) sprintf(this_uid, "<b>%s</b>", show_spaces(row0[3]));
            else strcpy(this_uid, show_spaces(row0[3]));
            if(!strcmp(row0[4], uid)) sprintf(assoc, "<b>%s</b>", show_spaces(row0[4]));
            else strcpy(assoc, show_spaces(row0[4]));
            printf("<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td>", row0[0], time_text(atol(row0[1]), true), deleted, this_uid, assoc);

            printf("<td>");
            if(row0[7][0] == '1') printf("Mo");
            if(row0[7][1] == '1') printf("Tu");
            if(row0[7][2] == '1') printf("We");
            if(row0[7][3] == '1') printf("Th");
            if(row0[7][4] == '1') printf("Fr");
            if(row0[7][5] == '1') printf("Sa");
            if(row0[7][6] == '1') printf("Su");
            printf("</td><td>%s", show_date(atol(row0[5]), false));
            printf("</td><td>%s</td>", show_date(atol(row0[6]), false));

            printf("<td>"); 
            switch(row0[8][0])
            {
            case 'J': printf("Join");          break;
            case 'V': printf("Divide");        break;
            case 'N': printf("Next/Prev");     break;
            case   0:                          break;
            default:  printf("[%s]", row0[8]); break;
            }
            printf("</td><td>");
            switch(row0[13][0])
            {
            case 'P': printf("Passenger use");  break;
            case 'O': printf("Operating use");  break;
            case   0:                           break;
            default:  printf("[%s]", row0[13]); break;
            }
            printf("</td><td>%s</td><td>", location_name(row0[10], true));
            switch(row0[14][0])
            {
            case 'P': printf("Permanent"); break;
            case 'N': printf("New");       break;
            case 'O': printf("Overlay");   break;
            case 'C': printf("Cancelled"); break;
            case   0:                      break;
            default:  printf("[%s]", row0[14]); break;
            }
            printf("</td></tr>\n");
         }
         if(header)
         {
            printf("</table><br>\n");
         }
      }

      // CR Records              0            1                  2                 3               4             5                      6                  7           8                 9                           10                11              12               13                    14                     15               16
      sprintf(query, "SELECT tiploc_code, tiploc_instance, CIF_train_category, signalling_id, CIF_headcode, CIF_train_service_code, CIF_power_type, CIF_timing_load, CIF_speed, CIF_operating_characteristics, CIF_train_class, CIF_sleepers, CIF_reservations, CIF_connection_indicator, CIF_catering_code, CIF_service_branding, uic_code FROM cif_changes_en_route WHERE cif_schedule_id = %u", schedule_id);
      if(!db_query(query))
      {
         result0 = db_store_result();
         while((row0 = mysql_fetch_row(result0)))
         {
            printf("<h3>Change en route at %s", location_name(row0[0], true));
            if(row0[1][0]) printf("[%s]", row0[1]);
            printf("</h3>\n");

            printf("<table class=\"train-table\">");

            printf("<tr><td>UIC code</td><td>%s</td><td></td></tr>", row0[16]);

            printf("<tr><td>Signalling ID</td><td>%s</td><td></td></tr>", row0[3]);

            printf("<tr><td>Train category</td>");
            word i;
            for(i=0; i < CATEGORIES && (categories[i][0]!=row0[2][0] || categories[i][1]!=row0[2][1]) ; i++);
            if(i < CATEGORIES)
               printf("<td>%s</td><td>%s</td></tr>", row0[2], categories[i]+2);
            else if(row0[2][0])
               printf("<td>%s</td><td>Unrecognised</td></tr>", row0[2]);
            else
               printf("<td>%s</td><td></td></tr>", row0[2]);

            printf("<tr><td>TOC Headcode</td><td>%s</td><td></td></tr>\n", row0[16]);

            printf("<tr><td>Train service code</td><td>%s</td><td></td></tr>\n", row0[5]);

            printf("<tr><td>Power type</td><td>%s</td><td>\n", row0[6]);
            sprintf(zs, "%-3s", row0[6]);
            for(i=0; i < POWER_TYPES && strncmp(power_types[i], zs, 3) ; i++);
            if(i < POWER_TYPES)
               printf("%s", power_types[i]+3);
            else if(row0[9][0])
               printf("Unrecognised");
            printf("</td></tr>");

            printf("<tr><td>Timing Load</td><td>%s</td><td>", row0[7]);
            if(!row0[7][1]) switch(row0[7][0])
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

            printf("<tr><td>Speed</td>");
            if(row0[8][0])
               printf("<td>%s</td><td>mph</td>", row0[8]);
            else
               printf("<td>&nbsp;</td><td>&nbsp;</td>");

            printf("<tr><td>Operating Characteristics</td><td>%s</td><td>", row0[9]);
            for(i=0; i<6 && row0[9][i]; i++)
            {
               if(i) printf("<br>");
               switch(row0[9][i])
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

            printf("<tr><td>Seating Class</td><td>%s</td><td>", row0[10]);
            switch(row0[10][0])
            {
            case 0:   printf("&nbsp;"); break;
            case 'B': printf("First and Standard."); break;
            case ' ': printf("First and Standard."); break;
            case 'S': printf("Standard only."); break;
            default:  printf("Unrecognised"); break;
            }
            printf("</td></tr>\n");

            printf("<tr><td>Sleepers</td><td>%s</td><td>", row0[11]);
            switch(row0[11][0])
            {
            case 0:   printf("&nbsp;"); break;
            case 'B': printf("First and Standard."); break;
            case 'F': printf("First only."); break;
            case 'S': printf("Standard only."); break;
            default:  printf("Unrecognised"); break;
            }
            printf("</td></tr>\n");

            printf("<tr><td>Reservations</td><td>%s</td><td>", row0[12]);
            switch(row0[12][0])
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

            printf("<tr><td>Connection Indicator</td><td></td><td>%s</td></tr>\n", row0[13]);
            
            printf("<tr><td>Catering Code</td><td></td><td>%s</td></tr>\n", row0[14]);

            printf("<tr><td>Service Branding</td><td></td><td>%s</td></tr>\n", row0[15]);

            printf("</tr></table>\n");
         }
         mysql_free_result(result0);
      }

      {
         struct tm * broken;
         broken = localtime(&when);
         printf("<a class=\"linkbutton-cp\" href=\"%strain_text/%u/%02d/%02d/%02d\" target=\"_blank\">Display in plain text</a>\n", URL_BASE, schedule_id, broken->tm_mday, broken->tm_mon + 1, broken->tm_year%100);
      }
      printf("</td><td width=\"5%%\">&nbsp;</td><td style=\"vertical-align:top;\">\n"); // Outer table

      // TRUST
      char trust_id[16];
      trust_id[0] = '\0';
      if(when)
      {
         struct tm * broken = gmtime(&when);
         printf("<table width=\"900px\"><tr>\n");
         printf("<td>Real Time Data For %s %02d/%02d/%02d</td>\n", days[broken->tm_wday % 7], broken->tm_mday, broken->tm_mon + 1, broken->tm_year % 100);
         printf("<td width = \"10%%\">&nbsp;</td>\n");
         printf("<td class=\"control-panel-row\"> Show real time data for date ");
         printf("<input type=\"text\" id=\"train_date\" size=\"8\" maxlength=\"8\" placeholder=\"dd/mm/yy\" value=\"\" onkeydown=\"if(event.keyCode == 13) train_date_onclick(%u); else ar_off();\">\n", schedule_id);
         printf(" <button id=\"search\" class=\"cp-button\" onclick=\"train_date_onclick(%u);\">Show</button> </td>\n", schedule_id);
         printf("</tr></table>\n");
         printf("<table width=\"100%%\">");

         char query[512];
         MYSQL_RES * result2;
         MYSQL_ROW row2;
         word movement_header_done = false;
         const char * const nm_header = "<tr class=\"small-table\"><th>&nbsp;&nbsp;Received&nbsp;&nbsp;</th><th>Event</th><th colspan=11>Data</th></tr>\n";
         // const char * const movement_header = "<tr class=\"small-table\"><th>&nbsp;&nbsp;Received&nbsp;&nbsp;</th><th>Event</th><th>&nbsp;&nbsp;&nbsp;&nbsp;Source&nbsp;&nbsp;&nbsp;&nbsp;</th><th>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Location&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</th><th>P</th><th>Event Type</th><th>Planned Type</th><th>WTT</th><th>GBTT</th><th>Actual</th><th>&nbsp;&nbsp;Var&nbsp;&nbsp;</th><th>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Next Report (Run Time)&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</th><th>Flags</th></tr>\n";
         const char * const movement_header = "<tr class=\"small-table\"><th>Received</th><th>Event</th><th>Source</th><th>Location</th><th>P</th><th>Event Type</th><th>Planned Type</th><th>WTT</th><th>GBTT</th><th>Actual</th><th>Var</th><th>Next Report (Run Time)</th><th>Flags</th></tr>\n";

         // Calculate dom from when, deducting 24h if next_day and/or adding 24h if after 00:00.
         // time_t start_date = when - (next_day?(24*60*60):0);
         // if(sort_time < DAY_START) start_date += (24*60*60);
         // 
         // NO NO NO.  The link to get here MUST have the start date of the train, NOT the date at the point clicked.
         // So we DON'T need to fiddle here.
         time_t start_date = when;

         byte dom = broken->tm_mday;

         // Buffer for non-movement reports
#define MAX_NM 32
         char nm[MAX_NM][512];
         time_t nmt[MAX_NM];
         word next_nm = 0;

         // Activations
         // Only accept activations where dom matches, and are +- 15 days (To eliminate last months activation.)  YUK
         sprintf(query, "SELECT created, trust_id, deduced FROM trust_activation WHERE cif_schedule_id = %u AND substring(trust_id FROM 9) = '%02d' AND created > %ld AND created < %ld order by created", schedule_id, dom, start_date - 15*24*60*60, start_date + 15*24*60*60);
         if(!db_query(query))
         {
            result1 = db_store_result();
            while((row1 = mysql_fetch_row(result1)) && next_nm < MAX_NM)
            {
               nmt[next_nm] = atol(row1[0]);
               sprintf(nm[next_nm++], "<tr class=\"small-table-act\"><td>%s</td><td>Activated</td><td colspan=11>%sTrain ID = \"%s\"</td></tr>\n", time_text(atol(row1[0]), true), (atoi(row1[2]))?"[DEDUCED] ":"", row1[1]);
               sprintf(query, "SELECT created, reason, type, reinstate, loc_stanox from trust_cancellation where trust_id='%s' AND created > %ld AND created < %ld order by created ", row1[1], start_date - 15*24*60*60, start_date + 15*24*60*60);
               strcpy(trust_id, row1[1]);

               // Activation Extra
               {
                  char q[512];
                  sprintf(q, "SELECT * FROM trust_activation_extra WHERE created = %s AND trust_id = '%s'", row1[0], row1[1]);
                  if(!db_query(q))
                  {
                     result2 = db_store_result();
                     if((row2 = mysql_fetch_row(result2)) && next_nm < MAX_NM)
                     {
                        nmt[next_nm] = atol(row1[0]);
                        sprintf(nm[next_nm++], "<tr class=\"small-table-act\"><td>&nbsp;</td><td></td><td colspan=11>Schedule source \"%s\", Train File Address \"%s\", TP Origin Timestamp %s, TP Origin %s</td></tr>\n", row2[2], row2[3], date_text(atol(row2[5]), true), show_stanox(row2[7]));
                        nmt[next_nm] = atol(row1[0]);
                        sprintf(nm[next_nm++], "<tr class=\"small-table-act\"><td>&nbsp;</td><td></td><td colspan=11>Origin dep time %s, TSC \"%s\", call type %s mode %s, WTT ID \"%s\", Sched Origin %s</td></tr>\n", time_text(atol(row2[8]), true), row2[9], row2[12], row2[14], row2[17], show_stanox(row2[16]));
                     }
                     mysql_free_result(result2);
                  }
               }
            }
            mysql_free_result(result1);
               
            if(!db_query(query))
            {
               result2 = db_store_result();
               while((row2 = mysql_fetch_row(result2)) && next_nm < MAX_NM)
               {
                  nmt[next_nm] = atol(row2[0]);
                  if(atoi(row2[3]))
                  {
                     sprintf(nm[next_nm++], "<tr class=\"small-table-act\"><td>%s</td><td>Reinstated</td><td colspan=11>At %s.</td></tr>\n", time_text(atol(row2[0]), true), show_stanox(row2[4]));
                  }
                  else
                  {
                     sprintf(nm[next_nm++], "<tr class=\"small-table-cape\"><td>%s</td><td>Cancelled</td><td colspan=11>At %s.  Reason %s %s</td></tr>\n", time_text(atol(row2[0]), true), show_stanox(row2[4]), show_cape_reason(row2[1]), row2[2]);
                  }
               }
               mysql_free_result(result2);
            }

            // "Other" messages
            sprintf(query, "SELECT created, reason, loc_stanox FROM trust_changeorigin WHERE trust_id='%s' AND created > %ld AND created < %ld ORDER BY created", trust_id, start_date - 15*24*60*60, start_date + 15*24*60*60);
            if(!db_query(query))
            {
               result2 = db_store_result();
               while((row2 = mysql_fetch_row(result2)) && next_nm < MAX_NM)
               {
                  nmt[next_nm] = atol(row2[0]);
                  sprintf(nm[next_nm++], "<tr class=\"small-table-other\"><td>%s</td><td>Change Origin</td><td colspan=11>Reason: %s  Location: %s</td></tr>\n", time_text(atol(row2[0]), true), show_cape_reason(row2[1]), show_stanox(row2[2]));
               }
               mysql_free_result(result2);
            } 
            sprintf(query, "SELECT created, original_stanox, stanox FROM trust_changelocation WHERE trust_id='%s' AND created > %ld AND created < %ld ORDER BY created", trust_id, start_date - 15*24*60*60, start_date + 15*24*60*60);
            if(!db_query(query))
            {
               result2 = db_store_result();
               while((row2 = mysql_fetch_row(result2)) && next_nm < MAX_NM)
               {
                  strcpy(query, show_stanox(row2[1]));
                  nmt[next_nm] = atol(row2[0]);
                  sprintf(nm[next_nm++], "<tr class=\"small-table-other\"><td>%s</td><td>Change Location</td><td colspan=11>From: %s  To: %s</td></tr>\n", time_text(atol(row2[0]), true), query, show_stanox(row2[2]));
               }
               mysql_free_result(result2);
            } 
            sprintf(query, "SELECT created, trust_id, new_trust_id FROM trust_changeid WHERE (trust_id='%s' OR new_trust_id = '%s') AND created > %ld AND created < %ld ORDER BY created", trust_id, trust_id, start_date - 15*24*60*60, start_date + 15*24*60*60);
            if(!db_query(query))
            {
               result2 = db_store_result();
               while((row2 = mysql_fetch_row(result2)) && next_nm < MAX_NM)
               {
                  nmt[next_nm] = atol(row2[0]);
                  sprintf(nm[next_nm++], "<tr class=\"small-table-other\"><td>%s</td><td>Change ID</td><td colspan=11>Old ID: %s  New ID: %s</td></tr>\n", time_text(atol(row2[0]), true), row2[1], row2[2]);
               }
               mysql_free_result(result2);
            } 

            if(next_nm) 
            {
               printf(nm_header);
               word i, j;
               time_t latest = 0;
               time_t earliest;
               for(i = 0; i < next_nm; i++) if(nmt[i] > latest) latest = nmt[i];
               latest++;
               do
               {
                  earliest = latest;
                  for(i = j = 0; i < next_nm; i++) { if(nmt[i] < earliest) { earliest = nmt[i]; j = i; }}
                  if(earliest < latest) 
                  {
                     printf("%s", nm[j]);
                     nmt[j] = latest;
                  }
               }
               while(earliest < latest);
            }

            // Movements
            //                       0       1           2                   3         4                  5                 6                   7                   8                   9     
            sprintf(query, "SELECT created, platform, loc_stanox, actual_timestamp, gbtt_timestamp, planned_timestamp, timetable_variation, next_report_stanox, next_report_run_time, flags from trust_movement where trust_id='%s' AND created > %ld AND created < %ld order by actual_timestamp, planned_timestamp, created", trust_id, start_date - 15*24*60*60, start_date + 15*24*60*60);
            if(!db_query(query))
            {
               result2 = db_store_result();
               while((row2 = mysql_fetch_row(result2)))
               {
               if(!movement_header_done)
               {
                  printf("%s", movement_header);
                  movement_header_done = true;
               }
		 word flags = atoi(row2[9]);
                  printf("<tr class=\"small-table\"><td>%s</td><td>Movement</td>", time_text(atol(row2[0]), true));
                  printf("<td>%s</td>", (flags & 0x0004) ? "Man.":"Auto.");
                  printf("<td>%s</td>", show_stanox_link(row2[2]));
                  printf("<td>%s</td>", row2[1]); // Platform
		  switch(flags & 0x0003)
		     {
		     case 0: printf("<td>ERROR</td><td>ERROR</td>"); break;
                     case 1: printf("<td>Dep.</td><td>Dep.</td>"); break;
                     case 2: printf("<td>Arr.</td><td>Arr.</td>"); break;
                     case 3: printf("<td>Arr.</td><td>Dest.</td>"); break;
		     }
                  printf("<td>%s</td>", show_trust_time(row2[5], true));
                  printf("<td>%s</td>", show_trust_time(row2[4], true));
                  printf("<td>%s</td>", show_trust_time(row2[3], true));
                  printf("<td>%s ", row2[6]);
		  switch(flags & 0x0018)
		     {
		     case 0x0000: printf("Early</td>"); break;
		     case 0x0008: printf("On time</td>"); break;
		     case 0x0010: printf("Late</td>"); break;
		     case 0x0018: printf("Off route</td>"); break;
		     }
		  
                  if(row2[7][0])
                     printf("<td>%s (%s)</td>", show_stanox(row2[7]), row2[8]);
                  else
                     printf("<td></td>");
                  printf("<td>%s%s%s</td>", (flags & 0x0020)?"Off route. ":"", (flags & 0x0040)?"Terminated. ":"", (flags & 0x0080)?"Correction. ":"");
                  printf("</tr>\n");
               }
               mysql_free_result(result2);
            }
         }
         if(!next_nm && !movement_header_done)
         {
            printf("<p>No real time data recorded.</p>");
         }
      }
      else
      {
         printf("<p>\n");
         printf("<table><tr><td class=\"control-panel-row\"> Show real time data for date ");
         printf("<input type=\"text\" id=\"train_date\" size=\"8\" maxlength=\"8\" placeholder=\"dd/mm/yy\" value=\"\" onkeydown=\"if(event.keyCode == 13) train_date_onclick(%u); else ar_off();\">\n", schedule_id);
         printf(" <button id=\"search\" class=\"cp-button\" onclick=\"train_date_onclick(%u);\">Show</button> \n", schedule_id);
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

   train[0] = '\0';

   sprintf(query, "SELECT tiploc_code, departure FROM cif_schedule_locations WHERE record_identity = 'LO' AND cif_schedule_id = %u", schedule_id);
   if(!db_query(query))
   {
      result0 = db_store_result();
      if((row0 = mysql_fetch_row(result0)))
      {
         sprintf(train, "%s %s to ", show_time(row0[1]), location_name(row0[0], true));
      }
   }
   sprintf(query, "SELECT tiploc_code FROM cif_schedule_locations WHERE record_identity = 'LT' AND cif_schedule_id = %u", schedule_id);
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
   sprintf(zs, " FROM cif_schedules WHERE id = %u", schedule_id);
   strcat(query, zs);

   printf("<pre>\n");
   if(!db_query(query))
   {
      result0 = db_store_result();
      if((row0 = mysql_fetch_row(result0)))
      {
         printf("Train %u (%s) %s %s\n\n", schedule_id, row0[3], row0[8], train);

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
                  printf(" %s", time_text(atol(row1[0]), true));
               }
               mysql_free_result(result1);
            }
         }
         printf("\n");

         printf("                  Created: %s\n", time_text(atol(row0[32]), true));

         if(strtoul(row0[33], NULL, 10) < 0xffffffff)
         {
            strcpy(zs, time_text(atol(row0[33]), true));
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
         case 'S': printf("Ship");    break;
         case 'T': printf("Trip");         break;
         case '1': printf("STP passenger");break;
         case '2': printf("STP freight");  break;
         case '3': printf("STP trip");     break;
         case '4': printf("STP ship");     break;
         case '5': printf("STP bus");      break;
         default:  printf("Unrecognised"); break;
         }
         printf("\n");
         printf("\n");

         //                      0              1                2            3                4        5          6      7               8                9        10    11    12                       13                 14                   15  
         sprintf(query, "SELECT location_type, record_identity, tiploc_code, tiploc_instance, arrival, departure, pass, public_arrival, public_departure, platform, line, path, engineering_allowance, pathing_allowance, performance_allowance, location_type FROM cif_schedule_locations WHERE cif_schedule_id = %u ORDER BY next_day,sort_time", schedule_id);

         if(!db_query(query))
         {
            printf("                          Location   Arr     Dep    P PathLine\n");
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
               
               printf("%3s %3s %3s ", row0[9], row0[11], row0[10]);

               if(row0[12][0]) printf("[%s]", show_time_text(row0[12]));  // Eng
               if(row0[13][0]) printf("(%s)", show_time_text(row0[13]));  // Path
               if(row0[14][0]) printf("{%s}", show_time_text(row0[14]));  // Perf
               printf("%s", show_act_text(row0[15]));

               printf("\n");
            }
         }
         printf("\n[] Engineering allowance.     () Pathing allowance.    {} Performance allowance.\n");
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
      sprintf(result, "%s (%s%s%s)", row0[0], tiploc, row0[1][0]?" ":"", row0[1]);
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
   MYSQL_RES * result0 = NULL;
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
   // Special bodges to make Huyton Panel fit on the screen.
   if(      mode == PANEL && !strcmp(tiploc, "ALERTN"))  strcpy(cache_val[next_cache], "Liverpool South Pwy");
   else if (mode == PANEL && !strcmp(tiploc, "LVRPLSH")) strcpy(cache_val[next_cache], "Liverpool Lime Street");
   else if (mode == PANEL && !strcmp(tiploc, "MNCRIAP")) strcpy(cache_val[next_cache], "Manchester Airport");
   else 
   {
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

   if(stanox[0] != '\0')
   {
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
   }
   else
   {
      strcpy(result, "-");
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
      return location_name_link(row0[0], true, "sum", 0);
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
   static char output[512];
   char in[16], z[8];
   word i, j, hit;

   if(strlen(input) > 14)
   {
      strcpy(output, "ERROR");
      return output;
   }

   strcpy(in, input);

   while (strlen(in) < 12) { strcat(in, " "); }

   output[0] = '\0';

   for(i = 0; i < strlen(in) && in[i] != ' '; i+=2)
   {
      hit = 999;
      for(j = 0; j < ACTIVITIES; j++)
      {
         if(in[i] == activities[j][0] && in[i+1] == activities[j][1])
         {
            hit = j;
         }
      }
      if(i) strcat(output, "<br>");
      if(hit < 999) 
      {
         strcat(output, activities[hit] + 2);
         strcat(output, ".");
      }
      else
      {
         z[0] = in[i];
         z[1] = in[i+1];
         z[2] = '\0';
         strcat(output, z);
      }
   }
   return output;
}

static char * show_act_text(const char * const input)
{
   static char output[512];
   char in[16], z[8];
   word i, j, hit;

   if(strlen(input) > 14)
   {
      strcpy(output, "ERROR");
      return output;
   }

   strcpy(in, input);

   while (strlen(in) < 12) { strcat(in, " "); }

   output[0] = '\0';

   for(i = 0; i < strlen(in) && in[i] != ' '; i+=2)
   {
      hit = 999;
      for(j = 0; j < ACTIVITIES; j++)
      {
         if(in[i] == activities_text[j][0] && in[i+1] == activities_text[j][1])
         {
            hit = j;
         }
      }
      if(i) strcat(output, "\n");
      if(hit < 999) 
      {
         strcat(output, activities_text[hit] + 2);
         strcat(output, ".");
      }
      else
      {
         z[0] = in[i];
         z[1] = in[i+1];
         z[2] = '\0';
         strcat(output, z);
      }
   }
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
         if(age > 31*60*60) status = 2;
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
            sprintf(status_text, "%u per minute.", rate);
         }
      }
   }
   printf("<tr><td class=\"status-text\">Message process rate </td>");
   printf("<td class=\"status%d\"> %s </td>\n", status, status_text);
   printf("</tr>\n");

   // TD feed
   status = 3;
   strcpy(status_text, "Failed to read database.");
   sprintf(q, "SELECT last_timestamp FROM describers ORDER BY last_timestamp DESC");
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
            sprintf(status_text, "%u per minute.", rate);
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
            sprintf(status_text, "%u per minute.", rate);
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
      if(!statfs("/", &fstatus))
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
         if(age > 31*60*60) status = 2;
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
            sprintf(status_text, "(%u per minute.)", rate);
         }
      }
   }
   printf("tr%d26|status%d||%s\n", column, status, status_text);

   // TD feed
   status = 3;
   strcpy(status_text, "DB error.");
   sprintf(q, "SELECT last_timestamp FROM describers ORDER BY last_timestamp DESC");
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
            sprintf(status_text, "(%u per minute.)", rate);
         }
      }
   }
   printf("tr%d29|status%d||%s\n", column, status, status_text);

   // Disc space
   status = 3;
   strcpy(status_text, "Error.");
   {
      struct statfs fstatus;
      if(!statfs("/", &fstatus))
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

static void display_jiancha(void)
{
   _log(PROC, "display_jiancha()");
   word jiancha = 0;

   if(db_query("SELECT time FROM updates_processed ORDER BY time DESC LIMIT 1"))
   {
      jiancha = 1;
   }

   printf("jiancha:%d\n", jiancha);
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
   static char reason[270];
   char query[128];
   MYSQL_RES * result;
   MYSQL_ROW row;

   strcpy(reason, code);

   if(strlen(code) == 0) 
   {
      strcpy(reason, "Unspecified");
      return reason;
   }

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
