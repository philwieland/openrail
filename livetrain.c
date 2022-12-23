/*
    Copyright (C) 2022 Phil Wieland

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
#include <ctype.h>

#include "misc.h"
#include "db.h"
#include "build.h"

#define NAME "livetrain"

#ifndef RELEASE_BUILD
#define BUILD "3c20p"
#else
#define BUILD RELEASE_BUILD
#endif

#define TEMP_TABLE 1

static void display_control_panel(const char * const location, const time_t when);
static char * show_date(const time_t time, const byte local);
static void train(void);
static void show_running(const time_t when, const word activated);
static char * show_tiploc_link(const char * const tiploc, const word use_cache, const char * const view, const time_t when);
static char * location_name(const char * const tiploc, const word use_cache);
static char * show_stanox(const char * const stanox);
static char * show_act(const char * const input);
static char * show_trust_time(const char * const ms, const word local);
static char * trust_to_cif_time(const char * const s);
static char * show_expected_time(const char * const scheduled, const word deviation, const word late);
static const char * const show_cape_reason(const char * const code);

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

#define ACTIVITIES 29
static const char * activities[ACTIVITIES] = {
   "A Stops or shunts for other trains to pass",
   "AEAttach/detach assisting locomotive",
   "BLStops for banking locomotive",
   "C Stops to change trainmen",
   "D Stops to set down passengers",
   "-DStops to detach vehicles",
   "E Stops for examination",
   "H Notional activity for WTT columns",
   "HHNotional activity for WTT columns",
   "K Passenger count point",
   "KE Ticket examination point",
   "L Stops to change locomotives",
   "N Stop not advertised",
   "OPStops for other operating reasons",
   "ORTrain locomotive on rear",
   "PRPropelling between points shown",
   "R Stops when required",
   "RMReversing movement or driver changes ends",
   "RRStops for locomotive to run round",
   "S Stops for railway personnel only",
   // "T Stops to take up and set down passengers",
   "T Take up and set down passengers",
   "-TStops to attach and detach vehicles",
   "TBTrain begins",
   "TFTrain finishes",
   "TWStops (or at pass) for tablet, staff or token",
   "U Stops to take up passengers",
   "-UStops to attach vehicles",
   "W Stops for watering of coaches",
   // "X Passes another train at crossing point on single line",
   "X Passes another train at crossing point",
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

static word next_row_id;

static const char * const create_temp_table = "CREATE TEMPORARY TABLE train"
//static const char * const create_temp_table = "CREATE TABLE train"
   "("
   "id                            INT UNSIGNED NOT NULL AUTO_INCREMENT, "
   "activities                    CHAR(12), " // MISNAMED!  contains activity
   "record_identity               CHAR(2), " // LO, LI, LT, etc
   "tiploc_code                   CHAR(7), "
   "tiploc_instance               CHAR(1), "
   "arrival                       CHAR(5), "
   "departure                     CHAR(5), "
   "pass                          CHAR(5), "
   "public_arrival                CHAR(4), "
   "public_departure              CHAR(4), "
   "sort_time                     SMALLINT UNSIGNED, "
   "next_day                      BOOLEAN, "
   "splatform                     CHAR(3), "
   "line                          CHAR(3), "
   "path                          CHAR(3), "
   "engineering_allowance         CHAR(2), "
   "pathing_allowance             CHAR(2), "
   "performance_allowance         CHAR(2), "
   "a_tplatform                     VARCHAR(8), "
   "d_tplatform                     VARCHAR(8), "
   //"loc_stanox                    VARCHAR(8), "
   "a_actual_timestamp              INT UNSIGNED, "
   "d_actual_timestamp              INT UNSIGNED, "
   "a_timetable_variation           SMALLINT UNSIGNED, "
   "d_timetable_variation           SMALLINT UNSIGNED, "
   "a_flags                         SMALLINT UNSIGNED, "
   "d_flags                         SMALLINT UNSIGNED, "
   "cancelled_here                  SMALLINT UNSIGNED, "
   "PRIMARY KEY(id)"
   ")";

enum columns { id, act, record_identity, tiploc_code, tiploc_instance, arrival, departure, pass, public_arrival, public_departure, sort_time, next_day, splatform, line, path, engineering_allowance, pathing_allowance, performance_allowance, a_tplatform, d_tplatform, a_actual_timestamp, d_actual_timestamp,a_timetable_variation, d_timetable_variation, a_flags, d_flags, cancelled_here, MAX_COLUMN };


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
int main()
{
   char zs[1024];

   now = time(NULL);

   struct timeval ha_clock;
   gettimeofday(&ha_clock, NULL);
   qword start_time = ha_clock.tv_sec;
   start_time = start_time * 1000 + ha_clock.tv_usec / 1000;


   char * parms = getenv("REQUEST_URI");

   // Decode %-escapes
   word i, j, k, l;
   char * p;

   for(p = parms, i = 0; p && *p && i < 1000; p++, i++)
   {
      if(*p == '%' && isxdigit(*(p+1)) && isxdigit(*(p+2)))
      {
         p++;
         if(*p >= '0' && *p <= '9') zs[i] = *p - '0';
         else if(*p >= 'A' && *p <= 'F') zs[i] = *p - 'A' + 10;
         else if(*p >= 'a' && *p <= 'f') zs[i] = *p - 'a' + 10;
         zs[i] *= 16;
         p++;
         if(*p >= '0' && *p <= '9') zs[i] += *p - '0';
         else if(*p >= 'A' && *p <= 'F') zs[i] += *p - 'A' + 10;
         else if(*p >= 'a' && *p <= 'f') zs[i] += *p - 'a' + 10;
      }
      else
      {
         zs[i] = *p;
      }
   }
   zs[i] = '\0';

   // Parse parms
   i = j = k = l = 0;
   if(parms && zs[0] == '/') i++;
   if(parms && zs[i] == '/') i++;
   while(j < PARMS && zs[i] && k < PARMSIZE - 1 && parms)
   {
      if(zs[i] == '/')
      {
         parameters[j++][k] = '\0';
         k = 0;
         i++;
      }
      else
      {
         parameters[j][k++] = zs[i++];
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
   
   if(!strcasecmp(parameters[l], "d")) debug++;

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

   if(refresh) parameters[l][0] = '\0';

   printf("Content-Type: text/html; charset=iso-8859-1\n\n");
   printf("<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\" \"http://www.w3.org/TR/html4/loose.dtd\">\n");
   printf("<html xmlns=\"http://www.w3.org/1999/xhtml\" lang=\"en\" xml:lang=\"en\">\n");
   printf("<head>\n");
   printf("<title>%s %s</title>\n", NAME, BUILD);
   printf("<link rel=\"stylesheet\" type=\"text/css\" href=\"/auxy/liverail.css\">\n");
   printf("<script type=\"text/javascript\" src=\"/auxy/liverail.js\"></script>\n");
   printf("</head>\n");
   printf("<body onload=\"startup();\">\n");
   
   // Initialise location name cache
   location_name(NULL, false);

   // Initialise database
   db_init(conf[conf_db_server], conf[conf_db_user], conf[conf_db_password], conf[conf_db_name], DB_MODE_NORMAL);

   _log(GENERAL, "Parameters:  (l = %d)", l);
   for(i=0;i < PARMS; i++)
   {
      _log(GENERAL, "%d = \"%s\"", i, parameters[i]);
   }

   // Main bulk of page
   train();
   
   // Footer
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
      {
         printf("<p><div id=\"bottom-line\">%sCompleted at %s by %s %s at %s.  Elapsed time %s ms.</div>\n", banner, time_text(now, 1), NAME, BUILD, host, commas_q(elapsed));
         struct tm * broken = localtime(&now);
         printf("<br><div class=\"licence\">&copy;%04d.  Contains Information of Network Rail Infrastructure Limited licensed under the following licence: <a href=\"http://www.networkrail.co.uk/data-feeds/terms-and-conditions\"> Click Here.</a><br>", broken->tm_year + 1900);
         printf("These pages are provided strictly for non-commercial, non-safety critical purposes; no responsibility is accepted for any inaccurate or out of date information.");
         printf("</div></p>");
      }
      printf("</body></html>\n\n");
   }
   exit(0);
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
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

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
static void train(void)
{
   char query[2048], zs[128];
   MYSQL_RES * result0, * result1;
   MYSQL_ROW row0, row1;
   char train[1024];
   char headcode[8];
   char uid[8], requested_uid[8];
   dword schedule_id = 0;
   dword cape_id = 0;
   time_t cape_date = 0;
   word cape_vstp = false;
   struct tm broken;

   // Saved values for CR records
   char prev_uic_code[8], prev_signalling_id[8], prev_train_category[8], prev_toc_headcode[8], prev_tsc[16], prev_power_type[8];
   char prev_timing_load[8], prev_speed[8], prev_oper_chars[8], prev_train_class[4], prev_sleepers[4], prev_reservations[4];
   char prev_connection_ind[4], prev_catering_code[8], prev_service_brand[8];

   // Decode parameters
   // Date
   time_t when;
   if(atoi(parameters[3]) > 0 && atoi(parameters[4]) > 0)
   {
      broken = *(localtime(&now)); // Pre-populate
      broken.tm_mday = atoi(parameters[3]);
      broken.tm_mon = atoi(parameters[4]) - 1;
      if(atoi(parameters[5])) broken.tm_year = atoi(parameters[5]) + 100;
      broken.tm_hour = 12;
      broken.tm_min = 0;
      broken.tm_sec = 0;
      broken.tm_isdst = -1;
      when = timegm(&broken);
   }
   else
   {
      // No date supplied, use today
      broken = *(localtime(&now)); // Pre-populate
      broken.tm_hour = 12;
      broken.tm_min = 0;
      broken.tm_sec = 0;
      broken.tm_isdst = -1;
      when = timegm(&broken);
   }
   
   // P2 is CIF UID or schedule id
   if(parameters[2][0] == '$')
   {
      schedule_id = atol(parameters[2] + 1);
      if(!schedule_id) 
      {
         display_control_panel("", 0);
         return;
      }
   }
   else if(strlen(parameters[2]) == 5 || strlen(parameters[2]) == 6)
   {
      // UID
      _log(DEBUG, "Requested UID = \"%s\".", parameters[2]);

      strcpy(requested_uid, parameters[2]);
      if(strlen(requested_uid) < 6) sprintf(requested_uid, " %.6s", parameters[2]);

      // 
      sprintf(query, "SELECT id, CIF_stp_indicator, update_id, created FROM cif_schedules WHERE CIF_train_uid = '%s' AND deleted >= %ld AND created <= %ld",
              requested_uid, when + (12*60*60), when + (12*60*60));
      // Select the day
      broken.tm_hour = 12;
      broken.tm_min = 0;
      broken.tm_sec = 0;
      word day = broken.tm_wday;
      when = timegm(&broken);
   
      sprintf(zs, " AND (((%s) AND (schedule_start_date <= %ld) AND (schedule_end_date >= %ld)))",  days_runs[day],  when + 12*60*60, when - 12*60*60);
      strcat(query, zs);
      // sprintf(zs, " OR   ((%s) AND (schedule_start_date <= %ld) AND (schedule_end_date >= %ld)))",  days_runs[yest], when - 12*60*60, when - 36*60*60);
      // strcat(query, zs);

      strcat(query," ORDER BY CIF_stp_indicator DESC");

      db_query(query);
      result0 = db_store_result();

      while((row0 = mysql_fetch_row(result0)))
      {
         _log(DEBUG, "UID record found %s %s %s %s.", row0[0], row0[1], row0[2], row0[3]);

         if(row0[1][0] != 'C')
         {
            schedule_id = atol(row0[0]);
         }
         else
         {
            // Cancellation
            cape_id = atol(row0[0]);
            cape_vstp = !atoi(row0[2]);
            cape_date = atol(row0[3]);
         }
      }

      _log(DEBUG, "Found %ld", id);
      if(cape_id) _log(DEBUG, "   Cancelled by %ld VSTP = %d at %ld", cape_id, cape_vstp, cape_date);

      // Decide which to show
      if(cape_id && !cape_vstp)
      {
         schedule_id = cape_id;
      }
   }
   else
   {
      display_control_panel("", 0);
      return;
   }
    
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
         // Save some values for later CR records.
         strcpy(prev_uic_code,       row0[6]);
         strcpy(prev_signalling_id,  row0[8]);
         strcpy(prev_train_category, row0[9]);
         strcpy(prev_toc_headcode,   row0[10]);
         strcpy(prev_tsc,            row0[11]);
         strcpy(prev_power_type,     row0[13]);
         strcpy(prev_timing_load,    row0[14]);
         strcpy(prev_speed,          row0[15]);
         strcpy(prev_oper_chars,     row0[16]);
         strcpy(prev_train_class,    row0[17]);
         strcpy(prev_sleepers,       row0[18]);
         strcpy(prev_reservations,   row0[19]);
         strcpy(prev_connection_ind, row0[20]);
         strcpy(prev_catering_code,  row0[21]);
         strcpy(prev_service_brand,  row0[22]);

         strcpy(headcode, row0[8]);
         if(!headcode[0]) strcpy(headcode, row0[34]);
         printf("<h2>Service %u (%s) %s %s</h2>\n", schedule_id, show_spaces(row0[3]), headcode, train);
         strcpy(uid, row0[3]);

         // DATE WARNING
         if(atol(row0[23]) > now + 12*7*24*60*60)
         {
            printf("<p>This service runs more than twelve weeks in the future - All information should be considered provisional.</p>");
         }

         // VSTP Cancelled
         if(cape_vstp)
         {
            printf("<h2 style=\"color: red\">The service on %s", date_text(now, true));
            printf(" was cancelled by the VSTP process at %s.</h2>\n", time_text(cape_date, true));
         }

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

         printf("<tr><td>ATOC code</td><td>%s</td><td></td></tr>", row0[5]);
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
      }
      mysql_free_result(result0);

      // Association(s)         0         1         2         3              4                5                 6               7           8            9           10          11                     12                   13           14
      sprintf(query, "SELECT update_id, created, deleted, main_train_uid, assoc_train_uid, assoc_start_date, assoc_end_date, assoc_days, category, date_indicator, location, base_location_suffix, assoc_location_suffix, diagram_type, CIF_stp_indicator FROM cif_associations WHERE (main_train_uid = '%s' OR assoc_train_uid = '%s') AND deleted >= %ld AND created <= %ld AND assoc_start_date <= %ld AND assoc_end_date >= %ld", uid, uid, when + (12*60*60), when + (12*60*60), when + 12*60*60, when - 12*60*60);

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
         word i;
         result0 = db_store_result();
         while((row0 = mysql_fetch_row(result0)))
         {
            printf("<h3>Change en route at %s", location_name(row0[0], true));
            if(row0[1][0]) printf("[%s]", row0[1]);
            printf("</h3>\n");

            printf("<table class=\"train-table\">");

            if(strcmp(prev_uic_code, row0[16]))
            {
               printf("<tr><td>UIC code</td><td>%s</td><td></td></tr>", row0[16]);
               strcpy(prev_uic_code, row0[16]);
            }

            if(strcmp(prev_signalling_id, row0[3]))
            {
               printf("<tr><td>Signalling ID</td><td>%s</td><td></td></tr>", row0[3]);
               strcpy(prev_signalling_id, row0[3]);
            }

            if(strcmp(prev_train_category, row0[2]))
            {
               printf("<tr><td>Train category</td>");
               for(i=0; i < CATEGORIES && (categories[i][0]!=row0[2][0] || categories[i][1]!=row0[2][1]) ; i++);
               if(i < CATEGORIES)
                  printf("<td>%s</td><td>%s</td></tr>", row0[2], categories[i]+2);
               else if(row0[2][0])
                  printf("<td>%s</td><td>Unrecognised</td></tr>", row0[2]);
               else
                  printf("<td>%s</td><td></td></tr>", row0[2]);
               strcpy(prev_train_category, row0[2]);
            }

            if(strcmp(prev_toc_headcode, row0[16]))
            {
               printf("<tr><td>TOC Headcode</td><td>%s</td><td></td></tr>\n", row0[16]);
               strcpy(prev_toc_headcode,   row0[16]);
            }

            if(strcmp(prev_tsc, row0[5]))
            {
               printf("<tr><td>Train service code</td><td>%s</td><td></td></tr>\n", row0[5]);
               strcpy(prev_tsc, row0[5]);
            }

            if(strcmp(prev_power_type,     row0[6]))
            {
               printf("<tr><td>Power type</td><td>%s</td><td>\n", row0[6]);
               sprintf(zs, "%-3s", row0[6]);
               for(i=0; i < POWER_TYPES && strncmp(power_types[i], zs, 3) ; i++);
               if(i < POWER_TYPES)
                  printf("%s", power_types[i]+3);
               else if(row0[9][0])
                  printf("Unrecognised");
               printf("</td></tr>");
               strcpy(prev_power_type,     row0[6]);
            }

            if(strcmp(prev_timing_load,    row0[7]))
            {
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
               strcpy(prev_timing_load,    row0[7]);
            }

            if(strcmp(prev_speed,          row0[8]))
            {
               printf("<tr><td>Speed</td>");
               if(row0[8][0])
                  printf("<td>%s</td><td>mph</td>", row0[8]);
               else
                  printf("<td>&nbsp;</td><td>&nbsp;</td>");
               strcpy(prev_speed,          row0[8]);
            }

            if(strcmp(prev_oper_chars,     row0[9]))
            {
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
               strcpy(prev_oper_chars,     row0[9]);
            }

            if(strcmp(prev_train_class,    row0[10]))
            {
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
               strcpy(prev_train_class,    row0[10]);
            }

            if(strcmp(prev_sleepers,       row0[11]))
            {
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
               strcpy(prev_sleepers,       row0[11]);
            }

            if(strcmp(prev_reservations,   row0[12]))
            {
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
               strcpy(prev_reservations,   row0[12]);
            }

            if(strcmp(prev_connection_ind, row0[13]))
            {
               printf("<tr><td>Connection Indicator</td><td></td><td>%s</td></tr>\n", row0[13]);
               strcpy(prev_connection_ind, row0[13]);
            }

            if(strcmp(prev_catering_code,  row0[14]))
            {
               printf("<tr><td>Catering Code</td><td></td><td>%s</td></tr>\n", row0[14]);
               strcpy(prev_catering_code,  row0[14]);
            }

            if(strcmp(prev_service_brand,  row0[15]))
            {
               printf("<tr><td>Service Branding</td><td></td><td>%s</td></tr>\n", row0[15]);
               strcpy(prev_service_brand,  row0[15]);
            }

            printf("</tr></table>\n");
         }
         mysql_free_result(result0);
      }

      printf("</td><td width=\"5%%\">&nbsp;</td><td style=\"vertical-align:top;\">\n"); // Outer table

      // TRUST
      char trust_id[16], cancelled_at[16];
      cancelled_at[0] = trust_id[0] = '\0';
      if(when)
      {
         struct tm * broken = gmtime(&when);
         printf("<table width=\"900px\"><tr>\n");
         printf("<td>Real time data is shown for %s %02d/%02d/%02d</td>\n", days[broken->tm_wday % 7], broken->tm_mday, broken->tm_mon + 1, broken->tm_year % 100);
         printf("<td width = \"10%%\">&nbsp;</td>\n");
         printf("<td class=\"control-panel-row\"> Show real time data for ");
         printf("<input type=\"text\" id=\"train_date\" size=\"8\" maxlength=\"8\" placeholder=\"dd/mm/yy\" value=\"\" onkeydown=\"if(event.keyCode == 13) train_date_onclick_train('%s'); else ar_off();\">\n", uid);
         printf(" <button id=\"search\" class=\"cp-button\" onclick=\"train_date_onclick_train('%s');\">Show</button> </td>\n", uid);
         printf("</tr></table>\n");
         printf("<table width=\"100%%\">");

         char query[2048];
         MYSQL_RES * result2;
         MYSQL_ROW row2;
         const char * const nm_header = "<tr class=\"small-table\"><th>&nbsp;&nbsp;Received&nbsp;&nbsp;</th><th>Event</th><th colspan=11>TRUST Event Data</th></tr>\n";

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
                        sprintf(nm[next_nm++], "<tr class=\"small-table-act\"><td>&nbsp;</td><td></td><td colspan=11>Schedule source \"%s\", TP Origin Timestamp %s, TP Origin %s</td></tr>\n", row2[2], date_text(atol(row2[5]), true), show_stanox(row2[7]));
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
                     strcpy(cancelled_at, "");
                  }
                  else
                  {
                     sprintf(nm[next_nm++], "<tr class=\"small-table-cape\"><td>%s</td><td>Cancelled</td><td colspan=11>At %s.  Reason %s %s</td></tr>\n", time_text(atol(row2[0]), true), show_stanox(row2[4]), show_cape_reason(row2[1]), row2[2]);
                     strcpy(cancelled_at, row2[4]);
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

            printf("</table>\n<br><table width=\"100%%\">");

            // Movements and Schedule

            db_query("DROP TABLE IF EXISTS train");
            db_query(create_temp_table);
            
            // Get schedule information into our temporary table.
            sprintf(query, "INSERT INTO train (activities, record_identity, tiploc_code, tiploc_instance, arrival, departure, pass, public_arrival, public_departure, sort_time, next_day, splatform, line, path, engineering_allowance, pathing_allowance, performance_allowance) SELECT location_type, record_identity, tiploc_code, tiploc_instance, arrival, departure, pass, public_arrival, public_departure, sort_time, next_day, platform, line, path, engineering_allowance, pathing_allowance, performance_allowance FROM cif_schedule_locations WHERE cif_schedule_id = %u", schedule_id);
            db_query(query);

            // Mark the cancelled_here
            if(cancelled_at[0])
            {
               sprintf(query, "SELECT tiploc from corpus where stanox = %s", cancelled_at);
               if(!db_query(query) && (result0 = db_store_result()) && (row0 = mysql_fetch_row(result0)))
               {
                  sprintf(query, "UPDATE train SET cancelled_here = 1 WHERE tiploc_code = '%s'", row0[0]);
                  db_query(query);
               }
               if(result0) mysql_free_result(result0);
            }
            
            // Now insert the train movement data
            sprintf(query, "SELECT platform, loc_stanox, actual_timestamp, gbtt_timestamp, planned_timestamp, timetable_variation, flags FROM trust_movement WHERE trust_id = '%s' AND created > %ld AND created < %ld ORDER BY actual_timestamp", trust_id, now - 15*24*60*60, now + 15*24*60*60);

            next_row_id = 0;
            if(!db_query(query))
            {
               result2 = db_store_result();

               while((row2 = mysql_fetch_row(result2)))
               {
                  char planned[128], tiploc[16];
                  strcpy(planned, trust_to_cif_time(row2[4]));
                  if(!planned[0]) strcpy(planned, "xxxxH");
                  sprintf(query, "SELECT tiploc from corpus where stanox = %s", row2[1]);
                  if(!db_query(query) && (result0 = db_store_result()) && (row0 = mysql_fetch_row(result0)))
                  {
                     strcpy(tiploc, row0[0]);
                  }
                  else
                  {
                     strcpy(tiploc, "Error1");
                  }
                  //if(row0) mysql_free_result(row0);
                  
                  word dep = (atoi(row2[6]) & 0x0003) == 1;
                  if(!dep)
                  {
                     // Arrival
                     sprintf(query, "SELECT id FROM train WHERE tiploc_code = '%s' AND (arrival = '%s' OR pass = '%s' OR (arrival IS NULL AND pass IS NULL))", tiploc, planned, planned);
                     db_query(query);
                     if((result0 = db_store_result()) && mysql_num_rows(result0))
                     {
                        row0 = mysql_fetch_row(result0);
                        // HIT
                        sprintf(query, "UPDATE train SET a_tplatform = '%s', a_actual_timestamp = '%s',a_timetable_variation = '%s', a_flags = '%s' WHERE id = '%s'", row2[0], row2[2], row2[5], row2[6], row0[0]);
                        db_query(query);
                        mysql_free_result(result0);
                        next_row_id = atoi(row0[0]);
                     }
                     else
                     {
                        // MISS
                        mysql_free_result(result0);
                        db_query("SELECT sort_time, next_day FROM train ORDER BY GREATEST(IF(a_actual_timestamp, a_actual_timestamp, 0), IF(d_actual_timestamp, d_actual_timestamp, 0)) DESC LIMIT 1");
                        result0 = db_store_result();
                        row0 = mysql_fetch_row(result0);
                                                   
                        sprintf(query, "INSERT INTO train SET tiploc_code = '%s', sort_time = '%s', next_day = '%s', a_tplatform = '%s', a_actual_timestamp = '%s', a_timetable_variation = '%s', a_flags = '%s'", tiploc, row0[0], row0[1], row2[0], row2[2], row2[5], row2[6]);
                        db_query(query);
                        next_row_id++;
                     }
                  }
                  else
                  {
                     // Departure
                     sprintf(query, "SELECT id FROM train WHERE tiploc_code = '%s' AND (departure = '%s' OR pass = '%s' OR (departure is NULL AND pass IS NULL))", tiploc, planned, planned);
                     db_query(query);
                     if((result0 = db_store_result()) && mysql_num_rows(result0))
                     {
                        row0 = mysql_fetch_row(result0);
                        
                        // HIT
                        sprintf(query, "UPDATE train SET d_tplatform = '%s', d_actual_timestamp = '%s', d_timetable_variation = '%s', d_flags = '%s' WHERE id = '%s'", row2[0], row2[2], row2[5], row2[6], row0[0]);
                        db_query(query);
                        mysql_free_result(result0);
                        next_row_id = atoi(row0[0]) + 1;
                     }
                     else
                     {
                        // MISS
                        // This won't work if the miss is received before any hits.
                        mysql_free_result(result0);
                        db_query("SELECT sort_time, next_day FROM train ORDER BY GREATEST(IF(a_actual_timestamp, a_actual_timestamp, 0), IF(d_actual_timestamp, d_actual_timestamp, 0)) DESC LIMIT 1");
                        result0 = db_store_result();
                        row0 = mysql_fetch_row(result0);
                                                   
                        sprintf(query, "INSERT INTO train SET tiploc_code = '%s', sort_time = '%s', next_day = '%s', d_tplatform = '%s', d_actual_timestamp = '%s', d_timetable_variation = '%s', d_flags = '%s'", tiploc, row0[0], row0[1], row2[0], row2[2], row2[5], row2[6]);
                        db_query(query);
                        next_row_id++;
                     }
                  }                       
               }
            }
            show_running(when, trust_id[0]);
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

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
static void show_running(const time_t when, const word activated)
{
   MYSQL_RES * result0;
   MYSQL_ROW row0;
   word row_id;
   word saved_var, saved_late;
   char column[128], c[128];
   char * clas;
   word cancelled, cape_here;
      

   db_query("SELECT * FROM train ORDER BY next_day, sort_time, GREATEST(IF(a_actual_timestamp, a_actual_timestamp, 0), IF(d_actual_timestamp, d_actual_timestamp, 0))");
   result0 = db_store_result();

   printf("<tr class=\"small-table\">");
   if(activated)
      printf("<th></th><th></th><th colspan=3>Arrive</th><th colspan=2>Pass</th><th colspan=3>Depart</th><th colspan=3>Schedule</th><th colspan=2>Actual</th>");
   else
      printf("<th></th><th></th><th colspan=2>Arrive</th><th colspan=1>Pass</th><th colspan=2>Depart</th><th colspan=3>Schedule</th>");
   printf("</tr>");

   printf("<tr class=\"small-table\">");
   if(activated)
      printf("<th>Location</th><th>P</th><th>WTT</th><th>Public</th><th>Actual</th><th>WTT</th><th>Actual</th><th>WTT</th><th>Public</th><th>Actual</th><th>Path-Line</th><th>Allowances</th><th>Activities</th><th>P</th><th>Notes</th>");
   else
      printf("<th>Location</th><th>P</th><th>WTT</th><th>Public</th><th>WTT</th><th>WTT</th><th>Public</th><th>Path-Line</th><th>Allowances</th><th>Activities</th>");

   word zi;
   for(zi = 0; zi < MAX_COLUMN && debug; zi++)
   {
      printf("<th>%d</th>", zi);
   }
   printf("</tr>\n");

   saved_var = saved_late = cancelled = 0;
   
   while((row0 = mysql_fetch_row(result0)))
   {
      row_id = atoi(row0[id]);
      cape_here = (row0[cancelled_here] && atoi(row0[cancelled_here]));
      if(cape_here) cancelled = true;
      printf("<tr class=\"small-table\">");
      printf("<td>%s", show_tiploc_link(row0[tiploc_code], true, "sum", when));
      if(row0[tiploc_instance] && row0[tiploc_instance][0] && row0[tiploc_instance][0] != ' ')
      {
         printf(" [%s]", row0[tiploc_instance]);
      }
      printf("</td>");

      // P
      printf("<td>");
      if(row0[splatform]) printf("%s", row0[splatform]);
      printf("</td>");

      // Times WTT a.
      printf("<td>");
      if(row0[arrival]          && row0[arrival][0])          printf("%s", show_time(row0[arrival]));
      printf("</td>");

      // Public a.
      printf("<td>");
      if(row0[public_arrival]   && row0[public_arrival][0])   printf("%s",  show_time(row0[public_arrival]));
      printf("</td>");

      // Actual a.
      if(activated)
      {
         column[0] = c[0] = '\0';
         clas = "";
         //                                                            vv Pass events may have an actual arrival which we don't want to show.
         if(row0[a_actual_timestamp] && row0[a_actual_timestamp][0] && !(row0[pass] && row0[pass][0]))
         {
            sprintf(column, "%s ", show_trust_time(row0[a_actual_timestamp], true));
            switch(atoi(row0[a_flags]) & 0x0018)
            {
            case 0x0000:
               sprintf(c, "%s Early", row0[a_timetable_variation]);
               if(atoi(row0[a_timetable_variation]) > 2) clas = " class=\"summ-table-minor\"";
               else clas = " class=\"summ-table-good\"";
               break;
            case 0x0008: sprintf(c, "On time"); clas = " class=\"summ-table-good\"";break;
            case 0x0010:
               sprintf(c, "%s Late",  row0[a_timetable_variation]);
               if(atoi(row0[a_timetable_variation]) > 5) clas = " class=\"summ-table-major\"";
               else if(atoi(row0[a_timetable_variation]) > 2) clas = " class=\"summ-table-minor\"";
               else clas = " class=\"summ-table-good\"";
               break;
               // case 0x0018: printf("Off route"); break;
            }
            strcat(column, c);
            saved_var = atoi(row0[a_timetable_variation]);
            saved_late = atoi(row0[a_flags]) & 0x0010;
         }
         else
         {
            // No data yet
            clas = " class=\"summ-table-idle\"";
            if(row0[arrival] && row0[arrival][0] && activated)
            {
               if(cancelled && !cape_here)
               {
                  sprintf(column, "Cancelled");
                  clas = " class=\"summ-table-cape\"";
               }
               else if(row_id >= next_row_id)
               {
                  sprintf(column, "Exp. %s", show_expected_time(row0[arrival], saved_var, saved_late));
               }
               else
               {
                  sprintf(column, "No report");
               }
            }
         }
         printf("<td%s>%s</td>", clas, column);
      }
      
      // WTT p.
      printf("<td>");
      if(row0[pass] && row0[pass][0]) printf("%s",  show_time(row0[pass]));
      printf("</td>\n");

      // Actual p.
      if(activated)
      {
         column[0] = c[0] = '\0';
         clas = "";
         if(row0[d_actual_timestamp] && row0[d_actual_timestamp][0] && row0[pass] && row0[pass][0])
         {
            sprintf(column, "%s ", show_trust_time(row0[d_actual_timestamp], true));
            switch(atoi(row0[d_flags]) & 0x0018)
            {
            case 0x0000:
               sprintf(c, "%s Early", row0[d_timetable_variation]);
               if(atoi(row0[d_timetable_variation]) > 2) clas = " class=\"summ-table-minor\"";
               else clas = " class=\"summ-table-good\"";
               break;
            case 0x0008: sprintf(c, "On time"); clas = " class=\"summ-table-good\"";break;
            case 0x0010:
               sprintf(c, "%s Late", row0[d_timetable_variation]);
               if(atoi(row0[d_timetable_variation]) > 5) clas = " class=\"summ-table-major\"";
               else if(atoi(row0[d_timetable_variation]) > 2) clas = " class=\"summ-table-minor\"";
               else clas = " class=\"summ-table-good\"";
               break;
               // case 0x0018: printf("Off route"); break;
            }
            strcat(column, c);
            saved_var = atoi(row0[d_timetable_variation]);
            saved_late = atoi(row0[d_flags]) & 0x0010;
         }
         else if(row0[a_actual_timestamp] && row0[a_actual_timestamp][0] && row0[pass] && row0[pass][0])
         {
            // if we've only got an arrival at a pass location use that.
            sprintf(column, "%s ", show_trust_time(row0[a_actual_timestamp], true));
            switch(atoi(row0[a_flags]) & 0x0018)
            {
            case 0x0000:
               sprintf(c, "%s Early", row0[a_timetable_variation]);
               if(atoi(row0[a_timetable_variation]) > 2) clas = " class=\"summ-table-minor\"";
               else clas = " class=\"summ-table-good\"";
               break;
            case 0x0008: sprintf(c, "On time"); clas = " class=\"summ-table-good\"";break;
            case 0x0010:
               sprintf(c, "%s Late", row0[a_timetable_variation]);
               if(atoi(row0[a_timetable_variation]) > 5) clas = " class=\"summ-table-major\"";
               else if(atoi(row0[a_timetable_variation]) > 2) clas = " class=\"summ-table-minor\"";
               else clas = " class=\"summ-table-good\"";
               break;
               // case 0x0018: printf("Off route"); break;
            }
            strcat(column, c);
            saved_var = atoi(row0[a_timetable_variation]);
            saved_late = atoi(row0[a_flags]) & 0x0010;
         }
         else
         {
            // No data yet
            clas = " class=\"summ-table-idle\"";
            if(row0[pass] && row0[pass][0] && activated)
            {
               if(cancelled)
               {
                  sprintf(column, "Cancelled");
                  clas = " class=\"summ-table-cape\"";
               }
               else if(row_id >= next_row_id)
               {
                  sprintf(column, "Exp. %s", show_expected_time(row0[pass], saved_var, saved_late));
               }
               else
               {
                  sprintf(column, "No report");
               }
            }
         }
         printf("<td%s>%s</td>", clas, column);
      }
      
      // WTT d.
      printf("<td>");
      if(row0[departure]        && row0[departure][0])        printf("%s",show_time(row0[departure]));
      printf("</td>\n");

      // Public d.
      printf("<td>");
      if(row0[public_departure] && row0[public_departure][0]) printf("%s",  show_time(row0[public_departure]));
      printf("</td>\n");

      // Actual d.
      if(activated)
      {
         column[0] = c[0] = '\0';
         clas = "";
         if(row0[d_actual_timestamp] && row0[d_actual_timestamp][0] && !(row0[pass] && row0[pass][0]))
         {
            sprintf(column, "%s ", show_trust_time(row0[d_actual_timestamp], true));
            switch(atoi(row0[d_flags]) & 0x0018)
            {
            case 0x0000:
               sprintf(c, "%s Early", row0[d_timetable_variation]);
               if(atoi(row0[d_timetable_variation]) > 2) clas = " class=\"summ-table-minor\"";
               else clas = " class=\"summ-table-good\"";
               break;
            case 0x0008: sprintf(c, "On time"); clas = " class=\"summ-table-good\"";break;
            case 0x0010:
               sprintf(c, "%s Late", row0[d_timetable_variation]);
               if(atoi(row0[d_timetable_variation]) > 5) clas = " class=\"summ-table-major\"";
               else if(atoi(row0[d_timetable_variation]) > 2) clas = " class=\"summ-table-minor\"";
               else clas = " class=\"summ-table-good\"";
               break;
               // case 0x0018: printf("Off route"); break;
            }
            strcat(column, c);
            saved_var = atoi(row0[d_timetable_variation]);
            saved_late = atoi(row0[d_flags]) & 0x0010;
         }
         else
         {
            // No data yet
            clas = " class=\"summ-table-idle\"";
            if(row0[departure] && row0[departure][0] && activated)
            {
               if(cancelled)
               {
                  sprintf(column, "Cancelled");
                  clas = " class=\"summ-table-cape\"";
               }
               else if(row_id >= next_row_id)
               {
                  sprintf(column, "Exp. %s", show_expected_time(row0[departure], saved_var, saved_late));
               }
               else
               {
                  sprintf(column, "No report");
               }
            }
         }
         printf("<td%s>%s</td>", clas, column);
      }

      // Path-Line
      printf("<td>");
      if(row0[path])
      {
         if(row0[path][0] || row0[line][0])
         {
            printf("%s - %s", row0[path], row0[line]);
         }
      }
      printf("</td>");
      
      // Allow.
      printf("<td>");
      if(row0[engineering_allowance] && row0[engineering_allowance][0]) printf("Eng: %s ",  show_time(row0[engineering_allowance]));
      if(row0[pathing_allowance]     && row0[pathing_allowance][0]    ) printf("Path: %s ", show_time(row0[pathing_allowance]    ));
      if(row0[performance_allowance] && row0[performance_allowance][0]) printf("Perf: %s ", show_time(row0[performance_allowance]));
      printf("</td>");
      
      // Activities
      printf("<td>");
      if(row0[act]) printf("%s",show_act(row0[act]));
      printf("</td>");

      if(activated)
      {
         // TRUST
         // Platform
         column[0] = c[0] = '\0';
         clas = "";
         {
            char a[32], d[32];
            a[0] = d[0] = '\0';
            if(row0[a_tplatform]) strcpy(a, row0[a_tplatform]);
            if(row0[d_tplatform]) strcpy(d, row0[d_tplatform]);
            if(a[0] && d[0] && strcmp(a, d))
            {
               // Both are set and they're different!
               sprintf(column, "a. %s d. %s", a, d);
            }
            else
            {
               if(a[0])
               {
                  sprintf(column, "%s", a);
               }
               else
               {
                  sprintf(column,"%s", d);
               }
            }
         }
         if(column[0] && row0[splatform] && row0[splatform][0] && (atoi(column) != atoi(row0[splatform]))) clas = " class=\"summ-table-minor\"";
         // else if(row_id >= next_row_id && !(row0[a_actual_timestamp] && row0[a_actual_timestamp][0])) clas = " class=\"summ-table-idle\"";
         printf("<td%s>%s</td>", clas, column);
         
         // Notes        
         column[0] = c[0] = '\0';
         clas = "";
         
         word merged_flags = atoi(row0[a_flags] ? row0[a_flags] : "") | atoi(row0[d_flags] ? row0[d_flags]:"");
         if(merged_flags & 0x0020) { strcat(column, "Off route "); clas = " class=\"summ-table-minor\""; }
         if(merged_flags & 0x0040) { strcat(column, "Terminated "); }
         if(merged_flags & 0x0080) { strcat(column, "Correction "); }
         // if(row_id >= next_row_id && !(row0[a_actual_timestamp] && row0[a_actual_timestamp][0])) clas = " class=\"summ-table-idle\"";
         printf("<td%s>%s</td>", clas, column);
      }

      // Debug info
      for(zi = 0; zi < MAX_COLUMN && debug; zi++)
      {
         printf("<td>%s</td>", row0[zi]);
      }
      printf("</tr>\n");
   }
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
static char * show_tiploc_link(const char * const tiploc, const word use_cache, const char * const view, const time_t when)
{
   // Not re-entrant
   // Set use_cache to false if hit is not expected, to bypass search.  Cache will still be updated.
   static char result[256];

   char * name = location_name(tiploc, use_cache);

   sprintf(result, "<a class=\"linkbutton\" href=\"%s%s/%s/%s\">%s</a>", URL_BASE, view, tiploc, show_date(when, 0), name);

   return result;
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
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

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
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

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
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

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
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

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
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

   strcpy(result, time_text(stamp, local));

   // Lose the seconds
   result[14] = '\0';

   if((stamp%60) > 29) strcat(result, "&half;");

   // Lose the date
   return result + 9;
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
static char * trust_to_cif_time(const char * const s)
{
   // The input is a string containing the number of s since epoch
   _log(PROC, "trust_to_cif_time(%s)", s);

   static char result[32];

   time_t stamp = atol(s);

   if(!stamp)
   {
      result[0] = '\0';
      return result;
   }

   strcpy(result, time_text(stamp, true));

   // Lose the colon
   result[11] = result[12];
   result[12] = result[13];
   result[13] = '\0';

   if((stamp%60) > 29) strcat(result, "H");

   // Lose the date
   return result + 9;
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
static char * show_expected_time(const char * const scheduled, const word deviation, const word late)
{
   // Scheduled in format hhmmH

   // Can we do something clever here with regard to "overdue" ?
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
   sprintf(result, "%02d:%02d", hour, minute);
   if(scheduled[4] == 'H') strcat(result, "&half;");
   return result;
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
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

