/*
    Copyright (C) 2015, 2016, 2017 Phil Wieland

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

#define NAME "railquery"
#define DISPLAY_NAME "Open Rail Query"

#ifndef RELEASE_BUILD
#define BUILD "YB07p"
#else
#define BUILD RELEASE_BUILD
#endif

static void display_menu(void);
static void report_b(void);
static void report_c(void);
static void report_d(void);
static void report_e(void);
static void report_be(void);
static void report_f(void);
static char * location_name(const char * const tiploc);
static char * location_name_stanox(const word stanox);
static char * describe_schedule(const dword id);

#define URL_BASE "/rail/query/"
#define LIVERAIL_URL_BASE "/rail/liverail/"

word debug;
static time_t now;
#define PARMS 10
#define PARMSIZE 256
static char parameters[PARMS][256];

// (Hours * 60 + Minutes) * 4
#define DAY_START  4*60*4

static char mode;
static word text_mode;

#define QUERIES 3
MYSQL_RES * db_result[QUERIES];
MYSQL_ROW      db_row[QUERIES];

int main()
{
   now = time(NULL);

   qword start_time = time_ms();
   mode = '-';
   text_mode = false;

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
         if((parms[i] >= 'A' && parms[i] <= 'Z') || (parms[i] >= 'a' && parms[i] <= 'z') || (parms[i] >= '0' && parms[i] <= '9') || parms[i] == ' ')
            parameters[j][k++] = toupper(parms[i++]);
         else
            i++;
         l = j;
      }
   }
   if(k) parameters[j++][k] = '\0';

   while(j < PARMS) parameters[j++][0] = '\0';

   {
      char config_file_path[256];

      strcpy(config_file_path, "/etc/openrail.conf");

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

   if(parameters[0][0]) mode = toupper(parameters[0][0]);
   text_mode = (parameters[0][0] && toupper(parameters[0][1] == 'T'));
   _log(DEBUG, "Display mode  = '%c', text mode = %s", mode, text_mode?"On":"Off");

   printf("Content-Type: text/html; charset=iso-8859-1\n\n");
   printf("<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\" \"http://www.w3.org/TR/html4/loose.dtd\">\n");
   printf("<html xmlns=\"http://www.w3.org/1999/xhtml\" lang=\"en\" xml:lang=\"en\">\n");
   printf("<head>\n");
   printf("<title>%s %s</title>\n", DISPLAY_NAME, BUILD);
   printf("<link rel=\"stylesheet\" type=\"text/css\" href=\"/auxy/liverail.css\">\n");
   printf("<script type=\"text/javascript\" src=\"/auxy/railquery.js\"></script>\n");
   printf("</head>\n");
   printf("<body style=\"font-family: arial,sans-serif;\" onload=\"startup();\">\n");

   // Initialise database
   db_init(conf[conf_db_server], conf[conf_db_user], conf[conf_db_password], conf[conf_db_name]);

   _log(GENERAL, "Parameters:  (l = %d)", l);
   for(i=0;i < PARMS; i++)
   {
      _log(GENERAL, "%d = \"%s\"", i, parameters[i]);
   }

   switch(mode)
   {
   case 'B':
      report_b();
      break;

   case 'C':
      report_c();
      break;

   case 'D':
      report_d();
      break;

   case 'E':
      report_e();
      break;

   case 'F':
      report_f();
      break;

   case 'Z':
   default:
      display_menu();
      break;
   }

   if(mode != '-')
   {
      printf("<br><button class=\"cp-button\" onclick=\"menu_onclick();\">Main Menu</button>\n");
   }

   char host[256];
   if(gethostname(host, sizeof(host))) host[0] = '\0';
   qword elapsed = time_ms() - start_time;
   printf("<p><div id=\"bottom-line\">Completed at %s by %s %s at %s.  Elapsed time %s ms.</div>", time_text(time(NULL), 1), DISPLAY_NAME, BUILD, host, commas_q(elapsed));
   printf("<br><div class=\"licence\">Contains Information of Network Rail Infrastructure Limited licensed under the following licence: <a href=\"http://www.networkrail.co.uk/data-feeds/terms-and-conditions\"> Click Here</a>.</br>");
   printf("These pages are provided strictly for non-commercial, non-safety critical purposes; no responsibility is accepted for any inaccurate or out of date information.");
   printf("</div></p>");

   printf("</body></html>\n\n");
   
   exit(0);
}

static void display_menu(void)
{
   // Parameters on a 'Z' option:
   // [1] Garner ID
   // [2] CIF UID

   word p;
   for (p = 1; mode != 'Z' && p < PARMS; p++) parameters[p][0] = '\0';

   printf(
          "<table>"
          "<tr valign=\"top\"><td class=\"control-panel-row\" colspan=7>"
          "<table><tr><td><h4>Return to Live Trains: </h4></td><td>"
          "&nbsp;Show trains at <input type=\"text\" id=\"search_loc\" size=\"28\" maxlength=\"64\" placeholder=\"Location name / TIPLOC / 3-alpha\" value=\"\" onkeydown=\"if(event.keyCode == 13) search_onclick();\">"
          " on <input type=\"text\" id=\"search_date\" size=\"8\" maxlength=\"8\" placeholder=\"dd/mm/yy\" value=\"\" onkeydown=\"if(event.keyCode == 13) search_onclick();\">"
          " <button id=\"search\" class=\"cp-button\" onclick=\"search_onclick();\">All</button>"
          " <button id=\"search\" class=\"cp-button\" onclick=\"freight_onclick();\">Freight</button>"
          " <button id=\"search\" class=\"cp-button\" onclick=\"summary_onclick();\">Summary</button>"
          " <button id=\"search\" class=\"cp-button\" onclick=\"depart_onclick();\">Departures</button>"
          "</td></tr></table></tr>"
          /*
          "<tr valign=\"top\"><td class=\"control-panel-row\" colspan=7>"
          "<h4>Options</h4>\n"
          "Display results in plain text.<input type=\"checkbox\" id=\"plain-text\" value=\"0\">"
          "</td></tr>\n"
          */
          "<tr valign=\"top\"><td class=\"control-panel-row\">"
          "<h4>[A]Go direct to schedule</h4>\n");
   printf("Schedule ID number <input type=\"text\" id=\"A-id\" size=\"7\" maxlength=\"9\" value=\"%s\" onkeydown=\"if(event.keyCode == 13) A_onclick();\">&nbsp; &nbsp\n", parameters[1]);
   printf("<br><button class=\"cp-button\" onclick=\"A_onclick();\">Go</button>\n"

          "</td><td width=\"5%%\">&nbsp;</td><td class=\"control-panel-row\">\n"
          "<h4>[B]Schedules by CIF UID</h4>\n");
   printf("CIF UID <input type=\"text\" id=\"B-u\" size=\"16\" maxlength=\"64\" value=\"%s\" onkeydown=\"if(event.keyCode == 13) B_onclick();\">\n", parameters[2]);
   printf("<br>Only if valid this week.<input type=\"checkbox\" id=\"B-w\" value=\"0\">\n"
          "<br><button class=\"cp-button\" onclick=\"B_onclick();\">Search</button>\n"

          "</td><td width=\"5%%\">&nbsp;</td><td class=\"control-panel-row\">\n"
          "<h4>[C]TRUST Activations by Headcode</h4>\n"
          "Headcode (or pseudo-headcode)<input type=\"text\" id=\"C-h\" size=\"3\" maxlength=\"4\" value=\"\" onkeydown=\"if(event.keyCode == 13) C_onclick();\">" 
          "<br>Only this week <input type=\"checkbox\" id=\"C-w\" value=\"0\">"
          "<br><button class=\"cp-button\" onclick=\"C_onclick();\">Search</button>\n"

          "</td><td width=\"5%%\">&nbsp;</td><td class=\"control-panel-row\">\n"
          "<h4>[D]TRUST Activations by Schedule</h4>\n");
   printf("Schedule ID number <input type=\"text\" id=\"D-s\" size=\"7\" maxlength=\"9\" value=\"%s\" onkeydown=\"if(event.keyCode == 13) D_onclick();\">", parameters[1]);
   printf("<br>Only this week <input type=\"checkbox\" id=\"D-w\" value=\"0\">"
          "<br><button class=\"cp-button\" onclick=\"D_onclick();\">Search</button>\n"
          "</td></tr>\n"

          "<tr valign=\"top\"><td class=\"control-panel-row\">"
          "<h4>[E]Schedules by headcode</h4>\n"
          "Headcode <input type=\"text\" id=\"E-v\" size=\"3\" maxlength=\"4\" value=\"\" onkeydown=\"if(event.keyCode == 13) E_onclick();\">\n"
          "<br>Only if valid this week.<input type=\"checkbox\" id=\"E-w\" value=\"0\">\n"
          "<br><button class=\"cp-button\" onclick=\"E_onclick();\">Search</button>\n"

          "</td><td width=\"5%%\">&nbsp;</td><td class=\"control-panel-row\">\n"
          "<h4>[F]Historical Analysis</h4>\n");
   printf("Schedule ID number <input type=\"text\" id=\"F-id\" size=\"7\" maxlength=\"9\" value=\"%s\" onkeydown=\"if(event.keyCode == 13) F_onclick();\">&nbsp; &nbsp\n", parameters[1]);
   printf("<br>Location <input type=\"text\" id=\"F-l\" size=\"20\" maxlength=\"50\" value=\"\" onkeydown=\"if(event.keyCode == 13) F_onclick();\">&nbsp; &nbsp\n");
   printf("<br><button class=\"cp-button\" onclick=\"F_onclick();\">Report</button>\n"
   
          /*
          "</td><td width=\"5%%\">&nbsp;</td><td class=\"control-panel-off\">\n"
          "<h4>[G]Overlay Differences</h4>\n");
   printf("Overlay garner schedule ID <input type=\"text\" id=\"G-id\" size=\"7\" maxlength=\"9\" value=\"%s\" onkeydown=\"if(event.keyCode == 13) G_onclick();\">&nbsp; &nbsp\n", parameters[1]);
   printf("<br><button class=\"cp-button\" onclick=\"G_onclick();\">Report</button>\n"

          "</td><td width=\"5%%\">&nbsp;</td><td class=\"control-panel-off\">\n"
          "<h4>[H]Database Checks</h4>\n"
          "Check for duplicate schedules.\n"
          "<br><button class=\"cp-button\" onclick=\"H_onclick();\">Report</button>\n"
          */
          "</td><td width=\"5%%\">&nbsp;</td><td class=\"control-panel-row\">\n"
          "</td><td width=\"5%%\">&nbsp;</td><td class=\"control-panel-row\">\n"

          "</td></tr></table>\n"
          );

   printf("<input type=\"hidden\" id=\"date\" value=\"%s\"/>\n", date_text(time(NULL), true));

   printf("<div class=\"help-text-box\">");
   printf("<p><b>Open Rail real time railway data advanced database queries.</b></p>"
          "<p>This part of the Open Rail system is experimental, and still under development.  Some queries may return incorrect or confusing results.  A degree of understanding of the inner workings of the railway systems behind this data (The &quot;CIF&quot; schedule database, and &quot;TRUST&quot;.) may be needed to understand the results of some queries.</p>"
          "<p>A schedule ID number is a unique identification number for a schedule in the Open Rail database.  When you see one in the results of one of these queries, click on it and you will be returned to this page with the number filled in ready for queries [A], [D] and [F].</p>"
"<p>A CIF UID is the ID for a schedule in Network Rail's database, it consists of a letter or a space followed by five digits, e.g. Y63320.</p>"
"<p>The Open Rail database contains live running data for the last four hundred days, and future schedule information up to the limit of Network Rail's information.</p>"
          );
   printf("</div>\n");
}


static void report_b(void)
{
   // p1 CIF UID
   // p2 W for this week

   char query[1024], q1[1024];

   sprintf(query, "SELECT train_status, schedule_start_date, schedule_end_date, signalling_id, CIF_train_uid, CIF_stp_indicator, update_id, id, ");
   strcat(query, "runs_mo, runs_tu, runs_we, runs_th, runs_fr, runs_sa, runs_su, deleted, created, deduced_headcode, deduced_headcode_status");
   strcat(query,  " FROM cif_schedules WHERE ");
   // Sanitise
   char safe[256];
   db_real_escape_string(safe, parameters[1], strlen(parameters[1]));
   // Insert missing leading space if necessary
   if(strlen(parameters[1]) == 5)
      sprintf(q1, "CIF_train_uid = ' %s'", safe);
   else
      sprintf(q1, "CIF_train_uid = '%s'", safe);

   strcat(query, q1);

   // This week
   if(parameters[2][0] == 'W')
   {
      sprintf(q1, " AND schedule_start_date < %ld AND schedule_end_date > %ld and deleted > %ld", now+(7*24*60*60), now-(7*24*60*60), now-(7*24*60*60));
      strcat(query, q1);
   }
   strcat(query, " ORDER BY schedule_start_date, created");
   if(db_query(query))
   {
   }
   else
   {
      report_be();
   }
}

static void report_be(void)
{
   char query[1024];
      db_result[0] = db_store_result();
      if(text_mode)
      {
         printf("<pre>Activated                 TRUST ID       Schedule ID\n");
      }
      else
      {
         printf("<table class=\"result-table\"><tr><th>Type</th><th>ID</th><th>Head</th><th>CIF UID</th><th>STP</th><th>From</th><th>To</th><th>Days</th><th>Valid dates</th><th>Created</th><th>Deleted</th></tr>\n");
      }
      while((db_row[0] = mysql_fetch_row(db_result[0])))
      {
         if(text_mode)
         {
            printf("%20s %15s %15s%s\n", time_text(atol(db_row[0][0]), true), db_row[0][2], db_row[0][1], (db_row[0][3][0] == '0')?"":" Deduced");
         }
         else
         {
            printf("<tr class=\"result-table\">\n");
            
            word vstp = (db_row[0][6][0] == '0' && db_row[0][6][1] == 0);
            dword id = atol(db_row[0][7]);

            // Status
            if(vstp) printf("<td class=\"result-table-vstp\">(VSTP) ");
            else if(db_row[0][0][0] == 'F' || db_row[0][0][0] == '2' || db_row[0][0][0] == '3') printf("<td class=\"result-table-freight\">");
            else printf("<td>");
            switch(db_row[0][0][0])
            {
            case 'B': printf("Bus</td>");         break;
            case 'F': printf("Freight</td>");     break;
            case 'P': printf("Passenger</td>");   break;
            case 'T': printf("Trip</td>");        break;
            case '1': printf("STP passenger</td>");    break;
            case '2': printf("STP freight</td>"); break;
            case '3': printf("STP trip</td>");    break;
            case '5': printf("STP bus</td>");     break;
            default:  printf("%s</td>", db_row[0][0]); break;
            }

            // ID & Link)
            printf("<td><a href=\"%sZ/%ld/%s\" class=\"linkbutton\">%ld</td>", URL_BASE, atol(db_row[0][7]), db_row[0][4], atol(db_row[0][7]));
            // Headcode if we know it)
            printf("<td>");
            if(db_row[0][3][0])
               printf("%s", db_row[0][3]);
            else if(db_row[0][17][0])
               printf("%s (%s)", db_row[0][17], db_row[0][18]);
            else
               printf("&nbsp;");
            printf("</td>");
               
            // CIF ID
            printf("<td>%s</td>", show_spaces(db_row[0][4]));
               
            // CIF STP indicator
            switch(db_row[0][5][0])
            {
            case 'C': printf("<td>Cancelled</td>"); break;
            case 'N': printf("<td>New</td>");          break;
            case 'O': printf("<td>Overlay</td>");      break;
            case 'P': printf("<td>Permanent</td>");    break;
            default:  printf("<td>%s</td>", db_row[0][5]);  break;
            }
               
            // From
            sprintf(query, "SELECT tiploc_code, departure, public_departure FROM cif_schedule_locations where cif_schedule_id = %u AND record_identity = 'LO'", id);
            printf("<td>");
            if(!db_query(query))
            {
                  db_result[1] = db_store_result();
                  
                  if((db_row[1] = mysql_fetch_row(db_result[1])))
                  {
                     printf("%s %s", location_name(db_row[1][0]), show_time(db_row[1][1]));
                     if(db_row[1][2][0]) printf("(%s)", show_time(db_row[1][2]));
                  }
                  mysql_free_result(db_result[1]);
               }
               printf("</td>");
               
               // To
               sprintf(query, "SELECT tiploc_code, arrival, public_arrival FROM cif_schedule_locations where cif_schedule_id = %u AND record_identity = 'LT'", id);
               printf("<td>");
               if(!db_query(query))
               {
                  db_result[1] = db_store_result();
               
                  if((db_row[1] = mysql_fetch_row(db_result[1])))
                  {
                     printf("%s %s", location_name(db_row[1][0]), show_time(db_row[1][1]));
                     if(db_row[1][2][0]) printf("(%s)", show_time(db_row[1][2]));
                  }
                  mysql_free_result(db_result[1]);
               }
               printf("</td>");

               // Days
               printf("<td>");
               if(db_row[0][ 8][0] == '1') printf("Mo ");
               if(db_row[0][ 9][0] == '1') printf("Tu ");
               if(db_row[0][10][0] == '1') printf("We ");
               if(db_row[0][11][0] == '1') printf("Th ");
               if(db_row[0][12][0] == '1') printf("Fr ");
               if(db_row[0][13][0] == '1') printf("Sa ");
               if(db_row[0][14][0] == '1') printf("Su ");
               printf("</td>");
               // Dates
               {
                  time_t start = atol(db_row[0][1]);
                  time_t end   = atol(db_row[0][2]);
                  time_t deleted = atol(db_row[0][15]);
                  time_t created = atol(db_row[0][16]);
                  printf("<td>%s", date_text(start, 0));
                  if(end != start) printf(" - %s", date_text(end, 0));
                  printf("</td>");
                  printf("<td>%s</td>", date_text(created, 0));
                  printf("<td>");
                  if(deleted < (now + 24*60*60)) 
                     printf("%s", date_text(deleted, 0));
                  else
                     printf("&nbsp;");
                  printf("</td></tr>\n");
               }
        }
      }
      mysql_free_result(db_result[0]);
      if(text_mode)
      {
         printf("\n</pre>\n");
      }
      else
      {
         printf("</table>");
      }
}

static void report_c(void)
{
   // p1 Headcode
   // p2 W for this week

   char q[512], query[512];

   sprintf(query, "SELECT created, cif_schedule_id, trust_id, deduced FROM trust_activation WHERE SUBSTRING(trust_id,3,4) = '%s'", parameters[1]);
   if(parameters[2][0] == 'W')
   {
      sprintf(q, " AND created > %ld", now - (7*24*60*60));
      strcat(query, q);
   }

   strcat(query, " ORDER BY CREATED");

   if(db_query(query))
   {
   }
   else
   {
      db_result[0] = db_store_result();
      if(text_mode)
      {
         printf("<pre>Activated              TRUST ID       Schedule ID\n");
      }
      else
      {
         printf("<table class=\"result-table\"><tr><th>Activated</th><th>TRUST ID</th><th>Schedule ID Number</th><th>From</th><th>To</th></tr>\n");
      }
      while((db_row[0] = mysql_fetch_row(db_result[0])))
      {
         if(text_mode)
         {
            printf("%17s %15s %15s%s  ", time_text(atol(db_row[0][0]), true), db_row[0][2], db_row[0][1], (db_row[0][3][0] == '0')?"":" Deduced");
         }
         else
         {
            printf("<tr class=\"result-table\"><td>%s</td><td>%s</td><td><a class=\"linkbutton\" href=\"%sZ/%ld\">%ld</a> %s</td>\n", time_text(atol(db_row[0][0]), true), db_row[0][2], URL_BASE, atol(db_row[0][1]), atol(db_row[0][1]), (db_row[0][3][0] == '0')?"":" Deduced");
         }
         dword id = atol(db_row[0][1]);
         if(id)
         {
            sprintf(query, "SELECT tiploc_code, departure, public_departure FROM cif_schedule_locations where cif_schedule_id = %u AND record_identity = 'LO'", id);
            if(!text_mode) printf("<td>");
            if(!db_query(query))
            {
               db_result[1] = db_store_result();
               
               if((db_row[1] = mysql_fetch_row(db_result[1])))
               {
                  printf("%s %s", location_name(db_row[1][0]), show_time(db_row[1][1]));
                  if(db_row[1][2][0]) printf("(%s)", show_time(db_row[1][2]));
               }
               mysql_free_result(db_result[1]);
            }
            if(!text_mode) printf("</td>");
            
            // To
            sprintf(query, "SELECT tiploc_code, arrival, public_arrival FROM cif_schedule_locations where cif_schedule_id = %u AND record_identity = 'LT'", id);
            if(text_mode) printf("  to  ");
            else printf("<td>");
            if(!db_query(query))
            {
               db_result[1] = db_store_result();
               
               if((db_row[1] = mysql_fetch_row(db_result[1])))
               {
                  printf("%s %s", location_name(db_row[1][0]), show_time(db_row[1][1]));
                  if(db_row[1][2][0]) printf("(%s)", show_time(db_row[1][2]));
               }
               mysql_free_result(db_result[1]);
            }
            if(!text_mode) printf("</td>");
         }
         else
         {
            if(!text_mode) printf("<td>&nbsp;</td><td>&nbsp;</td>");
         }
         if(text_mode) printf("\n");
         else printf("</tr>\n");
      }
      mysql_free_result(db_result[0]);
      if(text_mode)
      {
         printf("\n</pre>\n");
      }
      else
      {
         printf("</table>");
      }
   }
}

static void report_d(void)
{
   // p1 Schedule ID
   // p2 W for this week
   char q[512], query[512];

   sprintf(query, "SELECT created, cif_schedule_id, trust_id, deduced FROM trust_activation WHERE cif_schedule_id = %s", parameters[1]);
   if(parameters[2][0] == 'W')
   {
      sprintf(q, " AND created > %ld", now - (7*24*60*60));
      strcat(query, q);
   }

   strcat(query, " ORDER BY CREATED");

   if(db_query(query))
   {
   }
   else
   {
      db_result[0] = db_store_result();
      if(text_mode)
      {
         printf("<pre>Activated                 TRUST ID       Schedule ID\n");
      }
      else
      {
         printf("<table class=\"result-table\"><tr><th>Activated</th><th>TRUST ID</th><th>Schedule ID Number</th></tr>\n");
      }
      while((db_row[0] = mysql_fetch_row(db_result[0])))
      {
         if(text_mode)
         {
            printf("%20s %15s %15s%s\n", time_text(atol(db_row[0][0]), true), db_row[0][2], db_row[0][1], (db_row[0][3][0] == '0')?"":" Deduced");
         }
         else
         {
            printf("<tr class=\"result-table\"><td>%s</td><td>%s</td><td><a class=\"linkbutton\" href=\"%sZ/%ld\">%ld</a> %s</td></tr>\n", time_text(atol(db_row[0][0]), true), db_row[0][2], URL_BASE, atol(db_row[0][1]), atol(db_row[0][1]), (db_row[0][3][0] == '0')?"":" Deduced");
         }
      }
      mysql_free_result(db_result[0]);
      if(text_mode)
      {
         printf("\n</pre>\n");
      }
      else
      {
         printf("</table>");
      }
   }
}

static void report_e(void)
{
   // p1 Headcode
   // p2 W for this week

   char query[1024], q1[1024];

   sprintf(query, "SELECT train_status, schedule_start_date, schedule_end_date, signalling_id, CIF_train_uid, CIF_stp_indicator, update_id, id, ");
   strcat(query, "runs_mo, runs_tu, runs_we, runs_th, runs_fr, runs_sa, runs_su, deleted, created, deduced_headcode, deduced_headcode_status");
   strcat(query,  " FROM cif_schedules WHERE ");
   // Sanitise
   char safe[256];
   db_real_escape_string(safe, parameters[1], strlen(parameters[1]));
   sprintf(q1, "(signalling_id = '%s' OR deduced_headcode = '%s') ", safe, safe);

   strcat(query, q1);

   // This week
   if(parameters[2][0] == 'W')
   {
      sprintf(q1, " AND schedule_start_date < %ld AND schedule_end_date > %ld and deleted > %ld", now+(7*24*60*60), now-(7*24*60*60), now-(7*24*60*60));
      strcat(query, q1);
   }
   strcat(query, " ORDER BY schedule_start_date");
   if(db_query(query))
   {
   }
   else
   {
      report_be();
   }
}

static void report_f(void)
{
   // [1] Garner schedule ID
   // [2] Location

   char query[1024];
   word stanox = 0;

   // Find stanox
   char * location  = parameters[2];
   word done = false;
   // Decode 3alpha and search for friendly name match
   // Process location NLC to TIPLOC
   if(strlen(location) == 3)
   {
      sprintf(query, "SELECT stanox FROM corpus WHERE 3alpha = '%s'", location);
      if(!db_query(query))
      {
         db_result[0] = db_store_result();
         if((db_row[0] = mysql_fetch_row(db_result[0])) && db_row[0][0][0]) 
         {
            stanox = atoi(db_row[0][0]);
            done = true;
         }
         mysql_free_result(db_result[0]);
      }
   }
   if(!done && strlen(location) < 8)
   {
      // Try TIPLOC
      sprintf(query, "SELECT stanox FROM corpus WHERE tiploc = '%s'", location);
      if(!db_query(query))
      {
         db_result[0] = db_store_result();
         if((db_row[0] = mysql_fetch_row(db_result[0])) && db_row[0][0][0]) 
         {
            stanox = atoi(db_row[0][0]);
            done = true;
         }
         mysql_free_result(db_result[0]);
      }
   }
   if(!done && strlen(location) < 6)
   {
      // Try STANOX
      word try = atoi(location);
      if(try)
      {
         sprintf(query, "SELECT stanox FROM corpus WHERE stanox = %s", location);
         if(!db_query(query))
         {
            db_result[0] = db_store_result();
            if((db_row[0] = mysql_fetch_row(db_result[0])) && db_row[0][0][0]) 
            {
               stanox = atoi(db_row[0][0]);
               done = true;
            }
            mysql_free_result(db_result[0]);
         }
      }
   }
   if(!done)
   {
      // Try fn
      sprintf(query, "SELECT stanox, fn FROM corpus WHERE fn like '%%%s%%' and stanox != 0 order by fn", location);
      if(!db_query(query))
      {
         db_result[0] = db_store_result();
         word found = mysql_num_rows(db_result[0]);
               
         if(found == 1 && (db_row[0] = mysql_fetch_row(db_result[0])) && db_row[0][0][0]) 
         {
            stanox = atoi(db_row[0][0]);
            done = true;
            
            mysql_free_result(db_result[0]);
         }
         else if(found > 0)
         {
            printf("<p>Select desired location</p>\n");

            printf("<table>");
            printf("<tr class=\"small-table\"><th>STANOX</th><th>Location</th></tr>\n");

            while((db_row[0] = mysql_fetch_row(db_result[0])) ) 
            {
               printf("<tr class=\"small-table\"><td>%s</td><td><a href=\"%sF/%s/%s\" class=\"linkbutton\">%s</a></td></tr>\n", db_row[0][0],  URL_BASE, parameters[1], db_row[0][0], db_row[0][1]);
            }
            printf("</table>");
            mysql_free_result(db_result[0]);
         
            return;
         }
      }
   }

   // Sumarise report
   printf("<h4>Performance of %s", describe_schedule(atol(parameters[1])));
   printf(" at %s</h4>\n", location_name_stanox(stanox));

   // Find Activations
   sprintf(query, "SELECT created, trust_id, deduced FROM trust_activation WHERE cif_schedule_id = '%s' ORDER BY CREATED", parameters[1]);
   if(db_query(query))
   {
   }
   else
   {
      db_result[0] = db_store_result();
      if(text_mode)
      {
         printf("<pre>Activated                 TRUST ID       Schedule ID\n");
      }
      else
      {
         printf("<table class=\"result-table\"><tr><th>Activated</th><th>TRUST ID</th><th>Report</th><th>&nbsp;</th></tr>\n");
      }
      while((db_row[0] = mysql_fetch_row(db_result[0])))
      {
         time_t when = atol(db_row[0][0]);
         printf("<tr class=\"result-table\"><td>%s</td><td>%s</td>", day_date_text(when, true), db_row[0][1]);
      
         sprintf(query, "SELECT timetable_variation, flags FROM trust_movement where trust_id = '%s' AND loc_stanox = %d AND created < %ld AND created > %ld ORDER BY (flags & 0x3) DESC",db_row[0][1], stanox, when + 8*24*60*60, when - 8*24*60*60);
         if(db_query(query))
         {
         }
         else
         {
            db_result[1] = db_store_result();
            if((db_row[1] = mysql_fetch_row(db_result[1])))
            {
               printf("<td>%s \n", db_row[1][0]);
               word flags = atoi(db_row[1][1]);
               switch(flags & 0x0018)
               {
               case 0x0000: printf("Early</td>"); break;
               case 0x0008: printf("On time</td>"); break;
               case 0x0010: printf("Late</td>"); break;
               case 0x0018: printf("Off route</td>"); break;
               }
            }
            else
            {
               // Check for a cape
               mysql_free_result(db_result[1]);
               sprintf(query, "select created from trust_cancellation WHERE trust_id = '%s' AND created < %ld AND created > %ld AND NOT reinstate",db_row[0][1], when + 8*24*60*60, when - 8*24*60*60);
               if(db_query(query))
               {
               }
               else
               {
                  db_result[1] = db_store_result();
                  if((db_row[1] = mysql_fetch_row(db_result[1])))
                  {
                     printf("<td>Cancelled.</td>\n");
                  }
                  else
                  {
                     printf("<td>No report.</td>\n");
                  }
               }
            }

            printf("<td><a href=\"%strain/%s/%s\" class=\"linkbutton\">Details</a></td></tr>\n", LIVERAIL_URL_BASE, parameters[1],date_text(when, true));
            mysql_free_result(db_result[1]);
         }
      }
      mysql_free_result(db_result[0]);
      if(text_mode)
      {
         printf("\n</pre>\n");
      }
      else
      {
         printf("</table>");
      }
   }
}

static char * location_name(const char * const tiploc)
{
   // Not re-entrant
   char query[256];
   static char result[256];

   sprintf(query, "select fn from corpus where tiploc = '%s'", tiploc);
   db_query(query);
   db_result[2] = db_store_result();
   if((db_row[2] = mysql_fetch_row(db_result[2])) && db_row[2][0][0]) 
   {
      strncpy(result, db_row[2][0], 127);
      result[127] = '\0';
   }
   else
   {
      strcpy(result, tiploc);
   }
   mysql_free_result(db_result[2]);

   return result;
}

static char * location_name_stanox(const word stanox)
{
   // Not re-entrant
   char query[256];
   static char result[256];

   sprintf(query, "select fn from corpus where stanox = %d", stanox);
   db_query(query);
   db_result[2] = db_store_result();
   if((db_row[2] = mysql_fetch_row(db_result[2])) && db_row[2][0][0]) 
   {
      strncpy(result, db_row[2][0], 127);
      result[127] = '\0';
   }
   else
   {
      sprintf(result, "%d", stanox);
   }
   mysql_free_result(db_result[2]);

   return result;
}

static char * describe_schedule(const dword id)
{
   // Shows WTT times only
   static char result[1024];

   char query[256];
   
   result[0] = '\0';

   sprintf(query, "SELECT tiploc_code, departure, public_departure FROM cif_schedule_locations where cif_schedule_id = %u AND record_identity = 'LO'", id);
   if(!db_query(query))
   {
      db_result[1] = db_store_result();
      
      if((db_row[1] = mysql_fetch_row(db_result[1])))
      {
         sprintf(result, "%s %s to ", show_time(db_row[1][1]), location_name(db_row[1][0]));
      }
      mysql_free_result(db_result[1]);
   }

   sprintf(query, "SELECT tiploc_code, arrival, public_arrival FROM cif_schedule_locations where cif_schedule_id = %u AND record_identity = 'LT'", id);
   if(!db_query(query))
   {
      db_result[1] = db_store_result();
               
      if((db_row[1] = mysql_fetch_row(db_result[1])))
      {
         strcat(result, location_name(db_row[1][0]));
      }
      mysql_free_result(db_result[1]);
   }
   return result;
}
