/*
    Copyright (C) 2015, 2016, 2017, 2018, 2020, 2021, 2022 Phil Wieland

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

#define MAX_SHOWN 500

#ifndef RELEASE_BUILD
#define BUILD "3c22p"
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
static void report_g(void);
static void report_h(void);
static char * location_name(const char * const tiploc);
static char * location_name_stanox(const dword stanox);
static char * describe_schedule(const dword id);
static time_t parse_date(const char * const s);

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

#define QUERIES 2
MYSQL_RES * db_result[QUERIES];
MYSQL_ROW      db_row[QUERIES];

int main()
{
   char zs[1024];
   now = time(NULL);

   qword start_time = time_ms();
   mode = '-';
   text_mode = false;

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
   if(parms && !strncmp(zs + i, "rail/query", 10)) i += 10;
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
         // Due to the simple nature of the queries we can use brute force here to bar Little Bobby Tables and others...
         if((zs[i] >= 'A' && zs[i] <= 'Z') || (zs[i] >= 'a' && zs[i] <= 'z') || (zs[i] >= '0' && zs[i] <= '9') || zs[i] == ' ' || zs[i] == '-')
            parameters[j][k++] = toupper(zs[i++]);
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
   db_init(conf[conf_db_server], conf[conf_db_user], conf[conf_db_password], conf[conf_db_name], DB_MODE_NORMAL);

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

   case 'G':
      report_g();
      break;

   case 'H':
      report_h();
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
   struct tm * broken = localtime(&now);
   printf("<br><div class=\"licence\">&copy;%04d.  Contains Information of Network Rail Infrastructure Limited licensed under the following licence: <a href=\"http://www.networkrail.co.uk/data-feeds/terms-and-conditions\"> Click Here</a>.</br>", broken->tm_year + 1900);
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
          "<br>Only this week. <input type=\"checkbox\" id=\"C-w\" value=\"0\">"
          "<br><button class=\"cp-button\" onclick=\"C_onclick();\">Search</button>\n"

          "</td><td width=\"5%%\">&nbsp;</td><td class=\"control-panel-row\">\n"
          "<h4>[D]TRUST Activations by Schedule</h4>\n");
   printf("Schedule ID number <input type=\"text\" id=\"D-s\" size=\"7\" maxlength=\"9\" value=\"%s\" onkeydown=\"if(event.keyCode == 13) D_onclick();\">", parameters[1]);
   printf("<br>Only this week. <input type=\"checkbox\" id=\"D-w\" value=\"0\">"
          "<br><button class=\"cp-button\" onclick=\"D_onclick();\">Search</button>\n"
          "</td></tr>\n"

          "<tr valign=\"top\"><td class=\"control-panel-row\">"
          "<h4>[E]Schedules by headcode</h4>\n"
          "Headcode <input type=\"text\" id=\"E-v\" size=\"3\" maxlength=\"4\" value=\"\" onkeydown=\"if(event.keyCode == 13) E_onclick();\">\n"
          "<br>Only if valid this week.<input type=\"checkbox\" id=\"E-w\" value=\"0\">\n"
          "<br><button class=\"cp-button\" onclick=\"E_onclick();\">Search</button>\n"

          "</td><td width=\"5%%\">&nbsp;</td><td class=\"control-panel-row\">\n"
          "<h4>[F]Service performance</h4>\n");
   printf("Schedule ID number <input type=\"text\" id=\"F-id\" size=\"7\" maxlength=\"9\" value=\"%s\" onkeydown=\"if(event.keyCode == 13) F_onclick();\">&nbsp; &nbsp\n", parameters[1]);
   printf("<br>Or CIF UID <input type=\"text\" id=\"F-uid\" size=\"7\" maxlength=\"9\" value=\"%s\" onkeydown=\"if(event.keyCode == 13) F_onclick();\">&nbsp; &nbsp\n", parameters[2]);
   printf("<br>Location <input type=\"text\" id=\"F-l\" size=\"20\" maxlength=\"50\" value=\"\" onkeydown=\"if(event.keyCode == 13) F_onclick();\">&nbsp; &nbsp\n");
   printf("<br><button class=\"cp-button\" onclick=\"F_onclick();\">Report</button>\n"
          
          "</td><td width=\"5%%\">&nbsp;</td><td class=\"control-panel-row\">\n"
          "<h4>[G]All trains at location</h4>\n");
   printf("Location <input type=\"text\" id=\"G-l\" size=\"20\" maxlength=\"50\" value=\"\" onkeydown=\"if(event.keyCode == 13) G_onclick();\">&nbsp; &nbsp\n");
   printf("<br><button class=\"cp-button\" onclick=\"G_onclick();\">Report</button>\n"

          "</td><td width=\"5%%\">&nbsp;</td><td class=\"control-panel-off\">\n"
          "&nbsp;</td></tr><tr><td colspan=\"7\" class=\"control-panel-row\">"
          "<h4>[H]Schedules with specific routing</h4>\n");
   printf("Via location <input type=\"text\" id=\"H-l0\" size=\"18\" maxlength=\"50\" value=\"\" onkeydown=\"if(event.keyCode == 13) H_onclick();\">&nbsp; &nbsp\n");
   printf("path <input type=\"text\" id=\"H-l0p\" size=\"2\" maxlength=\"6\" value=\"\" onkeydown=\"if(event.keyCode == 13) H_onclick();\">&nbsp; &nbsp\n");
   printf("line <input type=\"text\" id=\"H-l0l\" size=\"2\" maxlength=\"6\" value=\"\" onkeydown=\"if(event.keyCode == 13) H_onclick();\">&nbsp; &nbsp\n");
   printf(" and via location <input type=\"text\" id=\"H-l1\" size=\"18\" maxlength=\"50\" value=\"\" onkeydown=\"if(event.keyCode == 13) H_onclick();\">&nbsp; &nbsp\n");
   printf(" and not via location <input type=\"text\" id=\"H-l2\" size=\"18\" maxlength=\"50\" value=\"\" onkeydown=\"if(event.keyCode == 13) H_onclick();\">&nbsp; &nbsp\n");
   printf("<br>Runs between <input type=\"text\" id=\"H-d0\" size=\"5\" maxlength=\"8\" placeholder=\"dd/mm/yy\" value=\"\" onkeydown=\"if(event.keyCode == 13) H_onclick();\"> and <input type=\"text\" id=\"H-d1\" size=\"5\" maxlength=\"8\" placeholder=\"dd/mm/yy\" value=\"\" onkeydown=\"if(event.keyCode == 13) H_onclick();\">");
   printf("<br>Only passenger trains.<input type=\"checkbox\" id=\"H-p\" value=\"0\">\n");
   printf("&nbsp;&nbsp;&nbsp; Only 'permanent' trains.<input type=\"checkbox\" id=\"H-SP\" value=\"0\">\n");
   printf("&nbsp;&nbsp;&nbsp; Only 'overlay' trains.<input type=\"checkbox\" id=\"H-SO\" value=\"0\">\n");
   //printf("&nbsp;&nbsp;&nbsp; Exclude STP C trains.<input type=\"checkbox\" id=\"H-SXC\" value=\"0\">\n");
   printf("&nbsp;&nbsp;&nbsp; (In the latter two cases, also tick Only passenger trains if required.\n");
   printf("<br><button class=\"cp-button\" onclick=\"H_onclick();\">Report</button>\n"
          "<br>Leave any field blank for don't care."

          "</td></tr></table>"
          );

   printf("<input type=\"hidden\" id=\"date\" value=\"%s\"/>\n", date_text(time(NULL), true));

   printf("<div class=\"help-text-box\">");
   printf("<p><b>Open Rail real time railway data advanced database queries.</b></p>"
          "<p>This part of the Open Rail system is experimental, and still under development.  Some queries may return incorrect or confusing results.  A degree of understanding of the inner workings of the railway systems behind this data (The &quot;CIF&quot; schedule database, and &quot;TRUST&quot;.) may be needed to understand the results of some queries.</p>"
          "<p>A schedule ID number is a unique identification number for a schedule in the Open Rail database.  When you see one in the results of one of these queries, click on it and you will be returned to this page with the number filled in ready for queries [A], [D] and [F].</p>"
"<p>A CIF UID is the ID for a schedule in Network Rail's database, it consists of a letter or a space followed by five digits, e.g. Y63320.</p>"
"<p>Report [F] shows the performance (On time/late/cancelled) of a particular service at the specified location.</p>"
"<p>Report [G] shows the most recent 250 trains to pass or call at a location.  This is aimed at researching the historical useage of a quiet part of the network.</p>"          
"<p>Report [H] shows schedules that meet all the specified paramters.  This is aimed at researching the useage of a particular route.</p>"          
"<p>The Open Rail database contains live running data for the past three years and future schedule information up to the limit of Network Rail's published information.</p>"
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
   // [2] CIF UID
   // [3] Location

   char query[1024];
   dword stanox = 0;

   // Find stanox
   char * location  = parameters[3];
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
            stanox = atol(db_row[0][0]);
            if(stanox) done = true;
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
            stanox = atol(db_row[0][0]);
            _log(GENERAL, "Try TIPLOC found %s %d.", db_row[0][0], stanox);
            if(stanox) done = true;
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
               stanox = atol(db_row[0][0]);
               if(stanox) done = true;
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
            stanox = atol(db_row[0][0]);
            if(stanox) done = true;
            
            mysql_free_result(db_result[0]);
         }
         else if(found > 0)
         {
            printf("<p>Select desired location</p>\n");

            printf("<table>");
            printf("<tr class=\"small-table\"><th>STANOX</th><th>Location</th></tr>\n");

            while((db_row[0] = mysql_fetch_row(db_result[0])) ) 
            {
               printf("<tr class=\"small-table\"><td>%s</td><td><a href=\"%sF/%s/%s/%s\" class=\"linkbutton\">%s</a></td></tr>\n", db_row[0][0],  URL_BASE, parameters[1], parameters[2], db_row[0][0], db_row[0][1]);
            }
            printf("</table>");
            mysql_free_result(db_result[0]);
         
            return;
         }
      }
   }

   // Note.  If ID provided, ignore UID field.
   if(parameters[1][0])
   {
      // Sumarise report
      printf("<h4>Performance of %s", describe_schedule(atol(parameters[1])));
      printf(" at %s</h4>\n", location_name_stanox(stanox));
      sprintf(query, "SELECT created, trust_id, deduced, cif_schedule_id FROM trust_activation WHERE cif_schedule_id = '%s' ORDER BY CREATED", parameters[1]);
   }
   else
   {
      // Sanitise
      char safe[250], safe1[256];
      db_real_escape_string(safe, parameters[2], strlen(parameters[2]));
      // Insert missing leading space if necessary
      if(strlen(parameters[2]) == 5)
         sprintf(safe1, " %s", safe);
      else
         strcpy(safe1, safe);
      printf("<h4>Performance of %s", show_spaces(safe1));
      printf(" at %s</h4>\n", location_name_stanox(stanox));
      sprintf(query, "SELECT trust_activation.created, trust_id, deduced, cif_schedules.id FROM cif_schedules,trust_activation WHERE cif_schedules.id=trust_activation.cif_schedule_id AND cif_schedules.CIF_train_uid = '%s' ORDER BY trust_activation.created", safe1);
   }

   // Find Activations
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

            printf("<td><a href=\"%strain/%s/%s\" class=\"linkbutton\">Details</a></td></tr>\n", LIVERAIL_URL_BASE, db_row[0][3], date_text(when, true));
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

static void report_g(void)
{
   // [1] Location

   // TODO This assumes only one stanox for the location ???
   
   char query[1024];
   dword stanox = 0;
   word first = true;
   word count = 0;
   
   // Find stanox
   char * location  = parameters[1];
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
            stanox = atol(db_row[0][0]);
            if(stanox) done = true;
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
            stanox = atol(db_row[0][0]);
            _log(GENERAL, "Try TIPLOC found %s %d.", db_row[0][0], stanox);
            if(stanox) done = true;
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
               stanox = atol(db_row[0][0]);
               if(stanox) done = true;
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
            stanox = atol(db_row[0][0]);
            if(stanox) done = true;
            
            mysql_free_result(db_result[0]);
         }
         else if(found > 0)
         {
            printf("<p>Select desired location</p>\n");

            printf("<table>");
            printf("<tr class=\"small-table\"><th>STANOX</th><th>Location</th></tr>\n");

            while((db_row[0] = mysql_fetch_row(db_result[0])) ) 
            {
               printf("<tr class=\"small-table\"><td>%s</td><td><a href=\"%sG/%s\" class=\"linkbutton\">%s</a></td></tr>\n", db_row[0][0],  URL_BASE, db_row[0][0], db_row[0][1]);
            }
            printf("</table>");
            mysql_free_result(db_result[0]);
         
            return;
         }
      }
   }

   // if(!done) TODO

   printf("<h4>Most recent trains reported at %s</h4>", location_name_stanox(stanox));

   sprintf(query, "SELECT m.actual_timestamp, a.cif_schedule_id FROM trust_movement AS m INNER JOIN trust_activation AS a ON a.trust_id = m.trust_id AND IF(a.created > m.created, a.created - m.created, m.created - a.created) < 15*24*60*60 WHERE m.loc_stanox = '%d' GROUP BY a.cif_schedule_id,date(from_unixtime(m.actual_timestamp)) ORDER BY m.actual_timestamp DESC LIMIT 256", stanox);

   // Known bug:  This query has to find ALL calls, then sort them and then return the 256 most recent.  If a busy location is given, this takes a long time.

   // Find Calls / Passes   
   if(db_query(query))
   {
   }
   else
   {
      db_result[0] = db_store_result();
      while((db_row[0] = mysql_fetch_row(db_result[0])) && count++ < 250)
      {
         if(first)
         {
            printf("<table class=\"result-table\"><tr><th>When</th><th>Train</th><th>&nbsp;</th></tr>\n");
            first = false;
         }
         time_t when = atol(db_row[0][0]);
         printf("<tr class=\"result-table\"><td>%s</td><td>%s</td>", time_text(when, true), describe_schedule(atol(db_row[0][1])));
         
         printf("<td><a href=\"%strain/%s/%s\" class=\"linkbutton\">Details</a></td></tr>\n", LIVERAIL_URL_BASE, db_row[0][1], date_text(when, true));
      }
      mysql_free_result(db_result[0]);
      if(!first)
      {
         printf("</table>");
         if(db_row[0]) printf("<h4>Earlier services not shown.</h4>");
      }
      else
      {
         printf("<h4>None found.</h4>");
      }
   }
}

static void report_h(void)
{
   // [1] Location 0 Can be blank
   // [2] Path Can be blank
   // [3] Line Can be blank
   // [4] Location 1 Can be blank
   // [5] Location 2 Can be blank
   // [6] Passenger only
   // [7] STP restrictions
   // [8] Date 0 Can be blank
   // [9] Date 1 Can be blank

   char query[1024], q1[1024];
   char path[256], line[256];
   char tiploc0[16], tiploc1[16], tiploc2[16];
   word first = true;
   word passenger, only_stp_p, only_stp_o, not_stp_c, i;
   
   // Find TIPLOC 0
   char * location  = parameters[1];
   word done = false;
   // Decode 3alpha and search for friendly name match
   // Process location NLC to TIPLOC
   if(location[0])
   {
      if(strlen(location) == 3)
      {
         sprintf(query, "SELECT tiploc FROM corpus WHERE 3alpha = '%s'", location);
         if(!db_query(query))
         {
            db_result[0] = db_store_result();
            if((db_row[0] = mysql_fetch_row(db_result[0])) && db_row[0][0][0]) 
            {
               strcpy(tiploc0, db_row[0][0]);
               if(tiploc0[0]) done = true;
            }
            mysql_free_result(db_result[0]);
         }
      }
      if(!done && strlen(location) < 8)
      {
         // Try TIPLOC
         sprintf(query, "SELECT tiploc FROM corpus WHERE tiploc = '%s'", location);
         if(!db_query(query))
         {
            db_result[0] = db_store_result();
            if((db_row[0] = mysql_fetch_row(db_result[0])) && db_row[0][0][0]) 
            {
               strcpy(tiploc0, db_row[0][0]);
               if(tiploc0[0]) done = true;
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
                  strcpy(tiploc0, db_row[0][0]);
                  if(tiploc0[0]) done = true;
               }
               mysql_free_result(db_result[0]);
            }
         }
      }
      if(!done)
      {
         // Try fn
         sprintf(query, "SELECT tiploc, fn FROM corpus WHERE fn like '%%%s%%' and tiploc != '' order by fn", location);
         if(!db_query(query))
         {
            db_result[0] = db_store_result();
            word found = mysql_num_rows(db_result[0]);
            
            if(found == 1 && (db_row[0] = mysql_fetch_row(db_result[0])) && db_row[0][0][0]) 
            {
               strcpy(tiploc0, db_row[0][0]);
               if(tiploc0[0]) done = true;
               
               mysql_free_result(db_result[0]);
            }
            else if(found > 0)
            {
               printf("<h4>Select first location</h4>\n");
               
               printf("<table>");
               printf("<tr class=\"small-table\"><th>TIPLOC</th><th>Location</th></tr>\n");
               
               while((db_row[0] = mysql_fetch_row(db_result[0])) ) 
               {
                  //                                    tiploc                  new p2 p3 p4 p5 p6 p7 p8 p9
                  printf("<tr class=\"small-table\"><td>%s</td><td><a href=\"%sH/%s/%s/%s/%s/%s/%s/%s/%s/%s\" class=\"linkbutton\">%s</a></td></tr>\n", db_row[0][0],  URL_BASE, db_row[0][0], parameters[2], parameters[3], parameters[4], parameters[5], parameters[6], parameters[7], parameters[8], parameters[9], db_row[0][1]);
               }
               printf("</table>");
               mysql_free_result(db_result[0]);
               
               return;
            }
         }
      }
      if(!done)
      {
         printf("<p>Location \"%s\" not recognised.</p>", parameters[1]);
         return;
      }
   }
   else
   {
      tiploc0[0] = '\0';
   }
   
   // Find TIPLOC 1
   location  = parameters[4];
   done = false;
   if(location[0])
   {
      // Decode 3alpha and search for friendly name match
      // Process location NLC to TIPLOC
      if(strlen(location) == 3)
      {
         sprintf(query, "SELECT tiploc FROM corpus WHERE 3alpha = '%s'", location);
         if(!db_query(query))
         {
            db_result[0] = db_store_result();
            if((db_row[0] = mysql_fetch_row(db_result[0])) && db_row[0][0][0]) 
            {
               strcpy(tiploc1, db_row[0][0]);
               if(tiploc1[0]) done = true;
            }
            mysql_free_result(db_result[0]);
         }
   }
   if(!done && strlen(location) < 8)
   {
      // Try TIPLOC
      sprintf(query, "SELECT tiploc FROM corpus WHERE tiploc = '%s'", location);
      if(!db_query(query))
      {
         db_result[0] = db_store_result();
         if((db_row[0] = mysql_fetch_row(db_result[0])) && db_row[0][0][0]) 
         {
            strcpy(tiploc1, db_row[0][0]);
            if(tiploc1[0]) done = true;
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
               strcpy(tiploc1, db_row[0][0]);
               if(tiploc1[0]) done = true;
            }
            mysql_free_result(db_result[0]);
         }
      }
   }
   if(!done)
   {
      // Try fn
      sprintf(query, "SELECT tiploc, fn FROM corpus WHERE fn like '%%%s%%' and tiploc != '' order by fn", location);
      if(!db_query(query))
      {
         db_result[0] = db_store_result();
         word found = mysql_num_rows(db_result[0]);
               
         if(found == 1 && (db_row[0] = mysql_fetch_row(db_result[0])) && db_row[0][0][0]) 
         {
            strcpy(tiploc1, db_row[0][0]);
            if(tiploc1[0]) done = true;
            
            mysql_free_result(db_result[0]);
         }
         else if(found > 0)
         {
            printf("<h4>Select second location</h4>\n");

            printf("<table>");
            printf("<tr class=\"small-table\"><th>TIPLOC</th><th>Location</th></tr>\n");

            while((db_row[0] = mysql_fetch_row(db_result[0])) ) 
            {
               //                                    tiploc                   p1 new p3 p4 p5 p6 p7 p8 p9
               printf("<tr class=\"small-table\"><td>%s</td><td><a href=\"%sH/%s/%s/%s/%s/%s/%s/%s/%s/%s\" class=\"linkbutton\">%s</a></td></tr>\n", db_row[0][0],  URL_BASE, parameters[1], parameters[2], parameters[3], db_row[0][0], parameters[5], parameters[6], parameters[7], parameters[8], parameters[9], db_row[0][1]);
            }
            printf("</table>");
            mysql_free_result(db_result[0]);
         
            return;
         }
      }
   }
   if(!done)
   {
      printf("<p>Location \"%s\" not recognised.</p>", parameters[4]);
      return;
      }
   }
   else
   {
      tiploc1[0] = '\0';
   }
   
   // Find TIPLOC 2   CAN BE BLANK
   location  = parameters[5];
   if(location[0])
   {
      done = false;
      // Decode 3alpha and search for friendly name match
      // Process location NLC to TIPLOC
      if(strlen(location) == 3)
      {
         sprintf(query, "SELECT tiploc FROM corpus WHERE 3alpha = '%s'", location);
         if(!db_query(query))
         {
            db_result[0] = db_store_result();
            if((db_row[0] = mysql_fetch_row(db_result[0])) && db_row[0][0][0]) 
            {
               strcpy(tiploc2, db_row[0][0]);
               if(tiploc2[0]) done = true;
            }
            mysql_free_result(db_result[0]);
         }
      }
      if(!done && strlen(location) < 8)
      {
         // Try TIPLOC
         sprintf(query, "SELECT tiploc FROM corpus WHERE tiploc = '%s'", location);
         if(!db_query(query))
         {
            db_result[0] = db_store_result();
            if((db_row[0] = mysql_fetch_row(db_result[0])) && db_row[0][0][0]) 
            {
               strcpy(tiploc2, db_row[0][0]);
               if(tiploc2[0]) done = true;
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
                  strcpy(tiploc2, db_row[0][0]);
                  if(tiploc2[0]) done = true;
               }
               mysql_free_result(db_result[0]);
            }
         }
      }
      if(!done)
      {
         // Try fn
         sprintf(query, "SELECT tiploc, fn FROM corpus WHERE fn like '%%%s%%' and tiploc != '' order by fn", location);
         if(!db_query(query))
         {
            db_result[0] = db_store_result();
            word found = mysql_num_rows(db_result[0]);
            
            if(found == 1 && (db_row[0] = mysql_fetch_row(db_result[0])) && db_row[0][0][0]) 
            {
               strcpy(tiploc2, db_row[0][0]);
               if(tiploc2[0]) done = true;
               
               mysql_free_result(db_result[0]);
            }
            else if(found > 0)
            {
               printf("<h4>Select \"not via\" location</h4>\n");

               printf("<table>");
               printf("<tr class=\"small-table\"><th>TIPLOC</th><th>Location</th></tr>\n");

               while((db_row[0] = mysql_fetch_row(db_result[0])) ) 
               {
                  //                                    tiploc                   p1 p2 p3 p4new p6 p7 p8 p9
                  printf("<tr class=\"small-table\"><td>%s</td><td><a href=\"%sH/%s/%s/%s/%s/%s/%s/%s/%s/%s\" class=\"linkbutton\">%s</a></td></tr>\n", db_row[0][0],  URL_BASE, parameters[1], parameters[2], parameters[3], parameters[4], db_row[0][0], parameters[6], parameters[7], parameters[8], parameters[9], db_row[0][1]);
               }
               printf("</table>");
               mysql_free_result(db_result[0]);
               
               return;
            }
         }
      }
      if(!done)
      {
         printf("<p>Location \"%s\" not recognised.</p>", parameters[5]);
         return;
      }
   }
   else tiploc2[0] = '\0';

   strcpy(path, parameters[2]);
   strcpy(line, parameters[3]);

   // Passenger only
   passenger = (parameters[6][0] == 'P');

   // STP
   only_stp_p = only_stp_o = not_stp_c = false;
   for(i=0; i < strlen(parameters[7]); i++)
   {
      if(parameters[7][i] == 'P') only_stp_p = true;
      if(parameters[7][i] == 'O') only_stp_o = true;
      //if(parameters[7][i] == 'C') not_stp_c  = true;
   }
   
   // Validate dates
   time_t from = parse_date(parameters[8]);
   if(from) from -= 6*60*60;
   time_t to   = parse_date(parameters[9]);
   if(to)   to   += 6*60*60;
   else  to = 0xffffffffL;
   

   printf("<h2>Matching Schedules</h2>");

   
   /*
   printf("<p>Parameter 1 = \"%s\".</p>", parameters[1]);
   printf("<p>Parameter 2 = \"%s\".</p>", parameters[2]);
   printf("<p>Parameter 3 = \"%s\".</p>", parameters[3]);
   printf("<p>Parameter 4 = \"%s\".</p>", parameters[4]);
   printf("<p>Parameter 5 = \"%s\".</p>", parameters[5]);
   printf("<p>Parameter 6 = \"%s\".</p>", parameters[6]);
   printf("<p>Parameter 7 = \"%s\".</p>", parameters[7]);
   printf("<p>Parameter 8 = \"%s\".</p>", parameters[8]);
   printf("<p>Parameter 9 = \"%s\".</p>", parameters[9]);
   */
   
   //sprintf(query, "SELECT s.id FROM cif_schedules AS s INNER JOIN cif_schedule_locations AS l0 ON l0.cif_schedule_id = s.id AND l0.tiploc_code = '%s' INNER JOIN cif_schedule_locations AS l1 ON l1.cif_schedule_id = s.id AND l1.tiploc_code = '%s' WHERE s.schedule_start_date < %ld AND s.schedule_end_date > %ld AND deleted > %ld", tiploc0, tiploc1, to, from, from);

   sprintf(query, "SELECT s.train_status, s.schedule_start_date, s.schedule_end_date, s.signalling_id, s.CIF_train_uid, s.CIF_stp_indicator, s.update_id, s.id, ");
   strcat(query, "s.runs_mo, s.runs_tu, s.runs_we, s.runs_th, s.runs_fr, s.runs_sa, s.runs_su, s.deleted, s.created, s.deduced_headcode, s.deduced_headcode_status, s.CIF_operating_characteristics FROM cif_schedules AS s");
   if(tiploc0[0])
   {
      sprintf(q1, " INNER JOIN cif_schedule_locations AS l0 ON l0.cif_schedule_id = s.id AND l0.tiploc_code = '%s'", tiploc0);
      strcat(query, q1);
      if(path[0])
      {
         sprintf(q1, " AND l0.path LIKE '%s'", path);
         strcat(query, q1);
      }
      if(line[0])
      {
         sprintf(q1, " AND l0.line LIKE '%s'", line);
         strcat(query, q1);
      }
   }
   if(tiploc1[0])
   {
      sprintf(q1, " INNER JOIN cif_schedule_locations AS l1 ON l1.cif_schedule_id = s.id AND l1.tiploc_code = '%s'", tiploc1);
      strcat(query, q1);
   }

   sprintf(q1, " WHERE s.schedule_start_date < %ld AND s.schedule_end_date > %ld AND deleted > %ld", to, from, from);
         strcat(query, q1);

   if(passenger) strcat(query, " AND (train_status = 'P' OR train_status = 'B' OR train_status = '1' OR train_status = '5') AND (cif_train_category not like 'E%%')");

   if(only_stp_p) strcat(query, " AND (CIF_stp_indicator = 'P')");
   else if(only_stp_o) strcat(query, " AND (CIF_stp_indicator = 'O')");
   else if(not_stp_c)  strcat(query, " AND (CIF_stp_indicator != 'C')");
   
   // printf("<h3>|%s|</h3>", query);

   strcat(query, " ORDER BY signalling_id");
   
   if(db_query(query))
   {
   }
   else
   {
      db_result[0] = db_store_result();
      word shown = 0;
      while((db_row[0] = mysql_fetch_row(db_result[0])) && shown < MAX_SHOWN)
      {
         // Check the not via
         word show = true;
         if(tiploc2[0])
         {
            sprintf(query, "Select * FROM cif_schedule_locations WHERE cif_schedule_id = %s AND tiploc_code = '%s'", db_row[0][7], tiploc2);
            // printf("<p>|%s|</p>", query);
            if(db_query(query))
            {
            }
            else
            {
               db_result[1] = db_store_result();
               if(mysql_fetch_row(db_result[1])) show = false;
               mysql_free_result(db_result[1]);
            }
         }
         if(show)
         {
            shown++;
            if(first)
            {
               printf("<table class=\"result-table\"><tr><th>Type</th><th>ID</th><th>Head</th><th>CIF UID</th><th>STP</th><th>From</th><th>To</th><th>Runs</th><th>Valid dates</th><th>Created</th><th>Deleted</th></tr>\n");
               first = false;
            }

            /*
            printf("<tr class=\"result-table\"><td>%s</td>", describe_schedule(atol(db_row[0][7])));
         
            printf("<td><a href=\"%strain/%s\" class=\"linkbutton\">Details</a></td></tr>\n", LIVERAIL_URL_BASE, db_row[0][7]);
            */

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
               if(strchr(db_row[0][19], 'Q')) printf(" [Runs as required]");
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
         // else printf("Skip %s", db_row[0][0]);
      }
      
      mysql_free_result(db_result[0]);
      if(!first)
      {
         printf("</table>");
         if(db_row[0]) printf("<p>Only first %d matching schedules shown.</p>", MAX_SHOWN);
      }
      else
      {
         printf("<h4>None found.</h4>");
      }
   }      
}
static char * location_name(const char * const tiploc)
{
   // Not re-entrant
   char query[256];
   static char result[256];
   MYSQL_RES * db_result;
   MYSQL_ROW      db_row;

   sprintf(query, "select fn from corpus where tiploc = '%s'", tiploc);
   db_query(query);
   db_result = db_store_result();
   if((db_row = mysql_fetch_row(db_result)) && db_row[0][0]) 
   {
      strncpy(result, db_row[0], 127);
      result[127] = '\0';
   }
   else
   {
      strcpy(result, tiploc);
   }
   mysql_free_result(db_result);

   return result;
}

static char * location_name_stanox(const dword stanox)
{
   // Not re-entrant
   char query[256];
   static char result[256];
   MYSQL_RES * db_result;
   MYSQL_ROW      db_row;

   sprintf(query, "select fn from corpus where stanox = %d", stanox);
   db_query(query);
   db_result = db_store_result();
   if((db_row = mysql_fetch_row(db_result)) && db_row[0][0]) 
   {
      strncpy(result, db_row[0], 127);
      result[127] = '\0';
   }
   else
   {
      sprintf(result, "%d", stanox);
   }
   mysql_free_result(db_result);

   return result;
}

static char * describe_schedule(const dword id)
{
   // Shows WTT times only
   static char result[1024];
   MYSQL_RES * db_result;
   MYSQL_ROW      db_row;

   char query[256];
   char headcode[8];
   
   result[0] = '\0';
   headcode[0] = '\0';

   sprintf(query, "SELECT signalling_id FROM cif_schedules where id = %u", id);
   if(!db_query(query))
   {
      db_result = db_store_result();
      
      if((db_row = mysql_fetch_row(db_result)) && db_row[0][0])
      {
         sprintf(headcode, "%.4s ", db_row[0]);
      }
      
      mysql_free_result(db_result);
   }

   sprintf(query, "SELECT tiploc_code, departure, public_departure FROM cif_schedule_locations where cif_schedule_id = %u AND record_identity = 'LO'", id);
   if(!db_query(query))
   {
      db_result = db_store_result();
      
      if((db_row = mysql_fetch_row(db_result)))
      {
         sprintf(result, "%s%s %s to ", headcode, show_time(db_row[1]), location_name(db_row[0]));
      }
      mysql_free_result(db_result);
   }

   sprintf(query, "SELECT tiploc_code, arrival, public_arrival FROM cif_schedule_locations where cif_schedule_id = %u AND record_identity = 'LT'", id);
   if(!db_query(query))
   {
      db_result = db_store_result();
               
      if((db_row = mysql_fetch_row(db_result)))
      {
         strcat(result, location_name(db_row[0]));
      }
      mysql_free_result(db_result);
   }
   return result;
}

static time_t parse_date(const char * const s)
{
   word i,j,k;
   char z[3][16];
   struct tm broken;
   
   i = j = k = 0;
   while(s[i] && i < 16 && k < 3)
   {
      if(s[i] != '-')
      {
         z[k][j++] = s[i];
      }
      else
      {
         z[k++][j++] = '\0';
         j = 0;
      }
      i++;
   }
   z[k++][j++] = '\0';
   while(k < 3) z[k++][0] = '\0';

   // printf("<h2>|%s| -> |%s|%s|%s|</h2>", s, z[0], z[1], z[2]);
   
   broken = *(localtime(&now));

   if(atoi(z[0]) > 0 && atoi(z[1]) > 0)
   {
      broken.tm_mday = atoi(z[0]);
      broken.tm_mon = atoi(z[1]) - 1;
      if(atoi(z[2])) broken.tm_year = atoi(z[2]) + 100;

      broken.tm_hour = 12;
      broken.tm_min = 0;
      broken.tm_sec = 0;
      broken.tm_isdst = -1;
      return timegm(&broken);
   }
   else
   {
      return 0;
   }
}
