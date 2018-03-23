/*
    Copyright (C) 2017 Phil Wieland

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

#define NAME "ops"

#ifndef RELEASE_BUILD
#define BUILD "Z218p"
#else
#define BUILD RELEASE_BUILD
#endif

#define URL_BASE "/rail/ops/"

static word debug;
static time_t now;
#define PARMS 10
#define PARMSIZE 128
static char parameters[PARMS][PARMSIZE];

// Display modes
enum modes        { MENU, DESCRIBERS, BANNERS, MODES} mode;

static MYSQL_RES * result[4];
static MYSQL_ROW row[4];
static char query[4096];

static char host[256];

static void display_control_panel(const char * const b);
static void describers(void);
static void banners(void);

int main()
{
   char zs[1024];

   now = time(NULL);
   qword start_time = time_us();

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

   if(gethostname(host, sizeof(host))) host[0] = '\0';

   mode = MENU;
   if(parms)
   {
      _log(GENERAL, "PARMS = \"%s\"", parms);
   }
   else
   {
      _log(GENERAL, "No PARMS provided!");
   }

   if(!strcasecmp(parameters[0], "desc")) mode = DESCRIBERS;
   else if(!strcasecmp(parameters[0], "ban")) mode = BANNERS;
   _log(DEBUG, "Display mode  = %d", mode);

   printf("Content-Type: text/html; charset=iso-8859-1\n\n");
   printf("<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\" \"http://www.w3.org/TR/html4/loose.dtd\">");
   printf("<html xmlns=\"http://www.w3.org/1999/xhtml\" lang=\"en\" xml:lang=\"en\">");
   printf("<head>");
   printf("<title>%s %s</title>", NAME, BUILD);
   printf("<link rel=\"stylesheet\" type=\"text/css\" href=\"/auxy/liverail.css\">");
   //printf("<script type=\"text/javascript\" src=\"/auxy/liverail.js\"></script>");
   printf("</head>");
   // printf("<body onload=\"startup();\">\n");
   printf("<body>\n");

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
   case MENU:
      display_control_panel("");
      break;

   case DESCRIBERS:
      describers();
      break;

   case BANNERS:
      banners();
      break;

   default:
      break;
   }

   // Footer
   {
      qword elapsed = (time_us() - start_time + 500) / 1000;
      printf("<p><div id=\"bottom-line\">Completed at %s by %s %s at %s.  Elapsed time %s ms.</div>\n", time_text(time(NULL), 1), NAME, BUILD, host, commas_q(elapsed));
      //      printf("<br><div class=\"licence\">Contains Information of Network Rail Infrastructure Limited licensed under the following licence: <a href=\"http://www.networkrail.co.uk/data-feeds/terms-and-conditions\"> Click Here.</a><br>");
      //      printf("These pages are provided strictly for non-commercial, non-safety critical purposes; no responsibility is accepted for any inaccurate or out of date information.");
      //      printf("</div></p>");
        
      printf("</body></html>\n\n");
   }
   exit(0);
}

static void display_control_panel(const char * const b)
{
   printf("<table><tr><td class=\"control-panel-row\">\n");

   printf("Server: %s&nbsp; &nbsp;", host);
   
   printf("Pages: <button class=\"cp-button\" onclick=\"window.location = '%sdesc'\">Describers</button>\n", URL_BASE);
   printf("&nbsp; <button class=\"cp-button\" onclick=\"window.location = '%sban'\">Banners</button>\n", URL_BASE);
   printf("</td>");
   if(b[0])
   {
      printf("<td width=\"4%%\"></td>");
      printf("<td class=\"control-panel-row\">This page: %s</td>", b);
   }

   printf("</tr></table>\n");
}

static void describers()
{
   char menu[4096], menu1[1024], message[4096];

   strcpy(message, "&nbsp;");

   // Set up menu
   sprintf(menu, "<button class=\"cp-button\" onclick=\"window.location = '%sdesc'\">Refresh</button>\n", URL_BASE);
   sprintf(menu1,"<button class=\"cp-button\" onclick=\"window.location = '%sdesc/z/jj'\">Request reload</button>\n", URL_BASE);
   strcat(menu, menu1);
   display_control_panel(menu);

   // Process commands (if any)
   if(!parameters[1][1] && strlen(parameters[2]) == 2)
   {
      switch(parameters[1][0])
      {
      case 'a': // Request control mode 0
         sprintf(query, "UPDATE describers SET control_mode_cmd = 0 WHERE id = '%s'", parameters[2]);
      db_query(query);
      sprintf(message, "A request has been recorded to set the control mode of describer %s to normal.  Press %s to have this command processed.", parameters[2], menu1);
      break;
      case 'b': // Request control mode 2
         sprintf(query, "UPDATE describers SET control_mode_cmd = 2 WHERE id = '%s'", parameters[2]);
      db_query(query);
      sprintf(message, "A request has been recorded to set the control mode of describer %s to blank.  Press %s to have this command processed.", parameters[2], menu1);
      break;
      case 'c': // Request control mode 1
         sprintf(query, "UPDATE describers SET control_mode_cmd = 1 WHERE id = '%s'", parameters[2]);
      db_query(query);
      sprintf(message, "A request has been recorded to set the control mode of describer %s to clear (Erase database).  Press %s to have this command processed.", parameters[2], menu1);
      break;
      case 'd': // Request process mode 0
         sprintf(query, "UPDATE describers SET process_mode = 0 WHERE id = '%s'", parameters[2]);
      db_query(query);
      sprintf(message, "A request has been recorded to set the process mode of describer %s to ignore.  Press %s to have this command processed.", parameters[2], menu1);
      break;
      case 'e': // Request process mode 1
         sprintf(query, "UPDATE describers SET process_mode = 1 WHERE id = '%s'", parameters[2]);
      db_query(query);
      sprintf(message, "A request has been recorded to set the process mode of describer %s to process.  Press %s to have this command processed.", parameters[2], menu1);
      break;
      case 'f': // Request process mode 2
         sprintf(query, "UPDATE describers SET process_mode = 2 WHERE id = '%s'", parameters[2]);
      db_query(query);
      sprintf(message, "A request has been recorded to set the process mode of describer %s to process and log.  Press %s to have this command processed.", parameters[2], menu1);
      break;
      case 'z': // Request reload
         db_query("UPDATE describers SET control_mode_cmd = 1 WHERE id = ''");
         sprintf(message, "A request has been entered for tddb to reload this data.");
         break;
      }
   }

   printf("<table><tr>"); // Outer table

   db_query("SELECT id, last_timestamp, control_mode_cmd, control_mode, no_sig_address, process_mode, description FROM describers ORDER BY id");
   result[0] = db_store_result();
   word trow = 0;
   word trows = (mysql_num_rows(result[0]) + 2) / 3;
   if(trows < 32) trows = 32;

   while(result[0] && (row[0] = mysql_fetch_row(result[0])))
   {

      if(!((trow) % trows))
      {
         if(trow)
         {
            // End of column
            printf("</table></td>");
         }
         // Start of column
         printf("<td valign=\"top\">");
         printf("<table class=\"summ-table\">");
         printf("<tr class=\"summ-table-head\"><th></th><th></th><th></th><th colspan=\"2\">Control Mode</th><th></th><th>Set</th><th>Set</th></tr>");
         printf("<tr class=\"summ-table-head\"><th>ID</th><th></th><th>Last seen</th><th>Actual</th><th>Request</th><th>Process Mode</th><th>Control Mode</th><th>Process Mode</tr>");
      }
      trow++;

      if(row[0][0][0])
      {
         printf("<tr class=\"summ-table-idle\"><td>%s</td><td>%s</td>", row[0][0], row[0][6]);
         time_t last_seen = atol(row[0][1]);
         if(row[0][5][0] == '0') printf("<td>");
         else if(now - last_seen > 60) printf("<td class=\"summ-table-major\">");
         else printf("<td class=\"summ-table-good\">");
         printf("%s</td>", last_seen?time_text(last_seen, true):"Never");
         switch(row[0][3][0])
         {
         case '0': printf("<td>Normal</td>"); break;
         case '1': printf("<td class=\"summ-table-major\">Clear</td>"); break;
         case '2': printf("<td class=\"summ-table-major\">Blank</td>"); break;
         default:  printf("<td class=\"summ-table-crit\">%s</td>", row[0][3]); break;
         }
         switch(row[0][2][0])
         {
         case '0': printf("<td></td>"); break;
         case '1': printf("<td class=\"summ-table-major\">Clear</td>"); break;
         case '2': printf("<td class=\"summ-table-major\">Blank</td>"); break;
         default:  printf("<td class=\"summ-table-crit\">%s</td>", row[0][2]); break;
         }
         switch(row[0][5][0])
         {
         case '0': printf("<td>Ignore</td>"); break;
         case '1': printf("<td class=\"summ-table-good\">Process</td>"); break;
         case '2': printf("<td class=\"summ-table-major\">Process and log</td>"); break;
         default:  printf("<td class=\"summ-table-crit\">%s</td>", row[0][5]); break;
         }
         printf("<td><a class=\"linkbutton-summ\" href=\"%sdesc/a/%s\">Normal</a>&nbsp;", URL_BASE, row[0][0]);
         printf(    "<a class=\"linkbutton-summ\" href=\"%sdesc/c/%s\">Clear</a>&nbsp"  , URL_BASE, row[0][0]);
         printf(    "<a class=\"linkbutton-summ\" href=\"%sdesc/b/%s\">Blank</a></td>"  , URL_BASE, row[0][0]);
         printf("<td><a class=\"linkbutton-summ\" href=\"%sdesc/d/%s\">Ignore</a>&nbsp;", URL_BASE, row[0][0]);
         printf(    "<a class=\"linkbutton-summ\" href=\"%sdesc/e/%s\">Process</a>&nbsp"  , URL_BASE, row[0][0]);
         printf(    "<a class=\"linkbutton-summ\" href=\"%sdesc/f/%s\">Log</a></td>"  , URL_BASE, row[0][0]);
      }
      else
      {
         // Control record
         printf("<tr class=\"summ-table-idle\"><td colspan=\"2\">%s</td><td colspan=\"4\"></td>", row[0][6]);
         if(row[0][2][0] == '0') printf("<td colspan=\"2\"></td></tr>");
         else                    printf("<td colspan=\"2\" class=\"summ-table-crit\">Reload requested.</td>");
      }
      printf("</tr>\n");
   }
   mysql_free_result(result[0]);

   printf("</table></td></tr></table>");      
   printf("<p>%s</p>\n", message);
}


static void banners()
{
   char menu[4096], /* menu1[1024], */ message[4096];
   char escaped0[1024], escaped1[1024];
   time_t expires;
   word minutes;

   strcpy(message, "&nbsp;");

   // Set up menu
   sprintf(menu, "<button class=\"cp-button\" onclick=\"window.location = '%sban'\">Refresh</button>\n", URL_BASE);
   display_control_panel(menu);

   // Process commands (if any)
   if(!parameters[1][1])
   {
      switch(parameters[1][0])
      {
      case 'b': // New standard message (diagram)
         db_real_escape_string(escaped0, parameters[2], strlen(parameters[2]));
         db_real_escape_string(escaped1, parameters[3], strlen(parameters[3]));
         sprintf(query, "INSERT INTO banners VALUES('diagram_s','%s', '%s', 0, 0)", escaped0, escaped1);
         if(db_query(query))
         {
            strcpy(message, "Database error.");
         }
         else
         {
            strcpy(message, "Standard diagram message created.");
         }
         break;

      case 'c': // Delete a standard message
         sprintf(query, "DELETE FROM banners WHERE id = %s", parameters[2]);
         if(db_query(query))
         {
            strcpy(message, "Database error.");
         }
         else
         {
            strcpy(message, "Standard message deleted.");
         }
         break;

      case 'd': // Use a standard message for diagram
         if(parameters[2][0])
         {
            sprintf(query, "SELECT banner, banner1, expires FROM banners WHERE id = %s", parameters[2]);
            db_query(query);
            result[0] = db_store_result();
            if(result[0] && (row[0] = mysql_fetch_row(result[0])))
            {
               db_real_escape_string(escaped0, row[0][0], strlen(row[0][0]));
               db_real_escape_string(escaped1, row[0][1], strlen(row[0][1]));

               time_t expiry = atol(row[0][2]);
               if(expiry < now) expiry = 0;
               sprintf(query, "UPDATE banners SET banner = '%s', banner1 = '%s', expires = %ld WHERE type = 'diagram'", escaped0, escaped1, expiry);
               if(!db_query(query))
                  strcpy(message, "Diagram banner updated.");
               else
                  strcpy(message, "Database error.");
            }
            if(result[0]) mysql_free_result(result[0]);
         }
         else
         {
            if(!db_query("UPDATE banners SET banner = '', banner1 = '' WHERE type = 'diagram'"))
                  strcpy(message, "Diagram banner updated.");
               else
                  strcpy(message, "Database error.");
         }
         break;

      case 'e': // Expiry for diagram message
         minutes = atoi(parameters[2]);
         expires = 0;
         if(minutes) expires = now + (minutes * 60);
         expires -= expires%60;
         sprintf(query, "UPDATE banners SET expires = %ld where type = 'diagram'", expires);
         if(db_query(query))
         {
            strcpy(message, "Database error.");
         }
         else
         {
            strcpy(message, "Diagram expiry set.");
         }
         break;

      case 'v': // New standard message (webpage)
         db_real_escape_string(escaped0, parameters[2], strlen(parameters[2]));
         sprintf(query, "INSERT INTO banners VALUES('webpage_s','%s', '', 0, 0)", escaped0);
         if(db_query(query))
         {
            strcpy(message, "Database error.");
         }
         else
         {
            strcpy(message, "Standard diagram message created.");
         }
         break;

      case 'w': // Use a standard message for webpage
         if(parameters[2][0])
         {
            sprintf(query, "SELECT banner, banner1, expires FROM banners WHERE id = %s", parameters[2]);
            db_query(query);
            result[0] = db_store_result();
            if(result[0] && (row[0] = mysql_fetch_row(result[0])))
            {
               db_real_escape_string(escaped0, row[0][0], strlen(row[0][0]));
               time_t expiry = atol(row[0][2]);
               if(expiry < now) expiry = 0;
               sprintf(query, "UPDATE banners SET banner = '%s', banner1 = '', expires = %ld WHERE type = 'webpage'", escaped0, expiry);
               if(!db_query(query))
                  strcpy(message, "Web Page banner updated.");
               else
                  strcpy(message, "Database error.");
            }
            if(result[0]) mysql_free_result(result[0]);
         }
         else
         {
            if(!db_query("UPDATE banners SET banner = '', banner1 = '' WHERE type = 'webpage'"))
               strcpy(message, "Web page banner updated.");
            else
               strcpy(message, "Database error.");
         }
         break;

      case 'x': // Expiry for webpage
         minutes = atoi(parameters[2]);
         expires = 0;
         if(minutes) expires = now + (minutes * 60);
         expires -= expires%60;
         sprintf(query, "UPDATE banners SET expires = %ld where type = 'webpage'", expires);
         if(db_query(query))
         {
            strcpy(message, "Database error.");
         }
         else
         {
            strcpy(message, "Web page expiry set.");
         }
         break;

      case 'y': // Copy diagram banner to web banner.
         if(!db_query("SELECT banner, banner1 FROM banners WHERE type = 'diagram'"))
         {
            result[0] = db_store_result();
            if(result[0] && (row[0] = mysql_fetch_row(result[0])))
            {
               db_real_escape_string(escaped0, row[0][0], strlen(row[0][0]));
               db_real_escape_string(escaped1, row[0][1], strlen(row[0][1]));
               sprintf(query, "INSERT INTO banners VALUES('webpage_s','%s %s', '', 0, 0)", escaped0, escaped1);
               if(!db_query(query))
                  strcpy(message, "Web Page banner updated.");
               else
                  strcpy(message, "Database error.");
            }
            if(result[0]) mysql_free_result(result[0]);
         }
         break; 
      }
   }

   printf("<table class=\"train-table\"><tr><th>Current banner for diagrams</th><th width=\"50%%\">Current banner for web pages</th></tr>\n");

   printf("<tr><td>");
   db_query("SELECT banner, banner1, expires FROM banners WHERE type = 'diagram'");
   result[0] = db_store_result();
   if(result[0] && (row[0] = mysql_fetch_row(result[0])))
   {
      printf("<pre>");
      printf("|%-64s|<br>|%-64s|", row[0][0], row[0][1]);
      printf("</pre>");
      expires = atol(row[0][2]);
      if(expires)
      {
         if(expires > now) printf("Expires %s.", time_text(expires, true));
         else printf("<span class=\"summ-table-crit\">Expired.</span>");
      }
      else printf("No expiry set.");
   }
   else
   {
      printf("Database error on banner type diagram.<br>");
   }
   if(result[0]) mysql_free_result(result[0]);
   printf("</td><td>\n");

   db_query("SELECT banner, expires FROM banners WHERE type = 'webpage'");
   result[0] = db_store_result();
   if(result[0] && (row[0] = mysql_fetch_row(result[0])))
   {
      printf("<p><span style=\"background-color:yellow;\">%s</span>&nbsp;</p>", row[0][0]);
      expires = atol(row[0][1]);
      if(expires)
      {
         if(expires > now) printf("Expires %s.", time_text(expires, true));
         else printf("<span class=\"summ-table-crit\">Expired.</span>");
      }
      else printf("No expiry set.");
   }
   else
   {
      printf("<p>Database error on banner type webpage.</p>");
   }
   if(result[0]) mysql_free_result(result[0]);

   printf("</td></tr><tr><th>Alter banner for diagrams</th><th width=\"50%%\">Alter banner for web pages</th></tr>\n");

   printf("<td valign=\"top\">Standard messages:");
   printf("<table class=\"train-table\">");

   db_query("SELECT banner, banner1, id FROM banners WHERE type = 'diagram_s' ORDER BY banner");
   result[0] = db_store_result();
   while(result[0] && (row[0] = mysql_fetch_row(result[0])))
   {
      printf("<tr><td><button class=\"cp-button\" onclick=\"window.location = '%sban/d/%s'\">Apply</button>&nbsp;", URL_BASE, row[0][2]);
      printf("<button class=\"cp-button\" onclick=\"window.location = '%sban/c/%s'\">Delete</button></td><td><pre>%s\n%s</pre></td></tr>\n", URL_BASE, row[0][2], row[0][0], row[0][1]);
   }

   printf("<tr><td><button class=\"cp-button\" onclick=\"window.location = '%sban/d'\">Blank</button></td><td>Blank message.</td></tr>", URL_BASE);
   printf("</table>");
   if(result[0]) mysql_free_result(result[0]);

   printf("<p>Create a new message:<br>");
   printf("<input type=\"text\" class=\"plaintext\" id=\"2\" size=\"64\" maxlength=\"64\"><br>");
   printf("<input type=\"text\" class=\"plaintext\" id=\"3\" size=\"64\" maxlength=\"64\"><br>");
   printf("<button class=\"cp-button\" onclick=\"window.location = '%sban/b/' + document.getElementById('2').value + '/' + document.getElementById('3').value\">Create</button>", URL_BASE);
   printf("</p>");

   printf("<p>Message expiry:<br><button class=\"cp-button\" onclick=\"window.location = '%sban/e/0'\">No expiry</button>", URL_BASE);
   printf(" <input type=\"text\" id=\"1\" size=\"4\" maxlength=\"4\"> minutes ");
   printf("<button class=\"cp-button\" onclick=\"window.location = '%sban/e/' + document.getElementById('1').value\">Set</button>", URL_BASE);
   printf("</p>");
   printf("</td>");

   printf("<td valign=\"top\">");
   printf("<p>Standard messages:");
   printf("<table class=\"train-table\">");

   db_query("SELECT banner, banner1, id FROM banners WHERE type = 'webpage_s' ORDER BY banner");
   result[0] = db_store_result();
   while(result[0] && (row[0] = mysql_fetch_row(result[0])))
   {
      printf("<tr><td width=\"120px\"><button class=\"cp-button\" onclick=\"window.location = '%sban/w/%s'\">Apply</button>&nbsp;", URL_BASE, row[0][2]);
      printf("<button class=\"cp-button\" onclick=\"window.location = '%sban/c/%s'\">Delete</button></td><td><p>%s</p></td></tr>\n", URL_BASE, row[0][2], row[0][0]);
   }

   printf("<tr><td><button class=\"cp-button\" onclick=\"window.location = '%sban/w'\">Blank</button></td><td>Blank message.</td></tr>", URL_BASE);
   printf("</table>");
   if(result[0]) mysql_free_result(result[0]);

   printf("</p>");
   printf("<p>Create a new message:");
   printf("<textarea id=\"b\" rows=\"4\" cols=\"120\"></textarea>\n");
   printf("<br><button class=\"cp-button\" onclick=\"window.location = '%sban/v/' + document.getElementById('b').value\">Create</button>", URL_BASE);
   printf("</p>");
   printf("<p>Copy current diagram message:<br>");
   printf("<button class=\"cp-button\" onclick=\"window.location = '%sban/y'\">Copy</button>", URL_BASE);
   printf("</p>");

   printf("<p>Message expiry:<br><button class=\"cp-button\" onclick=\"window.location = '%sban/x/0'\">No expiry</button>", URL_BASE);
   printf(" <input type=\"text\" id=\"a\" size=\"4\" maxlength=\"4\"> minutes ");
   printf("<button class=\"cp-button\" onclick=\"window.location = '%sban/x/' + document.getElementById('a').value\">Set</button>", URL_BASE);
   printf("</p>");
   printf("</td>");

   printf("</tr></table>\n");

   printf("<p>%s</p>\n", message);
}
