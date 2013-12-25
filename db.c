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

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "misc.h"
#include "db.h"
#include "stomp.h"
#include "jsmn.h"

static MYSQL * mysql_object;
static char server[256], user[256], password[256], database[256];

word db_init(const char * const s, const char * const u, const char * const p, const char * const d)
{
   if(strlen(s) > 250) return 1;
   if(strlen(u) > 250) return 1;
   if(strlen(p) > 250) return 1;
   if(strlen(d) > 250) return 1;
   strcpy(server,   s);
   strcpy(user,     u);
   strcpy(password, p);
   strcpy(database, d);
   mysql_object = 0;
   return 0;
}

word db_query(const char * const query)
{
   char zs[2048];

   if(strlen(query) > 2000)
   {
      _log(MAJOR, "db_query() called with overlong query.");
      return 99;
   }

   sprintf(zs, "db_query(\"%s\")", query);
   _log(PROC, zs);

   if(db_connect()) return 9;
   
   if(mysql_query(mysql_object, query))
   {
      sprintf(zs, "db_query():  mysql_query() Error %u: %s    Query:", mysql_errno(mysql_object), mysql_error(mysql_object));
      _log(CRITICAL, zs);
      char report[128];
      strncpy(report, query, 96);
      report[96] = 0;
      if(strlen(query) > 96)
      {
         strcat(report, "...");
      }
      sprintf(zs,"\"%s\"", report);
      _log(CRITICAL, zs);

      db_disconnect();
      return 3;
   }
   return 0;
}

MYSQL_RES * db_store_result(void)
{
   if(mysql_object) return mysql_store_result(mysql_object);
   return NULL;
}

word db_row_count(void)
{
   // Returns number of rows affected by preceding DELETE or UPDATE.
   // Doesn't work after a SELECT.

   MYSQL_RES * result;
   MYSQL_ROW row;
   if(db_connect()) return 9;

   if (mysql_query(mysql_object, "SELECT row_count()"))
   {
      char zs[1024];
      sprintf(zs, "db_row_count():  mysql_query() Error %u: %s", mysql_errno(mysql_object), mysql_error(mysql_object));
      _log(CRITICAL, zs);
      
      db_disconnect();
      return 3;
   }

   result = mysql_store_result(mysql_object);
   row = mysql_fetch_row(result);

   mysql_free_result(result);

   return atoi(row[0]);
 
}

word db_connect(void)
{
   _log(PROC, "db_connect()");

   if(!mysql_object)
   {
      _log(DEBUG, "   Connecting to database.");
      mysql_object = mysql_init(NULL);
      if(mysql_object == NULL)
      {
         _log(CRITICAL, "db_connect() error 1: mysql_init() returned NULL");
         return 1;
      }
   
      if(mysql_real_connect(mysql_object, server, user, password, database, 0, NULL, 0) == NULL) 
      {
         char zs[1024];
         sprintf(zs, "db_connect() error 2: Connect failed:  Error %u: %s", mysql_errno(mysql_object), mysql_error(mysql_object));
         _log(CRITICAL, zs);

         db_disconnect();
         return 2;
      }

      // Disable auto-reconnect
      my_bool reconnect = 0;
      mysql_options(mysql_object, MYSQL_OPT_RECONNECT, &reconnect);   

      {
         char zs[128];
         sprintf(zs, "Connection to database \"%s\" opened.", database);
         _log(GENERAL, zs);
      }
   }
  
   return 0;
}

void db_disconnect(void)
{
   if(mysql_object) 
   {
      mysql_close(mysql_object);
      _log(GENERAL, "Connection to database closed.");
   }
   mysql_object = NULL;
    
}

void dump_mysql_result_query(const char * const query)
{
   if(db_connect()) return;

   MYSQL_RES * result;
   
   mysql_query(mysql_object, query);
   result = mysql_store_result(mysql_object);
   if(!result) return;

   dump_mysql_result(result);

   mysql_free_result(result);
}

void dump_mysql_result(MYSQL_RES * result)
{
   MYSQL_ROW row;
   MYSQL_FIELD * fields = mysql_fetch_fields(result);
   word num_fields = mysql_num_fields(result);
   word num_rows   = mysql_num_rows(result);
   word field, row_i;
   char zs[1024];

   sprintf(zs, "Result dump: %d rows of %d fields", num_rows, num_fields); _log(MINOR, zs);

   for(row_i = 0; row_i < num_rows; row_i++)
   {
      sprintf(zs, "Row %d", row_i);
      _log(MINOR, zs);
      
      row = mysql_fetch_row(result);
      for(field = 0; field < num_fields; field++)
      {
         sprintf(zs, "   %s = \"%s\"", fields[field].name, row[field]); 
         // See if we can spot a timestamp
         if(row[field][0] == '1' && strlen(row[field]) == 10)
         {
            time_t stamp = atol(row[field]);
            // Accept    Jan 2008      to      May 2033
            //        1,200,000,000         2,000,000,000
            if(stamp > 1200000000L && stamp < 2000000000L)
            {
               strcat(zs, "  ");
               strcat(zs, time_text(stamp, false ));
            }
         }               
         _log(MINOR, zs);
      }
   }
} 

dword db_insert_id(void)
{
   if(mysql_object) return mysql_insert_id(mysql_object);
   return 0;
}

word db_real_escape_string(char * to, const char * const from, const size_t size)
{
   if(db_connect()) return 9;
   
   return mysql_real_escape_string(mysql_object, to, from, size);
}
