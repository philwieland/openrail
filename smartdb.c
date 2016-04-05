/*
    Copyright (C) 2015, 2016 Phil Wieland

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
#include <time.h>
#include <mysql.h>
#include <unistd.h>
#include <sys/resource.h>
#include <curl/curl.h>

#include "jsmn.h"
#include "misc.h"
#include "db.h"
#include "database.h"

#define NAME  "smartdb"
#define BUILD "X328"

#define FILEPATH       "/tmp/smartdb"
#define FILEPATH_DEBUG "/tmp/smartdb-debug"

char filepath[128], filepath_z[128];

FILE * fp_result;

static size_t total_bytes;
word debug;

static word fetch_file(void);
static size_t file_write_data(void *buffer, size_t size, size_t nmemb, void *userp);
static word process_file(void);
static void process_smart_object(const char * const object_string);

dword count_records;

word opt_insecure, used_insecure, opt_verbose;

int main(int argc, char **argv)
{
   int c;
   char config_file_path[256];
   word usage = false;
   opt_insecure = opt_verbose = false;
   used_insecure = false;
   strcpy(config_file_path, "/etc/openrail.conf");
   while ((c = getopt (argc, argv, ":c:ip")) != -1)
   {
      switch (c)
      {
      case 'c':
         strcpy(config_file_path, optarg);
         break;
      case 'i':
         opt_insecure = true;
         break;
      case 'p':
         opt_verbose = true;
         break;
      case ':':
         break;
      case '?':
      default:
         usage = true;
         break;
      }
   }

   char * config_fail;
   if((config_fail = load_config(config_file_path)))
   {
      printf("Failed to read config file \"%s\":  %s\n", config_file_path, config_fail);
      usage = true;
   }

   debug = *conf[conf_debug];

   if(usage)
   {
      printf("\tUsage: %s [-c /path/to/config/file.conf] [-i] [-p]\n\n", argv[0] );
      exit(1);
   }

   char zs[1024];

   time_t start_time = time(NULL);
   count_records = 0;

   fp_result = NULL;

   _log_init(debug?"/tmp/smartdb.log":"/var/log/garner/smartdb.log", debug?1:(opt_verbose?4:0));

   _log(GENERAL, "");
   sprintf(zs, "%s %s", NAME, BUILD);
   _log(GENERAL, zs);

   if(debug == 1)
   {
      _log(GENERAL, "Debug mode selected.");

      strcpy(filepath, FILEPATH_DEBUG);
   }
   else
   {
      strcpy(filepath, FILEPATH);
   }
   strcpy(filepath_z, filepath);
   strcat(filepath_z, ".gz");

   // Enable core dumps
   struct rlimit limit;
   if(!getrlimit(RLIMIT_CORE, &limit))
   {
      limit.rlim_cur = RLIM_INFINITY;
      setrlimit(RLIMIT_CORE, &limit);
   }

   // Initialise database
   if(db_init(conf[conf_db_server], conf[conf_db_user], conf[conf_db_password], conf[conf_db_name])) exit(1);

   if(!fetch_file() && !process_file())
   {
      db_disconnect();
      char report[8192];

      _log(GENERAL, "End of run:");
      strcpy(report, "SMART data update completed:\n");
      if(used_insecure)
      {
         strcat(report, "*** Warning: Insecure download used.\n");
         _log(GENERAL, "*** Warning: Insecure download used.");
      }
      sprintf(zs, "Elapsed time             : %ld minutes", (time(NULL) - start_time + 30) / 60);
      _log(GENERAL, zs);
      strcat(report, zs);
      strcat(report, "\n");
      sprintf(zs, "Records loaded           : %ld", count_records);
      _log(GENERAL, zs);
      strcat(report, zs);
      strcat(report, "\n");
      
      email_alert(NAME, BUILD, "SMART Data Update Report", report);
      exit(0);
   }

   email_alert(NAME, BUILD, "SMART Data Update Failure Report", "SMART data update failed.  See log file for details.");
   
   db_disconnect();
   exit(1);
}

static word fetch_file(void)
{
   char zs[1024];
   word result = 0;

   static CURL * curlh;
   struct curl_slist * slist;

   if(!(curlh = curl_easy_init())) 
   {
      _log(CRITICAL, "fetch_file():  Failed to obtain libcurl easy handle.");
      result = 1;;
   }
   curl_easy_setopt(curlh, CURLOPT_WRITEFUNCTION, file_write_data);

   slist = NULL;
   slist = curl_slist_append(slist, "Cache-Control: no-cache");
   if(!slist)
   {
      _log(MAJOR,"fetch_file():  Failed to create slist.");
      result = 2;
   }

   if(!result)
   {
      char url[256];
      sprintf(url, "https://datafeeds.networkrail.co.uk/ntrod/SupportingFileAuthenticate?type=SMART");
      _log(DEBUG, "Target URL \"%s\"", url);

      curl_easy_setopt(curlh, CURLOPT_HTTPHEADER, slist);

      // Set timeouts
      curl_easy_setopt(curlh, CURLOPT_NOSIGNAL,              1L);
      curl_easy_setopt(curlh, CURLOPT_FTP_RESPONSE_TIMEOUT, 64L);
      curl_easy_setopt(curlh, CURLOPT_TIMEOUT,              64L);
      curl_easy_setopt(curlh, CURLOPT_CONNECTTIMEOUT,       64L);

      // Debugging prints.
      //   curl_easy_setopt(curlh, CURLOPT_VERBOSE,               1L);

      // URL and login
      curl_easy_setopt(curlh, CURLOPT_URL,     url);
      sprintf(zs, "%s:%s", conf[conf_nr_user], conf[conf_nr_password]);
      curl_easy_setopt(curlh, CURLOPT_USERPWD, zs);
      curl_easy_setopt(curlh, CURLOPT_FOLLOWLOCATION,        1L);

   }

   if(!result)
   {
      if(!(fp_result = fopen(filepath_z, "w")))
      {
         sprintf(zs, "Failed to open \"%s\" for writing.", filepath_z);
         _log(MAJOR, zs);
         result = 4;
      }
   }
   
   if(!result)
   {
      CURLcode res;
      if((res = curl_easy_perform(curlh)))
      {
         _log(MAJOR, "fetch_file(): curl_easy_perform() returned error %d: %s.", res, curl_easy_strerror(res));
         if(opt_insecure && (res == 51 || res == 60))
         {
            _log(MAJOR, "Retrying download in insecure mode.");
            // SSH failure, retry without
            curl_easy_setopt(curlh, CURLOPT_SSL_VERIFYPEER, 0L);
            curl_easy_setopt(curlh, CURLOPT_SSL_VERIFYHOST, 0L);
            used_insecure = true;
            if((res = curl_easy_perform(curlh)))
            {
               _log(MAJOR, "fetch_file(): In insecure mode curl_easy_perform() returned error %d: %s.", res, curl_easy_strerror(res));
               result = 3;
            }
         }
         else
         {
            result = 3;
         }
      }
   }

   if(fp_result) fclose(fp_result);
   fp_result = NULL;
   if(curlh) curl_easy_cleanup(curlh);
   curlh = NULL;
   if(slist) curl_slist_free_all(slist);
   slist = NULL;

   if(!result)
   {
      // Uncompress
      char * r;
      _log(GENERAL, "Uncompressing downloaded data.");
      sprintf(zs, "/bin/gunzip -f %s", filepath_z);
      if((r = system_call(zs)))
      {
         _log(MAJOR, "Failed to uncompress file:  %s", r);
         result = 5;
      }
   }
   return result;
}

static size_t file_write_data(void *buffer, size_t size, size_t nmemb, void *userp)
{
   _log(PROC, "file_write_data()");

   size_t bytes = size * nmemb;

   _log(DEBUG, "Bytes received = %zd", bytes);

   fwrite(buffer, size, nmemb, fp_result);

   total_bytes += bytes;

   return bytes;
}

static word process_file(void)
{
   char zs[1024];

#define MAX_BUF 32768
   char buffer[MAX_BUF];
#define MAX_OBJ 65536
   char obj[MAX_OBJ];
   char in_q = 0;
   char b_depth = 0;

   size_t ibuf;
   size_t iobj = 0;
   size_t buf_end;

   FILE * fp;
   
   _log(GENERAL, "Processing SMART data.");

   // Read in json data
   if(!(fp = fopen(filepath, "r")))
   {
      sprintf(zs, "Failed to open \"%s\" for reading.", filepath);
      _log(CRITICAL, zs);
      return 11;
   }      

   // Reset the database
   database_upgrade(smartdb);
   db_query("DELETE FROM smart");

   // Bodge:  Bin off the array stuff
   buf_end = fread(buffer, 1, 14, fp);

   while((buf_end = fread(buffer, 1, MAX_BUF, fp)))
   {
      // printf("Read %zd bytes.\n", buf_end);
      for(ibuf = 0; ibuf < buf_end; ibuf++)
      {
         char c = buffer[ibuf];
         if(c != '\r' && c != '\n') obj[iobj++] = c;
         if(iobj >= MAX_OBJ)
         {
            obj[64] = '\0';
            sprintf(zs, "Object buffer overflow:  \"%s\"...", obj);
            _log(CRITICAL, zs);
            exit(1);
         }
         if(c == '"') in_q = ! in_q;
         if(!in_q && c == '{') b_depth++;
         if(!in_q && c == '}' && b_depth-- && b_depth == 0)
         {
            obj[iobj] = '\0';
            process_smart_object(obj);
            iobj = 0;
         }
      }
   }

   fclose(fp);

   return 0;
   
}

#define EXTRACT_APPEND_SQL(a,b) { jsmn_find_extract_token(object_string, tokens, 0, a, zs, sizeof( zs )); \
if(zs[1] == '\0' && zs[0] == ' ') zs[0] = '\0'; \
db_real_escape_string(zs1, zs, strlen(zs)); \
if(strlen(zs1) > b) \
{ \
 _log(MINOR, "Field \"%s\" value \"%s\" truncated to fit.", a, zs1); \
 zs1[b] = '\0'; \
} \
strcat(query, ", '"); strcat(query, zs1); strcat(query, "'"); }

static void process_smart_object(const char * const object_string)
{
   char zs[1024], zs1[1024];

   jsmn_parser parser;

#define MAX_TOKENS 1024
   jsmntok_t tokens[MAX_TOKENS];

   jsmn_init(&parser);

   int r = jsmn_parse(&parser, object_string, tokens, MAX_TOKENS);
   if(r != JSMN_SUCCESS)
   {
      sprintf(zs, "Parser result %d.  ", r);
      
      switch(r)
      {
      case JSMN_SUCCESS:     strcat(zs, "Success.  "); break;
      case JSMN_ERROR_INVAL: strcat(zs, "Error - Invalid.  "); break;
      case JSMN_ERROR_NOMEM: strcat(zs, "Error - Nomem.  "); break;
      case JSMN_ERROR_PART:  strcat(zs, "Error - Part JSON.  "); break;
      default:               strcat(zs, "Unknown response.  "); break;
      }
      
      _log(MAJOR, zs);
      return;
   }

   // jsmn_dump_tokens(object_string, tokens, 0);
   // printf("|%s|\n", object_string);
   char query[2048];
   strcpy(query, "INSERT INTO smart VALUES(0"); // ID 
   EXTRACT_APPEND_SQL("fromberth", 4);
   EXTRACT_APPEND_SQL("td", 2);
   EXTRACT_APPEND_SQL("stanox", 10);
   EXTRACT_APPEND_SQL("route", 6);
   EXTRACT_APPEND_SQL("steptype", 1);
   EXTRACT_APPEND_SQL("toberth", 4);
   EXTRACT_APPEND_SQL("toline", 1);
   EXTRACT_APPEND_SQL("berthoffset", 10);
   EXTRACT_APPEND_SQL("platform", 6);
   EXTRACT_APPEND_SQL("event", 1);
   EXTRACT_APPEND_SQL("comment", 64);
   EXTRACT_APPEND_SQL("stanme", 9);
   EXTRACT_APPEND_SQL("fromline", 1);

   strcat(query, ")");

   if(!db_query(query)) count_records++;
   return;
}

