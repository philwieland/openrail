/*
    Copyright (C) 2013, 2015, 2016, 2017, 2018, 2022 Phil Wieland

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
#include <sys/stat.h>
#include <dirent.h>

#include "jsmn.h"
#include "misc.h"
#include "db.h"
#include "database.h"
#include "build.h"

#define NAME  "corpusdb"

#ifndef RELEASE_BUILD
#define BUILD "3721p"
#else
#define BUILD RELEASE_BUILD
#endif

#define TEMP_DIRECTORY "/var/tmp"
#define FILE_NAME      "corpusdb-downloaded.json"
#define FILEPATH_DEBUG "/tmp/corpusdb-debug.json"

char filepath[128], filepath_z[128];

FILE * fp_result;

static size_t total_bytes;
word debug;

// Stats
enum stats_categories {Locations, FriendlyNames,
                       MAXStats };
static qword stats[MAXStats];
static const char * const stats_category[MAXStats] = 
   {
      "Locations loaded", "Friendly names loaded",  
   };

static word fetch_corpus(void);
static size_t corpus_write_data(void *buffer, size_t size, size_t nmemb, void *userp);
static word process_corpus(void);
static void process_corpus_object(const char * object_string);
static word update_friendly_names(void);

word opt_insecure, used_insecure, opt_verbose;
word id_number;

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

   fp_result = NULL;

   _log_init(debug?"/tmp/corpusdb.log":"/var/log/garner/corpusdb.log", debug?1:(opt_verbose?4:0));

   _log(GENERAL, "");
   sprintf(zs, "%s %s", NAME, BUILD);
   _log(GENERAL, zs);

   if(debug)
   {
      _log(GENERAL, "Debug mode selected.");

      strcpy(filepath, FILEPATH_DEBUG);
   }
   else
   {
      strcpy(filepath, TEMP_DIRECTORY);
      strcat(filepath, "/");
      strcat(filepath, FILE_NAME);
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

   // Zero the stats
   {
      word i;
      for(i = 0; i < MAXStats; i++) { stats[i] = 0; }
   }

   // Initialise database
   if(db_init(conf[conf_db_server], conf[conf_db_user], conf[conf_db_password], conf[conf_db_name], DB_MODE_NORMAL)) exit(1);

   if(!fetch_corpus() && !process_corpus() && !update_friendly_names())
   {
      db_disconnect();
      char report[8192];

      _log(GENERAL, "End of run:");
      strcpy(report, "Corpus update completed:\n");
      if(used_insecure)
      {
         strcat(report, "*** Warning: Insecure download used.\n");
         _log(GENERAL, "*** Warning: Insecure download used.");
      }
      sprintf(zs, "%25s: %ld minutes", "Elapsed time", (time(NULL) - start_time + 30) / 60);
      _log(GENERAL, zs);
      strcat(report, zs);
      strcat(report, "\n");
      word i;
      for(i = 0; i < MAXStats; i++)
      {
         sprintf(zs, "%25s: %s", stats_category[i], commas_q(stats[i]));
         _log(GENERAL, zs);
         strcat(report, zs);
         strcat(report, "\n");
      }

      email_alert(NAME, BUILD, "Corpus Update Report", report);
   }
   else
   {
      email_alert(NAME, BUILD, "Corpus Update Failure Report", "Corpus update failed.  See log file for details.");
   }
   db_disconnect();

   exit(0);
}

static word fetch_corpus(void)
{
   char zs[1024];
   word result = 0;

   static CURL * curlh;
   struct curl_slist * slist;

   if(!(curlh = curl_easy_init())) 
   {
      _log(CRITICAL, "fetch_corpus():  Failed to obtain libcurl easy handle.");
      result = 1;;
   }
   curl_easy_setopt(curlh, CURLOPT_WRITEFUNCTION, corpus_write_data);

   slist = NULL;
   slist = curl_slist_append(slist, "Cache-Control: no-cache");
   if(!slist)
   {
      _log(MAJOR,"fetch_corpus():  Failed to create slist.");
      result = 2;
   }

   if(!result)
   {
      char url[256];
      sprintf(url, "https://%s/ntrod/SupportingFileAuthenticate?type=CORPUS", conf[conf_nr_server]);
      sprintf(zs, "Target URL \"%s\"", url);
      _log(DEBUG, zs);

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
         _log(MAJOR, "fetch_corpus(): curl_easy_perform() returned error %d: %s.", res, curl_easy_strerror(res));
         if(opt_insecure && (res == 51 || res == 60))
         {
            _log(MAJOR, "Retrying download in insecure mode.");
            // SSH failure, retry without
            curl_easy_setopt(curlh, CURLOPT_SSL_VERIFYPEER, 0L);
            curl_easy_setopt(curlh, CURLOPT_SSL_VERIFYHOST, 0L);
            used_insecure = true;
            if((res = curl_easy_perform(curlh)))
            {
               _log(MAJOR, "fetch_corpus(): In insecure mode curl_easy_perform() returned error %d: %s.", res, curl_easy_strerror(res));
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
         if((fp_result = fopen(filepath_z, "r")))
         {
            char error_message[2048];
            size_t length;
            if((length = fread(error_message, 1, 2047, fp_result)) && error_message[0] == '<')
            {
               error_message[length] = '\0';
               _log(MAJOR, "Received message:\n%s", error_message);   
            }
            fclose(fp_result);
         }
         result = 5;
      }
   }
   return result;
}

static size_t corpus_write_data(void *buffer, size_t size, size_t nmemb, void *userp)
{
   char zs[128];
   _log(PROC, "curl_write_data()");

   size_t bytes = size * nmemb;

   sprintf(zs, "Bytes received = %zd", bytes);
   
   _log(DEBUG, zs);

   fwrite(buffer, size, nmemb, fp_result);

   total_bytes += bytes;

   return bytes;
}

static word process_corpus(void)
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
   

   // Read in json data
   if(!(fp = fopen(filepath, "r")))
   {
      sprintf(zs, "Failed to open \"%s\" for reading.", filepath);
      _log(CRITICAL, zs);
      return 11;
   }      

   // Reset the database
   word e;
   if((e = database_upgrade(corpusdb)))
   {
      _log(CRITICAL, "Error %d in database_upgrade().  Aborting.", e);
      exit(1);
   }

   db_start_transaction();
   db_query("DELETE FROM corpus");
   id_number = 1;

   _log(GENERAL, "Processing CORPUS data.");

   // Bodge:  Bin off the array stuff
   buf_end = fread(buffer, 1, 15, fp);

   while((buf_end = fread(buffer, 1, MAX_BUF, fp)))
   {
      for(ibuf = 0; ibuf < buf_end; ibuf++)
      {
         char c = buffer[ibuf];
         if(c != '\r' && c != '\n') obj[iobj++] = c;
         if(iobj >= MAX_OBJ)
         {
            obj[64] = '\0';
            sprintf(zs, "Object buffer overflow:  \"%.100s\"...", obj);
            _log(CRITICAL, zs);
            exit(1);
         }
         if(c == '"') in_q = ! in_q;
         if(!in_q && c == '{') b_depth++;
         if(!in_q && c == '}' && b_depth-- && b_depth == 0)
         {
            obj[iobj] = '\0';
            process_corpus_object(obj);
            iobj = 0;
         }
      }
   }

   fclose(fp);
   return 0;
   
}

#define EXTRACT_APPEND_SQL(a) { jsmn_find_extract_token(object_string, tokens, 0, a, zs, sizeof( zs )); \
if(zs[1] == '\0' && zs[0] == ' ') zs[0] = '\0'; \
db_real_escape_string(zs1, zs, strlen(zs)); \
strcat(query, ", '"); strcat(query, zs1); strcat(query, "'"); }
#define EXTRACT_APPEND_SQL_INT(a) { jsmn_find_extract_token(object_string, tokens, 0, a, zs, sizeof( zs )); \
if((zs[0] == '\0') || (zs[1] == '\0' && zs[0] == ' ')) { zs[0] = '0'; zs[1] = '\0'; } \
db_real_escape_string(zs1, zs, strlen(zs)); \
strcat(query, ", "); strcat(query, zs1); }

static void process_corpus_object(const char * object_string)
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

   char query[2048];
   char nlcdesc16[1024];

   sprintf(query, "INSERT INTO corpus VALUES(%d, ''", id_number++); // ID and friendly name
   EXTRACT_APPEND_SQL_INT("stanox");
   EXTRACT_APPEND_SQL("uic");
   EXTRACT_APPEND_SQL("3alpha");
   
   jsmn_find_extract_token(object_string, tokens, 0, "nlcdesc16", nlcdesc16, sizeof(nlcdesc16));
   db_real_escape_string(zs1, nlcdesc16, strlen(nlcdesc16));
   strcat(query, ", '"); 
   if(zs1[0] == ' ') strcat(query, zs1 + 1); 
   else strcat(query, zs1);
      strcat(query, "'");
   EXTRACT_APPEND_SQL("tiploc");
   EXTRACT_APPEND_SQL("nlc");
   EXTRACT_APPEND_SQL("nlcdesc");

   strcat(query, ")");

   if(!db_query(query)) stats[Locations]++;

   return;
}

static word update_friendly_names(void)
{
   word result = 0;
   MYSQL_RES * result0, * result1;
   MYSQL_ROW row0, row1;
   word done, fn_available;

   char query[1024], location[256];

   fn_available = false;
   if(!db_query("SHOW TABLES LIKE 'location_data'"))
   {
      result0 = db_store_result();
      word num_rows = mysql_num_rows(result0);
      mysql_free_result(result0);
      if(num_rows) fn_available = true;
   }

   if(fn_available)
      _log(GENERAL,"Updating friendly names where known.");
   else
      _log(MAJOR, "No friendly names available in database.");

   db_query("SELECT id, stanox, uic, 3alpha, tiploc, nlc, nlcdesc FROM corpus WHERE fn = ''");

   result0 = db_store_result();

   while((row0 = mysql_fetch_row(result0)))
   {
      done = false;

      if(row0[4][0] && !done && fn_available) // tiploc
      {
         sprintf(query, "SELECT location FROM location_data WHERE tiploc = '%s'", row0[4]);
         db_query(query);

         result1 = db_store_result();
         if((row1 = mysql_fetch_row(result1)))
         {
            db_real_escape_string(location, row1[0], strlen(row1[0]));
            sprintf(query, "UPDATE corpus set fn = '%s' where id = %s", location, row0[0]);
            db_query(query);
            stats[FriendlyNames]++;
            done = true;
         }
         mysql_free_result(result1);
      }
      if(!done && row0[3][0] && fn_available) //3alpha
      {
         sprintf(query, "SELECT location FROM location_data WHERE crs = '%s'", row0[3]);
         db_query(query);

         result1 = db_store_result();
         if((row1 = mysql_fetch_row(result1)))
         {
            db_real_escape_string(location, row1[0], strlen(row1[0]));
            sprintf(query, "UPDATE corpus set fn = '%s' where id = %s", location, row0[0]);
            db_query(query);
            stats[FriendlyNames]++;
           done = true;
         }
          mysql_free_result(result1);
      }
      if(!done && row0[5][0] && fn_available)
      {
         sprintf(query, "SELECT location FROM location_data WHERE nlc = '%s'", row0[5]);
         db_query(query);
               
         result1 = db_store_result();
         if((row1 = mysql_fetch_row(result1)))
         {
            db_real_escape_string(location, row1[0], strlen(row1[0]));
            sprintf(query, "UPDATE corpus set fn = '%s' where id = %s", location, row0[0]);
            db_query(query);
            stats[FriendlyNames]++;
            done = true;
         }
         mysql_free_result(result1);
      }
              
      if(!done)
      {
         // Couldn't find any info
         sprintf(query, "update corpus set fn = nlcdesc where id = %s", row0[0]);
         // Don't count these ones. count_fns++;
         db_query(query);
      }
   }

   mysql_free_result(result0);

   _log(GENERAL, "Commit database updates...");
   db_commit_transaction();

   return result;
}

