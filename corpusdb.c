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
#include <time.h>
#include <mysql.h>
#include <unistd.h>
#include <sys/resource.h>
#include <curl/curl.h>

#include "jsmn.h"
#include "misc.h"
#include "db.h"

#include <libopenrailconfig.h>
#define NAME  "corpusdb"
#define BUILD "UB30"

#define FILEPATH       "/tmp/corpusdb"
#define FILEPATH_DEBUG "/tmp/corpusdb-debug"

char filepath[128], filepath_z[128];

FILE * fp_result;

static size_t total_bytes;
word debug;

static word fetch_corpus(void);
static size_t corpus_write_data(void *buffer, size_t size, size_t nmemb, void *userp);
static word process_corpus(void);
static void process_corpus_object(const char * object_string);
static word update_friendly_names(void);

// Database table

const char const * create_table_corpus =
"CREATE TABLE corpus"
"("
"id INT(10) UNSIGNED NOT NULL AUTO_INCREMENT, "
"fn                     VARCHAR(255) NOT NULL,"
"stanox                 CHAR(16) NOT NULL,"
"uic                    CHAR(16) NOT NULL,"
"3alpha                 CHAR(16) NOT NULL,"
"nlcdesc16              CHAR(20) NOT NULL,"
"tiploc                 CHAR(16) NOT NULL,"
"nlc                    CHAR(16) NOT NULL,"
"nlcdesc                VARCHAR(255) NOT NULL,"
"PRIMARY KEY (id), INDEX(TIPLOC)              "
") ENGINE = InnoDB";

dword count_locations, count_fns;


int main(int argc, char **argv)
{
   char config_buffer[1025];
   if ( argc != 2 ) /* argc should be two to ensure we have a filename */
   { 
     /* print the usage and exit */
     printf("No config file passed.\n\n\tUsage: %s /path/to/config/file.conf\n\n", argv[0] );
   }
   else
   {
   FILE *cfg = fopen(argv[1], "r");
   fread(config_buffer, 1024, 1, cfg);
   fclose(cfg);

   parse_config(config_buffer);
   char zs[1024];

   time_t start_time = time(NULL);
   count_locations = 0;
   count_fns = 0;

   fp_result = NULL;

     /* Determine debug mode
     
     We don't always want to run in production mode, so we
     read the content of the debug config variable and act 
     on it accordingly.
     
     If we do not have a variable set, we assume production 
     mode */
     if ( strcmp(conf.debug,"true") == 0  )
     {
       debug = 1;
     }
     else
     {
       debug = 0;
     }

   _log_init(debug?"/tmp/corpusdb.log":"/var/log/garner/corpusdb.log", debug?1:0);

   _log(GENERAL, "");
   sprintf(zs, "%s %s", NAME, BUILD);
   _log(GENERAL, zs);

   if(debug == 1)
   {
      _log(GENERAL, "Debug mode selected.  Using TEST database.");
      _log(GENERAL, "To use live database, update the debug option in the config file to 'false'.");

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
   db_init(conf.db_server, conf.db_user, conf.db_pass, conf.db_name);

   if(!fetch_corpus() && !process_corpus() && !update_friendly_names())
   {
      db_disconnect();
      char report[8192];

      _log(GENERAL, "End of run:");
      strcpy(report, "Corpus update completed:\n");
      sprintf(zs, "Elapsed time             : %ld minutes", (time(NULL) - start_time + 30) / 60);
      _log(GENERAL, zs);
      strcat(report, zs);
      strcat(report, "\n");
      sprintf(zs, "Locations loaded         : %ld", count_locations);
      _log(GENERAL, zs);
      strcat(report, zs);
      strcat(report, "\n");
      sprintf(zs, "Friendly names loaded    : %ld", count_fns);
      _log(GENERAL, zs);
      strcat(report, zs);
      strcat(report, "\n");
      
      email_alert(NAME, BUILD, "Corpus Update Report", report);
      exit(0);
   }

   email_alert(NAME, BUILD, "Corpus Update Failure Report", "Corpus update failed.  See log file for details.");
   
   db_disconnect();
   exit(1);
   }
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
      _log(MAJOR,"scraper_cif():  Failed to create slist.");
      result = 2;
   }

   if(!result)
   {
      char url[256];
      sprintf(url, "https://datafeeds.networkrail.co.uk/ntrod/SupportingFileAuthenticate?type=CORPUS");
      sprintf(zs, "Target URL \"%s\"", url);
      _log(DEBUG, zs);

      curl_easy_setopt(curlh, CURLOPT_HTTPHEADER, slist);

      // Set timeouts
      curl_easy_setopt(curlh, CURLOPT_NOSIGNAL,              1);
      curl_easy_setopt(curlh, CURLOPT_FTP_RESPONSE_TIMEOUT, 64);
      curl_easy_setopt(curlh, CURLOPT_TIMEOUT,              64);
      curl_easy_setopt(curlh, CURLOPT_CONNECTTIMEOUT,       64);

      // Debugging prints.
      //   curl_easy_setopt(curlh, CURLOPT_VERBOSE,               1);

      // URL and login
      curl_easy_setopt(curlh, CURLOPT_URL,     url);
      sprintf(zs, "%s:%s", conf.nr_user, conf.nr_pass);
      curl_easy_setopt(curlh, CURLOPT_USERPWD, zs);
      curl_easy_setopt(curlh, CURLOPT_FOLLOWLOCATION,        1);

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
         sprintf(zs, "scrapecif(): curl_easy_perform() returned error %d: %s.", res, curl_easy_strerror(res));
         _log(MAJOR, zs);
         result = 3;
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
      _log(GENERAL, "Uncompressing downloaded data");
      sprintf(zs, "/bin/gunzip -f %s", filepath_z);
      if(system(zs))
      {
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
   db_query("DROP TABLE IF EXISTS corpus");
   db_query(create_table_corpus);

   // Bodge:  Bin off the array stuff
   buf_end = fread(buffer, 1, 15, fp);

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

   // jsmn_dump_tokens(object_string, tokens, 0);
   // printf("|%s|\n", object_string);

   char query[2048];
   strcpy(query, "INSERT INTO corpus VALUES(0, ''"); // ID and friendly name
   EXTRACT_APPEND_SQL("stanox");
   EXTRACT_APPEND_SQL("uic");
   EXTRACT_APPEND_SQL("3alpha");
   EXTRACT_APPEND_SQL("nlcdesc16");
   EXTRACT_APPEND_SQL("tiploc");
   EXTRACT_APPEND_SQL("nlc");
   EXTRACT_APPEND_SQL("nlcdesc");

   strcat(query, ")");

   if(!db_query(query)) count_locations++;

   return;
}

static word update_friendly_names(void)
{
   word result = 0;
   MYSQL_RES * result0, * result1;
   MYSQL_ROW row0, row1;
   word done;

   char query[1024], location[256];

   _log(GENERAL,"Updating friendly names where known.");

   db_query("SELECT id, stanox, uic, 3alpha, tiploc, nlc, nlcdesc FROM corpus WHERE fn = ''");

   result0 = db_store_result();

   while((row0 = mysql_fetch_row(result0)))
   {
      done = false;

#if 0
      // stanox ain't unique so can't use them
      if(row0[1][0] ) // stanox
      {
         // stanox
         sprintf(query, "SELECT location FROM location_data WHERE stanox = '%s'", row0[1]);
         db_query(query);

         result1 = db_store_result();
         if((row1 = mysql_fetch_row(result1)))
         {
            db_real_escape_string(location, row1[0], strlen(row1[0]));
            sprintf(query, "UPDATE corpus set fn = '%s' where id = %s", location, row0[0]);
            db_query(query);
            count_fns++;
            done = true;
         }
         mysql_free_result(result1);
      }
#endif

      if(row0[4][0] && !done) // tiploc
      {
         sprintf(query, "SELECT location FROM location_data WHERE tiploc = '%s'", row0[4]);
         db_query(query);

         result1 = db_store_result();
         if((row1 = mysql_fetch_row(result1)))
         {
            db_real_escape_string(location, row1[0], strlen(row1[0]));
            sprintf(query, "UPDATE corpus set fn = '%s' where id = %s", location, row0[0]);
            db_query(query);
            count_fns++;
            done = true;
         }
         mysql_free_result(result1);
      }
      if(!done && row0[3][0]) //3alpha
      {
         sprintf(query, "SELECT location FROM location_data WHERE crs = '%s'", row0[3]);
         db_query(query);

         result1 = db_store_result();
         if((row1 = mysql_fetch_row(result1)))
         {
            db_real_escape_string(location, row1[0], strlen(row1[0]));
            sprintf(query, "UPDATE corpus set fn = '%s' where id = %s", location, row0[0]);
            db_query(query);
             count_fns++;
           done = true;
         }
          mysql_free_result(result1);
      }
      if(!done && row0[5][0])
      {
         sprintf(query, "SELECT location FROM location_data WHERE nlc = '%s'", row0[5]);
         db_query(query);
               
         result1 = db_store_result();
         if((row1 = mysql_fetch_row(result1)))
         {
            db_real_escape_string(location, row1[0], strlen(row1[0]));
            sprintf(query, "UPDATE corpus set fn = '%s' where id = %s", location, row0[0]);
            db_query(query);
            count_fns++;
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

   return result;
}

