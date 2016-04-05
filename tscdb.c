/*
    Copyright (C) 2013, 2014, 2015, 2016 Phil Wieland

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

#include "misc.h"
#include "jsmn.h"
#include "db.h"
#include "database.h"

#define NAME  "tscdb"
#define BUILD "X328"

static void process_object(const char * object);
static void process_timetable(const char * string, const jsmntok_t * tokens);
static void process_schedule(const char * string, const jsmntok_t * tokens);
static void process_create_schedule(const char * string, const jsmntok_t * tokens);
static void jsmn_dump_tokens(const char * string, const jsmntok_t * tokens, const word object_index);
static word fetch_file(void);
static size_t cif_write_data(void *buffer, size_t size, size_t nmemb, void *userp);

static word debug, fetch_all, run, tiploc_ignored, test_mode, verbose, opt_insecure, used_insecure;
static char * opt_filename;
static char * opt_url;
static char last_update_id[16];

static time_t start_time;

// Stats
enum stats_categories {Fetches, Sequence, 
                       DBError,
                       JSONRecords, ScheduleCreate,
                       TSCUpdate,
                       MAXStats };
static qword stats[MAXStats];
static const char * stats_category[MAXStats] = 
   {
      "File fetch", "Sequence number",
      "Database error",
      "JSON record", "Schedule create",
      "TSC update",
   };
// Buffer for reading file
#define MAX_BUF 8192
char buffer[MAX_BUF];

// Buffer for a JSON object in string form
#define MAX_OBJ 65536
char obj[MAX_OBJ];

// Buffer for JSON object after parsing.
#define MAX_TOKENS 8192
jsmntok_t tokens[MAX_TOKENS];

// 
FILE * fp_result;   
static size_t total_bytes;

#define MATCHES 2
static regex_t match[MATCHES];
static char* match_strings[MATCHES] =
   {
      "\"timestamp\":([[:digit:]]{1,12})",  // 0
      "\"sequence\":([[:digit:]]{1,12})",   // 1
   };

int main(int argc, char **argv)
{
   char config_file_path[256];
   opt_filename = NULL;
   opt_url = NULL;
   fetch_all = false;
   test_mode = false;
   verbose = false;
   opt_insecure = false;
   used_insecure = false;

   strcpy(config_file_path, "/etc/openrail.conf");
   word usage = false;
   int c;
   while ((c = getopt (argc, argv, ":c:u:f:tpih")) != -1)
      switch (c)
      {
      case 'c':
         strcpy(config_file_path, optarg);
         break;
      case 'u':
         if(!opt_filename) opt_url = optarg;
         break;
      case 'f':
         if(!opt_url) opt_filename = optarg;
         break;
      case 'a':
         fetch_all = true;
         break;
      case 't':
         test_mode = true;
         break;
      case 'p':
         verbose = true;
         break;
      case 'i':
         opt_insecure = true;
         break;
      case 'h':
         usage = true;
         break;
      case ':':
         break;
      case '?':
      default:
         usage = true;
      break;
      }

   char * config_fail;
   if((config_fail = load_config(config_file_path)))
   {
      printf("Failed to read config file \"%s\":  %s\n", config_file_path, config_fail);
      usage = true;
   }

   if(usage) 
   {
      printf("%s %s  Usage: %s [-c /path/to/config/file.conf] [-u <url> | -f <path> | -a] [-t | -r] [-p][-i]\n", NAME, BUILD, argv[0]);
      printf(
             "-c <file>  Path to config file.\n"
             "Data source:\n"
             "default    Fetch latest update.\n"
             "-u <url>   Fetch from specified URL.\n"
             "-f <file>  Use specified file.  (Must already be decompressed.)\n"
             "Actions:\n"
             "default    Apply data to database.\n"
             "-t         Report datestamp on download or file, do not apply to database.\n"
             "Options:\n"
             "-i         Insecure.  Circumvent certificate checks if necessary.\n"
             "-p         Print activity as well as logging.\n"
             );
      exit(1);
   }

   char zs[1024];

   start_time = time(NULL);

   debug = *conf[conf_debug];
  
   _log_init(debug?"/tmp/tscdb.log":"/var/log/garner/tscdb.log", (debug?1:(verbose?4:0)));
   
   _log(GENERAL, "");
   _log(GENERAL, "%s %s", NAME, BUILD);
   
   // Enable core dumps
   struct rlimit limit;
   if(!getrlimit(RLIMIT_CORE, &limit))
   {
      limit.rlim_cur = RLIM_INFINITY;
      setrlimit(RLIMIT_CORE, &limit);
   }
   
   int i;
   for(i = 0; i < MATCHES; i++)
   {
      if(regcomp(&match[i], match_strings[i], REG_ICASE + REG_EXTENDED))
      {
         sprintf(zs, "Failed to compile regex match %d", i);
         _log(MAJOR, zs);
      }
   }
   
   // Initialise database
   if(db_init(conf[conf_db_server], conf[conf_db_user], conf[conf_db_password], conf[conf_db_name])) exit(1);

   {
      word e;
      if((e=database_upgrade(cifdb)))
      {
         _log(CRITICAL, "Error %d in upgrade_database().  Aborting.", e);
         exit(1);
      }
   }

   run = 1;
   tiploc_ignored = false;

   // Zero the stats
   {
      word i;
      for(i = 0; i < MAXStats; i++) { stats[i] = 0; }
   }

   if(fetch_file())
   {
      if(opt_url || opt_filename)
      {
         _log(GENERAL, "Failed to find data.");
         exit(1);
      }
      {
         char report[256];
         _log(GENERAL, "Failed to fetch file.");
         
         sprintf(report, "Failed to collect timetable update after %lld attempts.", stats[Fetches]);
         email_alert(NAME, BUILD, "Timetable Update Failure Report", report);
      }
      exit(1);
   }

   char in_q = 0;
   char b_depth = 0;

   size_t ibuf = 0;
   size_t iobj = 0;
   size_t buf_end;

   // Determine applicable update
   {
      MYSQL_RES * result0;
      MYSQL_ROW row0;
      last_update_id[0] = '\0';
      if(!db_query("SELECT MAX(id) FROM updates_processed"))
      {
         result0 = db_store_result();
         if((row0 = mysql_fetch_row(result0)))
         {
            strcpy(last_update_id, row0[0]);
         }
         mysql_free_result(result0);
      }
   }

   if(last_update_id[0] == '\0')
   {
      _log(CRITICAL, "Failed to determine last update id from database.");
      exit(1);
   }


   if(test_mode)
   {
   }
   else
   {
      char c, pc;
      pc = 0;
      // Run through the file splitting off each JSON object and passing it on for processing.

      // DB may have dropped out due to long delay
      (void) db_connect();
      if(db_start_transaction()) _log(CRITICAL, "Failed to initiate database transaction.");
      while((buf_end = fread(buffer, 1, MAX_BUF, fp_result)) && run && !db_errored)
      {
         for(ibuf = 0; ibuf < buf_end && run && !db_errored; ibuf++)
         {
            c = buffer[ibuf];
            if(c != '\r' && c != '\n') 
            {
               obj[iobj++] = c;
               if(iobj >= MAX_OBJ)
               {
                  _log(CRITICAL, "Object buffer overflow!");
                  exit(1);
               }
               if(c == '"' && pc != '\\') in_q = ! in_q;
               if(!in_q && c == '{') b_depth++;
               if(!in_q && c == '}' && b_depth-- && !b_depth)
               {
                  obj[iobj] = '\0';
                  process_object(obj);
                  iobj = 0;
               }
            }
            pc = c;
         }
      }
      fclose(fp_result);
      if(db_errored)
      {
         _log(CRITICAL, "Update rolled back due to database error.");
         (void) db_rollback_transaction();
      }
      else
      {
         _log(GENERAL, "Committing database updates...");
         if(db_commit_transaction())
         {
            _log(CRITICAL, "Database commit failed.");
         }
         else
         {
            _log(GENERAL, "Committed.");
         }
      }
   }

#define REPORT_SIZE 16384
   char report[REPORT_SIZE];
   report[0] = '\0';

   _log(GENERAL, "");
   _log(GENERAL, "End of run:");

   if(used_insecure)
   {
      strcat(report, "*** Warning: Insecure download used.\n");
      _log(GENERAL, "*** Warning: Insecure download used.");
   }

   sprintf(zs, "             Elapsed time: %ld minutes", (time(NULL) - start_time + 30) / 60);
   _log(GENERAL, zs);
   strcat(report, zs); strcat(report, "\n");
   if(test_mode)
   {
      sprintf(zs, "Test mode.  No database changes made.");
      _log(GENERAL, zs);
      strcat(report, zs); strcat(report, "\n");
      exit(0);
   }

   for(i=0; i<MAXStats; i++)
   {
      sprintf(zs, "%25s: %s", stats_category[i], commas_q(stats[i]));
      if(i == DBError && stats[i]) strcat(zs, " ****************");
      _log(GENERAL, zs);
      strcat(report, zs);
      strcat(report, "\n");
   }

   db_disconnect();

   email_alert(NAME, BUILD, "Timetable Update Report", report);

   exit(0);
}

static void process_object(const char * object_string)
{
   // Passed a string containing a JSON object.
   // Parse it and process it.
   char zs[256];

   _log(PROC, "process_object()");

   jsmn_parser parser;
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

   if(tokens[1].type == JSMN_NAME)
   {
      char message_name[128];

      stats[JSONRecords]++; // NB This INCLUDES ones we don't use, like TiplocV1
      jsmn_extract_token(object_string, tokens, 1, message_name, sizeof(message_name));
      if(!strcmp(message_name, "JsonTimetableV1"))        process_timetable(object_string, tokens);
      else if(!strcmp(message_name, "JsonAssociationV1")) ;
      else if(!strcmp(message_name, "JsonScheduleV1"))    process_schedule(object_string, tokens);
      else if(!strcmp(message_name, "TiplocV1"))          ;
      else if(!strcmp(message_name, "EOF"))               ;
      else
      {
         _log(MINOR, "Unrecognised message name \"%s\".", message_name);
         jsmn_dump_tokens(object_string, tokens, 0);
         stats[JSONRecords]--;
      }
   }
   else
   {
      _log(MAJOR, "Unrecognised message.");
      jsmn_dump_tokens(object_string, tokens, 0);
   }
   return;
}

static void process_timetable(const char * string, const jsmntok_t * tokens)
{
   char timestamp[32], zs[256];
   strcpy(zs, "Timetable information: ");
   jsmn_find_extract_token(string, tokens, 0, "timestamp", timestamp, sizeof(timestamp));
   if(timestamp[0])
   {
      time_t stamp = atoi(timestamp);
      strcat(zs, "Timestamp ");
      strcat(zs, time_text(stamp, true));

      _log(GENERAL, "%s.  Sequence number %s.", zs, commas_q(stats[Sequence]));
   }
   else
   {
      strcat(zs, "Timestamp not found.  Processing aborted.");
      _log(CRITICAL, zs);
      run = 0;
   }
}

#define EXTRACT(a,b) jsmn_find_extract_token(string, tokens, 0, a, b, sizeof( b ))

static void process_schedule(const char * string, const jsmntok_t * tokens)
{
   char zs[128], zs1[1024];

   jsmn_find_extract_token(string, tokens, 0, "transaction_type", zs, sizeof(zs));
   if(zs[0])
   {
      // printf("   Transaction type: \"%s\"", zs);
      if(!strcasecmp(zs, "Delete"))      ;
      else if(!strcasecmp(zs, "Create")) process_create_schedule(string, tokens);
      else 
      {
         sprintf(zs1, "process_schedule():  Unrecognised transaction type \"%s\".", zs);
         _log(MAJOR, zs1);
      }
   }
   else
   {
      _log(MAJOR, "process_schedule():  Failed to determine transaction type.");
   }
}

#define EXTRACT_APPEND_SQL(a) { jsmn_find_extract_token(string, tokens, 0, a, zs, sizeof( zs )); sprintf(zs1, ", \"%s\"", zs); strcat(query, zs1); }


static void process_create_schedule(const char * string, const jsmntok_t * tokens)
{
   char zs[128];
   char query[2048];
   char uid[16], stp_indicator[2], tsc[16];
   MYSQL_RES * result0;
   MYSQL_ROW row0;

   stats[ScheduleCreate]++;

   EXTRACT("CIF_stp_indicator", stp_indicator);
   if(stp_indicator[0] == 'C') return;
   EXTRACT("CIF_train_uid", uid);
   EXTRACT("schedule_start_date", zs);
   time_t start_date = parse_datestamp(zs);
   EXTRACT("schedule_end_date", zs);
   time_t end_date   = parse_datestamp(zs);
   EXTRACT("CIF_train_service_code", tsc);

   sprintf(query, "SELECT id FROM cif_schedules WHERE CIF_stp_indicator = '%s' AND CIF_train_uid = '%s' AND schedule_start_date = %ld AND schedule_end_date = %ld AND CIF_train_service_code = '' AND update_id = %s", stp_indicator, uid, start_date, end_date, last_update_id);

   if(!db_query(query))
   {
      result0 = db_store_result();
      if((row0 = mysql_fetch_row(result0)))
      {
         sprintf(query, "UPDATE cif_schedules SET CIF_train_service_code = '%s' WHERE id = %s", tsc, row0[0]);
         db_query(query);
         stats[TSCUpdate]++;
         _log(GENERAL, "Updated schedule %s (%s %s) with TSC \"%s\".", row0[0], uid, stp_indicator, tsc);
      }
      mysql_free_result(result0);
   }        
}


static void jsmn_dump_tokens(const char * string, const jsmntok_t * tokens, const word object_index)
{
   char zs[256], zs1[256];

   word i;
   for(i = object_index + 1; tokens[i].start >= 0 && tokens[i].start < tokens[object_index].end; i++)
   {
      sprintf(zs, "Token %2d: Type=", i);
      switch(tokens[i].type)
      {
      case JSMN_PRIMITIVE: strcat(zs, "Primitive"); break;
      case JSMN_OBJECT:    strcat(zs, "Object   "); break;
      case JSMN_ARRAY:     strcat(zs, "Array    "); break;
      case JSMN_STRING:    strcat(zs, "String   "); break;
      case JSMN_NAME:      strcat(zs, "Name     "); break;
      }
      sprintf(zs1, " Start=%d End=%d", tokens[i].start, tokens[i].end);
      strcat(zs, zs1);
      if(tokens[i].type == JSMN_OBJECT || tokens[i].type == JSMN_ARRAY)
      {
         sprintf(zs1, " Size=%d", tokens[i].size);
         strcat(zs, zs1);
      }
      if(tokens[i].type == JSMN_STRING || tokens[i].type == JSMN_NAME || tokens[i].type == JSMN_PRIMITIVE)
      {
         jsmn_extract_token(string, tokens, i, zs1, sizeof(zs1));
         strcat(zs, " \"");
         strcat(zs, zs1);
         strcat(zs, "\"");
      }
      _log(GENERAL, "   %s", zs);
   }

   return;
}

static word fetch_file(void)
{
   // Returns 0=Success with relevant file open on fp_result
   // Or !0 = failure and file is closed.
   // DANGER: In failure case, fp_result is INVALID and may not be null.
   char zs[256], filepathz[256], filepath[256], url[256];
   time_t now, when;
   struct tm * broken;
   static char * weekdays[] = { "sun", "mon", "tue", "wed", "thu", "fri", "sat" };

   stats[Fetches]++;

   now = time(NULL);
   if(!opt_filename)
   {
      static CURL * curlh;
      struct curl_slist * slist;

      if(!(curlh = curl_easy_init())) 
      {
         _log(CRITICAL, "fetch_file():  Failed to obtain libcurl easy handle.");
         return 1;
      }
      curl_easy_setopt(curlh, CURLOPT_WRITEFUNCTION, cif_write_data);

      slist = NULL;
      slist = curl_slist_append(slist, "Cache-Control: no-cache");
      if(!slist)
      {
         _log(MAJOR,"fetch_file():  Failed to create slist.");
         return 1;
      }

      // Build URL
      when = now - 24*60*60;
      broken = localtime(&when); // Note broken contains "yesterday"
      if(opt_url)
      {
         strcpy(url, opt_url);
      }
      else
      {
         if(fetch_all)
         {
            sprintf(url, "https://datafeeds.networkrail.co.uk/ntrod/CifFileAuthenticate?type=CIF_ALL_FULL_DAILY&day=toc-full");
         }
         else
         {
            sprintf(url, "https://datafeeds.networkrail.co.uk/ntrod/CifFileAuthenticate?type=CIF_ALL_UPDATE_DAILY&day=toc-update-%s", weekdays[broken->tm_wday]);
         }
      }
      sprintf(zs, "Fetching \"%s\".", url);
      _log(GENERAL, zs);

      if(opt_url || debug)
      {
         sprintf(filepathz, "/tmp/tscdb-cif-fetch-%ld.gz", now);
         sprintf(filepath,  "/tmp/tscdb-cif-fetch-%ld",    now);
      }
      else if(fetch_all)
      {
         sprintf(filepathz, "/tmp/tscdb-cif-all-%02d-%02d-%02d-%s.gz", broken->tm_year%100, broken->tm_mon + 1, broken->tm_mday, weekdays[broken->tm_wday]);
         sprintf(filepath,  "/tmp/tscdb-cif-all-%02d-%02d-%02d-%s",    broken->tm_year%100, broken->tm_mon + 1, broken->tm_mday, weekdays[broken->tm_wday]);
      }
      else
      {
         sprintf(filepathz, "/tmp/tscdb-cif-%02d-%02d-%02d-%s.gz", broken->tm_year%100, broken->tm_mon + 1, broken->tm_mday, weekdays[broken->tm_wday]);
         sprintf(filepath,  "/tmp/tscdb-cif-%02d-%02d-%02d-%s",    broken->tm_year%100, broken->tm_mon + 1, broken->tm_mday, weekdays[broken->tm_wday]);
      }

      if(!(fp_result = fopen(filepathz, "w")))
      {
         sprintf(zs, "Failed to open \"%s\" for writing.", filepathz);
         _log(MAJOR, zs);
         return 1;
      }

      curl_easy_setopt(curlh, CURLOPT_HTTPHEADER, slist);

      // Set timeouts
      curl_easy_setopt(curlh, CURLOPT_NOSIGNAL,              1L);
      curl_easy_setopt(curlh, CURLOPT_FTP_RESPONSE_TIMEOUT, 128L);
      curl_easy_setopt(curlh, CURLOPT_TIMEOUT,              128L);
      curl_easy_setopt(curlh, CURLOPT_CONNECTTIMEOUT,       128L);

      // Debugging prints.
      // curl_easy_setopt(curlh, CURLOPT_VERBOSE,               1L);

      // URL and login
      curl_easy_setopt(curlh, CURLOPT_URL,     url);
      sprintf(zs, "%s:%s", conf[conf_nr_user], conf[conf_nr_password]);
      curl_easy_setopt(curlh, CURLOPT_USERPWD, zs);
      curl_easy_setopt(curlh, CURLOPT_FOLLOWLOCATION,        1L);  // On receiving a 3xx response, follow the redirect.
      total_bytes = 0;

      CURLcode result;
      if((result = curl_easy_perform(curlh)))
      {
         _log(MAJOR, "fetch_file(): curl_easy_perform() returned error %d: %s.", result, curl_easy_strerror(result));
         if(opt_insecure && (result == 51 || result == 60))
         {
            _log(MAJOR, "Retrying download in insecure mode.");
            // SSH failure, retry without
            curl_easy_setopt(curlh, CURLOPT_SSL_VERIFYPEER, 0L);
            curl_easy_setopt(curlh, CURLOPT_SSL_VERIFYHOST, 0L);
            used_insecure = true;
            if((result = curl_easy_perform(curlh)))
            {
               _log(MAJOR, "fetch_file(): In insecure mode curl_easy_perform() returned error %d: %s.", result, curl_easy_strerror(result));
               if(fp_result) fclose(fp_result);
               fp_result = NULL;
               return 1;
            }
         }
         else
         {
            if(fp_result) fclose(fp_result);
            fp_result = NULL;
            return 1;
         }
      }
      char * actual_url;
      if(!curl_easy_getinfo(curlh, CURLINFO_EFFECTIVE_URL, &actual_url) && actual_url)
      {
         _log(GENERAL, "Download was redirected to \"%s\".", actual_url);
      }
   
      if(fp_result) fclose(fp_result);
      fp_result = NULL;
      if(curlh) curl_easy_cleanup(curlh);
      curlh = NULL;
      if(slist) curl_slist_free_all(slist);
      slist = NULL;

      sprintf(zs, "Received %s bytes of compressed CIF updates.",  commas(total_bytes));
      _log(GENERAL, zs);

      if(total_bytes == 0) return 1;
   
      _log(GENERAL, "Decompressing data...");
      sprintf(zs, "/bin/gunzip -f %s", filepathz);
      char * rc;
      if((rc = system_call(zs)))
      {
         _log(MAJOR, "Failed to uncompress file:  %s", rc);
         if((fp_result = fopen(filepathz, "r")))
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
         return 1;
      }
      _log(GENERAL, "Decompressed.");
   }
   else
   {
      strcpy(filepath, opt_filename);
   }

   if(!(fp_result = fopen(filepath, "r")))
   {
      _log(MAJOR, "Failed to open \"%s\" for reading.", filepath);
      return 1;
   }

   // Check if it's really an update
   {
      sprintf(zs, "grep -q \\\"Delete\\\" %s", filepath);
      word not_update = system(zs);
      if(not_update && (total_bytes < 32000000L)) not_update = false;
      _log(test_mode?GENERAL:DEBUG, "File is an update assessment: %s.", not_update?"File is not an update":"File is an update");
      if(fetch_all && !not_update)
      {
         _log(MAJOR, "Requested full timetable looks like an update.");
         fclose(fp_result);
         return 1;
      }
      if(!fetch_all && not_update)
      {
         _log(MAJOR, "Requested update looks like a full timetable.");
         fclose(fp_result);
         return 1;
      }
   }

   // Read enough of the file to find the datestamp
   char front[256];
   regmatch_t matches[3];

   if(!fread(front, 1, sizeof(front), fp_result))
   {
      fclose(fp_result);
      return 1;
   }
   else
   {
      front[255] = '\0';
      if(regexec(&match[0], front, 2, matches, 0))      
      {
         // Failed
         _log(MAJOR, "Failed to derive CIF file timestamp.");
         fclose(fp_result);
         return 1;
      }
      else
      {
         char zstamp[256];
         extract_match(front, matches, 1, zstamp, sizeof(zstamp));
         time_t stamp = atoi(zstamp);
         _log(test_mode?GENERAL:DEBUG, "Recovered timestamp: %s", time_text(stamp, 0));
         _log(test_mode?GENERAL:DEBUG, "Stored in file \"%s\"", filepath);
         if(regexec(&match[1], front, 2, matches, 0))
         {
            _log(MINOR, "Failed to derive CIF file sequence number.");
         }
         else
         {
            extract_match(front, matches, 1, zstamp, sizeof(zstamp));
            stats[Sequence] = atol(zstamp);
            _log(test_mode?GENERAL:DEBUG, "Sequence number: %s", commas_q(stats[Sequence]));
         }

         if(!test_mode && !opt_url && !opt_filename && (now < stamp || now - stamp > 36*60*60))
         {
            _log(MAJOR, "Timestamp %s is incorrect.  Received sequence number %d.", time_text(stamp, true), stats[Sequence]);
            fclose(fp_result);
            stats[Sequence] = 0;
            return 1;
         }
      }
   }
   fseeko(fp_result, 0, SEEK_SET);
   
   return 0;
}

static size_t cif_write_data(void *buffer, size_t size, size_t nmemb, void *userp)
{
   char zs[128];
   _log(PROC, "cif_write_data()");

   size_t bytes = size * nmemb;

   sprintf(zs, "Bytes received = %zd", bytes);
   
   _log(DEBUG, zs);

   fwrite(buffer, size, nmemb, fp_result);

   total_bytes += bytes;

   return bytes;
}

