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
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>

#include "misc.h"
#include "db.h"
#include "database.h"

#define NAME  "cifdb"
#define BUILD "X328"

static word debug, opt_fetch_all, run, tiploc_ignored, CR_ignored, opt_test, opt_print, opt_insecure, used_insecure;
static char * opt_filename;
static char * opt_url;
static dword update_id;
static time_t start_time, last_reported_time;
#define INVALID_SORT_TIME 9999

#define NOT_DELETED 0xffffffffL

// Stats
enum stats_categories {Fetches,  
                       ScheduleCreate, ScheduleDeleteHit, ScheduleDeleteMiss,
                       ScheduleLocCreate, ScheduleCR,
                       // AssocCreate, AssocDeleteHit, AssocDeleteMiss,
                       HeadcodeDeduced,
                       CIFRecords,
                       MAXStats };
static qword stats[MAXStats];
static const char * const stats_category[MAXStats] = 
   {
      "File fetch",  
      "Schedule create", "Schedule delete hit", "Schedule delete miss",
      "Schedule location create", "Schedule CR create",
      // "Association Create", "Association Delete Hit", "Association Delete Miss",
      "Deduced schedule headcode",
      "CIF record", 
   };
#define HOME_REPORT_SIZE 512
static unsigned long home_report_id[HOME_REPORT_SIZE];
static word home_report_index;

static FILE * fp;

static size_t fetch_total_bytes;
static time_t fetch_extract_time;
static char fetch_filepath[512];

#define REPORT_SIZE 16384
static char report[REPORT_SIZE];

static word fetch_file(const word day);
static size_t cif_write_data(void *buffer, size_t size, size_t nmemb, void *userp);
static word process_file(void);
static void final_report(void);
static word process_HD(const char * const c);
static word process_schedule(const char * const c);
static word process_schedule_delete(const char * const c);
static word process_tiploc(const char * const c);
static word get_sort_time(const char const * buffer);
static void reset_database(void);
static char * tiploc_name(const char * const tiploc);
static void extract_field(const char * const c, size_t s, size_t l, char * d);
static char * extract_field_s(const char * const c, size_t s, size_t l);
static time_t parse_CIF_datestamp(const char * const s);

int main(int argc, char **argv)
{
   char config_file_path[256];
   opt_filename = NULL;
   opt_url = NULL;
   opt_fetch_all = false;
   opt_test = false;
   opt_print = false;
   opt_insecure = false;
   used_insecure = false;

   strcpy(config_file_path, "/etc/openrail.conf");
   word usage = false;
   int c;
   while ((c = getopt (argc, argv, ":c:u:f:atpih")) != -1)
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
         opt_fetch_all = true;
         break;
      case 't':
         opt_test = true;
         break;
      case 'p':
         opt_print = true;
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

   debug = *conf[conf_debug];

   if(usage) 
   {
      printf("%s %s  Usage: %s [-c /path/to/config/file.conf] [-u <url> | -f <path> | -a] [-t | -r] [-p][-i]\n", NAME, BUILD, argv[0]);
      printf(
             "-c <file>  Path to config file.\n"
             "Data source:\n"
             "default    Fetch latest update.\n"
             "-u <url>   Fetch from specified URL.\n"
             "-f <file>  Use specified file.  (Must already be decompressed.)\n"
             "-a         Fetch latest full timetable.\n"
             "Actions:\n"
             "default    Apply data to database.\n"
             "-t         Report datestamp on download or file, do not apply to database.\n"
             "Options:\n"
             "-i         Insecure.  Circumvent certificate checks if necessary.\n"
             "-p         Print activity as well as logging.\n"
             );
      exit(1);
   }

   start_time = time(NULL);

   _log_init(debug?"/tmp/cifdb.log":"/var/log/garner/cifdb.log", (debug?1:(opt_print?4:0)));
   
   _log(GENERAL, "");
   _log(GENERAL, "%s %s", NAME, BUILD);
   
   if(debug)
   {
      _log(DEBUG, "Configuration dump:");
      word i;
      for(i = 0; i < MAX_CONF; i++)
      {
         _log(DEBUG, "\"%s\"", conf[i]);
      }
   } 

   // Enable core dumps
   struct rlimit limit;
   if(!getrlimit(RLIMIT_CORE, &limit))
   {
      limit.rlim_cur = RLIM_INFINITY;
      setrlimit(RLIMIT_CORE, &limit);
   }
 
   // Initialise database
   if(db_init(conf[conf_db_server], conf[conf_db_user], conf[conf_db_password], conf[conf_db_name])) exit(1);

   if(opt_fetch_all && !opt_test) reset_database();

   if(!opt_test)
   {
      word e;
      if((e = database_upgrade(cifdb)))
      {
         _log(CRITICAL, "Error %d in upgrade_database().  Aborting.", e);
         exit(1);
      }
   }

   run = true;
   tiploc_ignored = false;
   CR_ignored = false;

   // Zero the stats
   {
      word i;
      for(i = 0; i < MAXStats; i++) { stats[i] = 0; }
   }
   home_report_index = 0;

   if(!opt_fetch_all && !opt_filename && !opt_url)
   {
      // Standard file fetch
      while(run)
      {
         if(stats[Fetches])
         {
            _log(GENERAL, "Waiting before retry...");
            time_t delay = 60 * 32;
            while(run && delay--) sleep(1);
         }

         if(fetch_file(0xffff))
         {
            _log(MAJOR, "Failed to fetch file.");
            if(opt_test || opt_filename || opt_url) run = false;
         }
         
         else if(run)
         {
            // Is it correct date?
            struct tm * broken = gmtime(&fetch_extract_time);
            time_t when = start_time - 24L*60L*60L;
            struct tm * wanted = gmtime(&when);
            
            if(broken->tm_year == wanted->tm_year &&
               broken->tm_mon  == wanted->tm_mon  &&
               broken->tm_mday == wanted->tm_mday)
            {
               // Got the file we want
               if(!process_file())
               {
                  // All done!
                  run = false;
               }
               else
               {
               }
            }
            else
            {
               _log(MAJOR, "Downloaded file has incorrect timestamp %s.", time_text(fetch_extract_time, true));
               if(stats[Fetches] > 16)
               {
                  _log(MAJOR, "Attempt %ld to fetch file failed.  Abandoning run.", stats[Fetches]);
                  // Email a failure report
                  sprintf(report, "Failed to collect timetable update after %lld attempts.\n\nAbandoning run.", stats[Fetches]);
                  email_alert(NAME, BUILD, "Timetable Update Failure Report", report);
                  run = false;
               }
               else
               {
                  _log(MAJOR, "Attempt %ld to fetch file failed.  Will retry after delay...", stats[Fetches]);
                  if(stats[Fetches] == 4)
                  {
                     sprintf(report, "Failed to collect timetable update after %lld attempts.\n\nContinuing to retry.", stats[Fetches]);
                     email_alert(NAME, BUILD, "Timetable Update Failure Report", report);
                  }
               }
            }
         }
      }
   }
   else if(opt_url || opt_filename)
   {
      if(fetch_file(0xffff)) exit(1);
      if(process_file()) exit(1);
   }
   else
   {
      // Special processing for opt_fetch_all.
      if(fetch_file(0xffff))
      {
         exit(1);
      }
      struct tm * broken = localtime(&fetch_extract_time);
      if(broken->tm_wday != 5)
      {
         _log(ABEND, "Unexpected extract day %d (%s) on full download.", broken->tm_wday, time_text(fetch_extract_time, true));
         exit(1);
      }
      if(!process_file())
      {
         // Successfully loaded full file, apply any required updates
         broken = localtime(&start_time);
         word last_day = broken->tm_wday;
         word day = 0; // Day after fetch-day of full update
         word fail = false;
         // Fetch updates from day after full up to today. ASSUMES full extract fetched on Saturday.
         while(last_day != 6 && day <= last_day && !fail)
         {
            fail = fetch_file(day++);
            if(!fail) fail = process_file();
         }
      }
   }

   // All done.  Send Report
   final_report();
   db_disconnect();
   _log(GENERAL, "Run complete.");
   exit(0);
}           

static void final_report(void)
{
   char zs[1024];
   word i;

   report[0] = '\0';

   _log(GENERAL, "");
   _log(GENERAL, "End of run:");

   if(used_insecure)
   {
      strcat(report, "*** Warning: Insecure download used.\n");
      _log(GENERAL, "*** Warning: Insecure download used.");
   }

   sprintf(zs, "        Data extract time: %s", time_text(fetch_extract_time, true));
   _log(GENERAL, zs);
   strcat(report, zs); strcat(report, "\n");
   if(opt_filename)
   {
      sprintf(zs, "           Data from file: %s", opt_filename);
      _log(GENERAL, zs);
      strcat(report, zs); strcat(report, "\n");
   }
   if(opt_url)
   {
      sprintf(zs, "     Data downloaded from: %s", opt_url);
      _log(GENERAL, zs);
      strcat(report, zs); strcat(report, "\n");
   }

   sprintf(zs, "             Elapsed time: %ld minutes", (time(NULL) - start_time + 30) / 60);
   _log(GENERAL, zs);
   strcat(report, zs); strcat(report, "\n");
   if(opt_test)
   {
      sprintf(zs, "Test mode.  No database changes made.");
      _log(GENERAL, zs);
      strcat(report, zs); strcat(report, "\n");
      return;
   }

   for(i=0; i<MAXStats; i++)
   {
      sprintf(zs, "%25s: %s", stats_category[i], commas_q(stats[i]));
      // if(i == DBError && stats[i]) strcat(zs, " ****************");
      _log(GENERAL, zs);
      strcat(report, zs);
      strcat(report, "\n");
   }

   if(conf[conf_huyton_alerts][0])
   {
      if(!opt_fetch_all)
      {
         // Report details of home trains
         sprintf(zs, "Schedule changes passing Huyton: %s", commas(home_report_index));
         _log(GENERAL, zs);
         strcat(report, "\n"); strcat(report, zs); strcat(report, "\n");
   
         word i;
         char train[256], q[1024];
         MYSQL_RES * result0, * result1;
         MYSQL_ROW row0, row1;

         for(i=0; i < home_report_index && i < HOME_REPORT_SIZE; i++)
         {
            row0 = NULL;
            result0 = NULL;
            sprintf(zs, "%9ld ", home_report_id[i]);
            
            //                       0             1                  2                    3                 4
            sprintf(q, "select CIF_train_UID, signalling_id, schedule_start_date, schedule_end_date, CIF_stp_indicator, deleted FROM cif_schedules WHERE id = %ld", home_report_id[i]);
            if(!db_query(q))
            {
               result0 = db_store_result();
               if((row0 = mysql_fetch_row(result0)))
               {
                  if(atol(row0[5]) > start_time)
                  {
                     strcat(zs, "created ");
                  }
                  else
                  {
                     strcat(zs, "deleted ");
                  }
               
                  sprintf(train, "(%s %s) %4s ", row0[0], row0[4], row0[1]);
                  strcat(zs, train);
               }
            }
            sprintf(q, "SELECT tiploc_code, departure FROM cif_schedule_locations WHERE record_identity = 'LO' AND cif_schedule_id = %ld", home_report_id[i]);
            if(!db_query(q))
            {
               result1 = db_store_result();
               if((row1 = mysql_fetch_row(result1)))
               {
                  sprintf(train, " %s %s to ", show_time_text(row1[1]), tiploc_name(row1[0]));
                  strcat(zs, train);
               }
               mysql_free_result(result1);
            }
            sprintf(q, "SELECT tiploc_code FROM cif_schedule_locations WHERE record_identity = 'LT' AND cif_schedule_id = %ld", home_report_id[i]);
            if(!db_query(q))
            {
               result1 = db_store_result();
               if((row1 = mysql_fetch_row(result1)))
               {
                  strcat (zs, tiploc_name(row1[0]));
               }
               mysql_free_result(result1);
            }
            
            if(row0)
            {
               dword from = atol(row0[2]);
               dword to   = atol(row0[3]);
               if(from == to)
               {
                  strcat(zs, "  Runs on ");
                  strcat(zs, day_date_text(from, true));
               }
               else
               {
                  strcat(zs, "  Runs from ");
                  strcat(zs, day_date_text(from, true));
                  strcat(zs, " to ");
                  strcat(zs, day_date_text(to,   true));
               }
            }
            if(result0) mysql_free_result(result0);
            
            _log(GENERAL, zs);
            if(strlen(report) < REPORT_SIZE - 256)
            {
               strcat(report, zs); strcat(report, "\n");
            }
         }
         if(home_report_index >= HOME_REPORT_SIZE)
         {
            _log(GENERAL, "[Further schedules omitted.]");
            strcat(report, "[Further schedules omitted.]"); strcat(report, "\n");
         }
         else if(strlen(report) >= REPORT_SIZE - 256)
         {
            strcat(report, "[Further schedules omitted.  See log file for details.]"); strcat(report, "\n");
         }
      }
   }

   email_alert(NAME, BUILD, "Timetable Update Report", report);

   exit(0);
}

static word get_sort_time(const char const * buffer)
{
   word result;
   char zs[8];
   if(!buffer[0] || buffer[0] == ' ') return INVALID_SORT_TIME;

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

static void reset_database(void)
{
   _log(GENERAL, "Resetting schedule database.");

   db_query("DROP TABLE IF EXISTS updates_processed");
   db_query("DROP TABLE IF EXISTS cif_associations");
   db_query("DROP TABLE IF EXISTS cif_schedules");
   db_query("DROP TABLE IF EXISTS cif_schedule_locations");

   _log(GENERAL, "Schedule database cleared.");
   db_disconnect();
}

static word fetch_file(const word day)
{
   // Returns 0=Success or error code.
   // In success case ONLY, returns following info in globals:
   // fetch_total_bytes
   // fetch_extract_time
   // fetch_filepath 

   // In choosing where to fetch from, day parameter takes priority.  If day > 7, obey opt_ settings
   // day is day of fetch, not day of extract!  So day = 2 (Tuesday) will fetch from mon url.

   char zs[256], filepathz[256], filepath[256], url[256];
   time_t now, when;
   struct tm * broken;
   static char * weekdays[] = { "sun", "mon", "tue", "wed", "thu", "fri", "sat", "sun" };
   word full_file = false; // Indicates that THIS FETCH is a full timetable.
   // N.B. an opt_fetch_all run will consist of full_file plus zero or more update fetches.  The latter will 
   // have the day parameter set

   stats[Fetches]++;
   fp = NULL;

   if(!opt_filename)
   {
      // Build URL
      now = time(NULL);
      when = now - 24*60*60;
      broken = localtime(&when); // Note broken contains "yesterday"
      if(day < 8)
      {
         sprintf(url, "https://datafeeds.networkrail.co.uk/ntrod/CifFileAuthenticate?type=CIF_ALL_UPDATE_DAILY&day=toc-update-%s.CIF.gz", weekdays[(day + 6) % 7]);
      }
      else if(opt_url)
      {
         strcpy(url, opt_url);
      }
      else if(opt_fetch_all)
      {
         sprintf(url, "https://datafeeds.networkrail.co.uk/ntrod/CifFileAuthenticate?type=CIF_ALL_FULL_DAILY&day=toc-full.CIF.gz");
         full_file = true;
      }
      else
      {
         sprintf(url, "https://datafeeds.networkrail.co.uk/ntrod/CifFileAuthenticate?type=CIF_ALL_UPDATE_DAILY&day=toc-update-%s.CIF.gz", weekdays[broken->tm_wday]);
      }
      
      // Fetch to temporary file name 
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
         
         _log(GENERAL, "Fetching \"%s\".", url);
         
         sprintf(filepathz, "/tmp/cifdb-cif-fetch-%ld.gz", now);
         sprintf(filepath,  "/tmp/cifdb-cif-fetch-%ld",    now);
         
         if(!(fp = fopen(filepathz, "w")))
         {
            _log(MAJOR, "Failed to open \"%s\" for writing.", filepathz);
            return 1;
         }
         
         curl_easy_setopt(curlh, CURLOPT_HTTPHEADER, slist);
         
         // Set timeouts
         curl_easy_setopt(curlh, CURLOPT_NOSIGNAL,              1L);
         curl_easy_setopt(curlh, CURLOPT_FTP_RESPONSE_TIMEOUT, 128L);
         curl_easy_setopt(curlh, CURLOPT_TIMEOUT,              128L);
         curl_easy_setopt(curlh, CURLOPT_CONNECTTIMEOUT,       128L);
         
         // Debugging prints.
         if(debug) curl_easy_setopt(curlh, CURLOPT_VERBOSE,               1L);
         
         // URL and login
         curl_easy_setopt(curlh, CURLOPT_URL,     url);
         sprintf(zs, "%s:%s", conf[conf_nr_user], conf[conf_nr_password]);
         curl_easy_setopt(curlh, CURLOPT_USERPWD, zs);
         curl_easy_setopt(curlh, CURLOPT_FOLLOWLOCATION,        1L);  // On receiving a 3xx response, follow the redirect.
         fetch_total_bytes = 0;
         
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
                  if(fp) fclose(fp);
                  fp = NULL;
                  return 1;
               }
            }
            else
            {
               if(fp) fclose(fp);
               fp = NULL;
               return 1;
            }
         }
         char * actual_url;
         if(!curl_easy_getinfo(curlh, CURLINFO_EFFECTIVE_URL, &actual_url) && actual_url)
         {
            _log(GENERAL, "Download was redirected to \"%s\".", actual_url);
         }
         
         if(fp) fclose(fp);
         fp = NULL;
         if(curlh) curl_easy_cleanup(curlh);
         curlh = NULL;
         if(slist) curl_slist_free_all(slist);
         slist = NULL;
         
         sprintf(zs, "Received %s bytes of compressed CIF updates.",  commas(fetch_total_bytes));
         _log(GENERAL, zs);
         
         if(fetch_total_bytes == 0) return 1;
         
         _log(GENERAL, "Decompressing data...");
         sprintf(zs, "/bin/gunzip -f %s", filepathz);
         char * rc;
         if((rc = system_call(zs)))
         {
            _log(MAJOR, "Failed to uncompress file:  %s", rc);
            if((fp = fopen(filepathz, "r")))
            {
               char error_message[2048];
               size_t length;
               if((length = fread(error_message, 1, 2047, fp)) && error_message[0] == '<')
               {
                  error_message[length] = '\0';
                  _log(MAJOR, "Received message:\n%s", error_message);   
               }
               fclose(fp);
            }
            return 1;
         }
         _log(GENERAL, "Decompressed.");
      }
   }
   else
   {
      strcpy(filepath, opt_filename);
   }
      
   if(!(fp = fopen(filepath, "r")))
   {
      _log(MAJOR, "Failed to open \"%s\" for reading.", filepath);
      return 1;
   }

   // Read enough of the file to find the datestamp
   char card[128];

   if(!fgets(card, sizeof(card), fp))
   {
      fclose(fp);
      _log(MAJOR, "Failed to read file header.");
      return 1;
   }
   else
   {
      fclose(fp);   
      card[strlen(card) - 1] = '\0'; // Lose the newline
      char record_identity[4];
      extract_field(card, 0, 2, record_identity);
      _log(GENERAL, "First card \"%s\".", card);
      if(strcmp("HD", record_identity))
      {
         //TODO Handle this properly
         _log(MAJOR, "File does not begin with a header record.");
         return 1;
      }
      
      struct tm broken;
      broken.tm_year = atoi(extract_field_s(card, 26, 2)) + 100;
      broken.tm_mon  = atoi(extract_field_s(card, 24, 2)) - 1;
      broken.tm_mday = atoi(extract_field_s(card, 22, 2));
      broken.tm_hour = atoi(extract_field_s(card, 28, 2));
      broken.tm_min  = atoi(extract_field_s(card, 30, 2));
      broken.tm_sec = 0;
      broken.tm_isdst = -1;
      fetch_extract_time = mktime(&broken); // Assumes local!

      _log(GENERAL, "Time of extract = %s", time_text(fetch_extract_time, true));

      if(!opt_filename)
      {
         // Build Filename
         sprintf(fetch_filepath, "/tmp/cif-%s-extracted-%04d-%02d-%02d", full_file?"full":"update", broken.tm_year + 1900, broken.tm_mon + 1, broken.tm_mday);

         struct stat z;
         word duplicates = 0;
         while(!stat(fetch_filepath, &z))
         {
            _log(DEBUG, "stat(\"%s\") found a file.", fetch_filepath);
            sprintf(fetch_filepath, "/tmp/cif-%s-extracted-%04d-%02d-%02d-%05d", full_file?"full":"update", broken.tm_year + 1900, broken.tm_mon + 1, broken.tm_mday, ++duplicates);
         }

         if(rename(filepath, fetch_filepath))
         {
            //TODO Handle this properly
            _log(ABEND, "Failed to rename(\"%s\", \"%s\").", filepath, fetch_filepath);
            exit(1);
         }
         _log(GENERAL, "Downloaded to file \"%s\".", fetch_filepath);
      }
      else
      {
         strcpy(fetch_filepath, opt_filename);
      }
   }
   return 0;
}

static size_t cif_write_data(void *buffer, size_t size, size_t nmemb, void *userp)
{
   char zs[128];
   _log(PROC, "cif_write_data()");

   size_t bytes = size * nmemb;

   sprintf(zs, "Bytes received = %zd", bytes);
   
   _log(DEBUG, zs);

   fwrite(buffer, size, nmemb, fp);

   fetch_total_bytes += bytes;

   return bytes;
}

static word process_file(void)
{
   char card[128];
   if(!(fp = fopen(fetch_filepath, "r")))
   {
      _log(MAJOR, "process_file():  Failed to open \"%s\" for reading.", fetch_filepath);
      return 1;
   }

   if(opt_test)
   {
      _log(GENERAL, "Ignoring data from file in test mode.");
      fclose(fp);
      return 0;
   }

   word fail = false;
   last_reported_time = time(NULL);

   update_id = 0;

   if(db_start_transaction()) return 1;

   while(fgets(card, sizeof(card), fp) && !fail)
   {
      // Comfort report
#define COMFORT_REPORT_PERIOD ((opt_print?1:10) * 60)
      time_t now = time(NULL);
      if(now - last_reported_time > COMFORT_REPORT_PERIOD)
      {
         char zs[128], zs1[128];
         sprintf(zs, "Progress:  Processed %s CIF cards.  ", commas_q(stats[CIFRecords]));
         sprintf(zs1, "Created %s schedules and ", commas_q(stats[ScheduleCreate]));
         strcat(zs, zs1);
         sprintf(zs1, "%s schedule locations.  Working...", commas_q(stats[ScheduleLocCreate]));
         strcat(zs, zs1);
         _log(GENERAL, zs);
         last_reported_time += COMFORT_REPORT_PERIOD;
      }

      card[strlen(card) - 1] = '\0'; // Lose the newline
      _log(DEBUG, "Line \"%s\", record identity \"%c%c\".", card, card[0], card[1]);
      if     (card[0] == 'H' && card[1] == 'D') fail = process_HD(card);
      else if(card[0] == 'B' && card[1] == 'S') fail = process_schedule(card);
      else if(card[0] == 'B' && card[1] == 'X') fail = process_schedule(card);
      else if(card[0] == 'L' && card[1] == 'O') fail = process_schedule(card);
      else if(card[0] == 'L' && card[1] == 'I') fail = process_schedule(card);
      else if(card[0] == 'L' && card[1] == 'T') fail = process_schedule(card);
      else if(card[0] == 'C' && card[1] == 'R') fail = process_schedule(card);
      else if(card[0] == 'A' && card[1] == 'A') ;
      else if(card[0] == 'T' && card[1] == 'I') fail = process_tiploc(card);
      else if(card[0] == 'T' && card[1] == 'A') fail = process_tiploc(card);
      else if(card[0] == 'T' && card[1] == 'D') fail = process_tiploc(card);
      else if(card[0] == 'Z' && card[1] == 'Z') ;
      else _log(MINOR, "Unexpected record %c%c ignored.", card[0], card[1]);
      stats[CIFRecords]++;
   }

   fclose(fp);

   if(!fail)
   {
      _log(GENERAL, "Committing database changes...");
      if(db_commit_transaction()) return 1;
   }
   if(fail) db_rollback_transaction();
   return fail;
}

static word process_HD(const char * const c)
{
   char query[256];

   _log(GENERAL, "Information from header card:");
   _log(GENERAL, "   Mainframe identity: %s", extract_field_s(c, 2, 20));
   _log(GENERAL, "         Extract time: %s", time_text(fetch_extract_time, true));
   _log(GENERAL, "     Current file ref: %s", extract_field_s(c, 32, 7));
   _log(GENERAL, "        Last file ref: %s", extract_field_s(c, 39, 7));
   _log(GENERAL, "     Update indicator: %s", extract_field_s(c, 46, 1));
   _log(GENERAL, "              Version: %s", extract_field_s(c, 47, 1));
   _log(GENERAL, "   Extract start date: %s", extract_field_s(c, 48, 6));
   _log(GENERAL, "     Extract end date: %s", extract_field_s(c, 54, 6));

   if(c[46] == 'F' && !opt_fetch_all)
   {
      _log(CRITICAL, "Expected an update, got a full extract.");
      return 1;
   }

   //Check if we've already processed this one, or one after it.
   {
      MYSQL_RES * result;
      MYSQL_ROW row;
      sprintf(query, "SELECT count(*) from updates_processed where time = %ld", fetch_extract_time);

      // Try twice in case database has gone away.
      if(db_connect() && db_connect()) return 1;
      if (db_query(query)) return 1;

      result = db_store_result();
      row = mysql_fetch_row(result);

      int rows = atoi(row[0]);

      mysql_free_result(result);

      if(rows > 0)
      {
         _log(CRITICAL, "A file with this extract time has already been processed.");
         return 1;
      }

      sprintf(query, "SELECT max(time) from updates_processed");

      if (db_query(query)) return 1;

      time_t latest = 0;
      result = db_store_result();
      if(result)
      {
         row = mysql_fetch_row(result);

         if(row && row[0]) latest = atol(row[0]);
         mysql_free_result(result);
      }

      if(latest > fetch_extract_time)
      {
         _log(CRITICAL, "A file with a later extract time (%s) has already been processed.", time_text(latest, true));
         return 1;
      }
   }

   //| id    | smallint(5) unsigned | NO   | PRI | NULL    | auto_increment |
   //| time  | int(10) unsigned     | NO   |     | NULL    |                |
   sprintf(query, "INSERT INTO updates_processed VALUES(0, %ld)", fetch_extract_time);
   if(db_query(query)) return 1;
   update_id = db_insert_id();
   _log(GENERAL, "Update id %ld", update_id);
   return 0;
}

#define EXTRACT_APPEND(a,b) { strcat(query, ", '"); strcat(query, extract_field_s(c, a, b)); strcat(query, "'"); }
static word process_schedule(const char * const c)
{
   static dword schedule_id;
   char query[1024], zs[128];
   word sort_arrive, sort_depart, sort_pass, sort_time;
   static word origin_sort_time;

   if(c[0] == 'B' && c[1] == 'S')
   {
      _log(DEBUG, "BS card \"%s\".", c);
      if(c[2] == 'R' || c[2] == 'D')
      {
         // Delete a schedule
         if(process_schedule_delete(c)) return 1;
      }
      if(c[2] == 'D')
      {
         return 0;
      }

      // Create a schedule
      sprintf(query, "INSERT INTO cif_schedules values(%ld, %ld, %lu", update_id, start_time, NOT_DELETED);

      // CIF_bank_holiday_running      | char(1)              | NO   |     | NULL    |                |
      EXTRACT_APPEND(28, 1);
      // CIF_stp_indicator             | char(1)              | NO   | MUL | NULL    |                |
      EXTRACT_APPEND(79, 1);
      // CIF_train_uid                 | char(6)              | NO   | MUL | NULL    |                |
      EXTRACT_APPEND( 3, 6);
      // applicable_timetable          | char(1)              | NO   |     | NULL    |                |
      strcat(query, ", ''"); // in BX record
      // atoc_code                     | char(2)              | NO   |     | NULL    |                |
      strcat(query, ", ''"); // in BX record
      // uic_code                      | char(5)              | NO   |     | NULL    |                |
      strcat(query, ", ''"); // in BX record
      // runs_mo                       | tinyint(1)           | NO   |     | NULL    |                |
      EXTRACT_APPEND(21, 1);
      // runs_tu                       | tinyint(1)           | NO   |     | NULL    |                |
      EXTRACT_APPEND(22, 1);
      // runs_we                       | tinyint(1)           | NO   |     | NULL    |                |
      EXTRACT_APPEND(23, 1);
      // runs_th                       | tinyint(1)           | NO   |     | NULL    |                |
      EXTRACT_APPEND(24, 1);
      // runs_fr                       | tinyint(1)           | NO   |     | NULL    |                |
      EXTRACT_APPEND(25, 1);
      // runs_sa                       | tinyint(1)           | NO   |     | NULL    |                |
      EXTRACT_APPEND(26, 1);
      // runs_su                       | tinyint(1)           | NO   |     | NULL    |                |
      EXTRACT_APPEND(27, 1);
      // schedule_end_date             | int(10) unsigned     | NO   | MUL | NULL    |                |
      sprintf(zs, ", %ld", parse_CIF_datestamp(extract_field_s(c, 15, 6)));
      strcat(query, zs);
      // signalling_id                 | char(4)              | NO   |     | NULL    |                |
      EXTRACT_APPEND(32, 4);
      // CIF_train_category            | char(2)              | NO   |     | NULL    |                |
      EXTRACT_APPEND(30, 2);
      // CIF_headcode                  | char(4)              | NO   |     | NULL    |                |
      EXTRACT_APPEND(36, 4);
      // CIF_train_service_code        | char(8)              | NO   |     | NULL    |                |
      EXTRACT_APPEND(41, 8);
      // CIF_business_sector           | char(1)              | NO   |     | NULL    |                |
      EXTRACT_APPEND(49, 1);
      // CIF_power_type                | char(3)              | NO   |     | NULL    |                |
      EXTRACT_APPEND(50, 3);
      // CIF_timing_load               | char(4)              | NO   |     | NULL    |                |
      EXTRACT_APPEND(53, 4);
      // CIF_speed                     | char(3)              | NO   |     | NULL    |                |
      EXTRACT_APPEND(57, 3);
      // CIF_operating_characteristics | char(6)              | NO   |     | NULL    |                |
      EXTRACT_APPEND(60, 6);
      // CIF_train_class               | char(1)              | NO   |     | NULL    |                |
      EXTRACT_APPEND(66, 1);
      // CIF_sleepers                  | char(1)              | NO   |     | NULL    |                |
      EXTRACT_APPEND(67, 1);
      // CIF_reservations              | char(1)              | NO   |     | NULL    |                |
      EXTRACT_APPEND(68, 1);
      // CIF_connection_indicator      | char(1)              | NO   |     | NULL    |                |
      EXTRACT_APPEND(69, 1);
      // CIF_catering_code             | char(4)              | NO   |     | NULL    |                |
      EXTRACT_APPEND(70, 4);
      // CIF_service_branding          | char(4)              | NO   |     | NULL    |                |
      EXTRACT_APPEND(74, 4);
      // schedule_start_date           | int(10) unsigned     | NO   | MUL | NULL    |                |
      sprintf(zs, ", %ld", parse_CIF_datestamp(extract_field_s(c, 9, 6)));
      strcat(query, zs);
      // train_status                  | char(1)              | NO   |     | NULL    |                |
      EXTRACT_APPEND(29, 1);
      // id                            | int(10) unsigned     | NO   | PRI | NULL    | auto_increment |
      // deduced_headcode              | char(4)              | NO   |     |         |                |
      // deduced_headcode_status       | char(1)              | NO   |     |         |                |
      strcat(query, ", 0, '', '')");

      //_log(GENERAL, "Query \"%s\"", query);

      if(db_query(query))
         return 1;

      schedule_id = db_insert_id();
      stats[ScheduleCreate]++;

      // c[79] is CIF_stp_indicator
      if(c[79] != 'C' && c[79] != 'N' && !opt_fetch_all)
      {
         // Search db for schedules with a deduced headcode, and add it to this one, status = D
         MYSQL_RES * result;
         MYSQL_ROW row;
         char uid[8];
         extract_field(c, 3, 6, uid);
         sprintf(query, "SELECT deduced_headcode FROM cif_schedules WHERE CIF_train_uid = '%s' AND deduced_headcode != '' AND schedule_end_date > %ld ORDER BY created DESC", uid, start_time - (64L * 24L * 60L * 60L));
         if(!db_query(query))
         {
            result = db_store_result();
            if((row = mysql_fetch_row(result)))
            {
               sprintf(query, "UPDATE cif_schedules SET deduced_headcode = '%s', deduced_headcode_status = 'D' WHERE id = %ld", row[0], schedule_id);
               db_query(query);
               stats[HeadcodeDeduced]++;
            }
            mysql_free_result(result);
         }
      }
      return 0;
   }

   if(c[0] == 'B' && c[1] == 'X')
   {
      // BS Extra. 
      // applicable_timetable          | char(1)              | NO   |     | NULL    |                |
      sprintf(query, "UPDATE cif_schedules SET applicable_timetable = '%s', atoc_code = '", extract_field_s(c, 13, 1));
      // atoc_code                     | char(2)              | NO   |     | NULL    |                |
      strcat(query, extract_field_s(c, 11, 2));
      strcat(query, "', uic_code = '");
      // uic_code                      | char(5)              | NO   |     | NULL    |                |
      strcat(query, extract_field_s(c, 6, 5));
      sprintf(zs, "' WHERE id = %ld", schedule_id);
      strcat(query, zs);
      return db_query(query);
   }

   if(c[0] == 'L')
   {
      if(c[1] == 'O')
      {
         //| update_id             | smallint(5) unsigned | NO   |     | NULL    |       |
         //| cif_schedule_id       | int(10) unsigned     | NO   | MUL | NULL    |       |
         sprintf(query, "INSERT INTO cif_schedule_locations VALUES(%ld, %ld", update_id, schedule_id);
         //| location_type         | char(12)             | NO   |     | NULL    |       |
         EXTRACT_APPEND(29, 12);
         //| record_identity       | char(2)              | NO   |     | NULL    |       |
         EXTRACT_APPEND(0, 2);
         //| tiploc_code           | char(7)              | NO   | MUL | NULL    |       |
         EXTRACT_APPEND(2, 7);
         //| tiploc_instance       | char(1)              | NO   |     | NULL    |       |
         EXTRACT_APPEND(9, 1);
         //| arrival               | char(5)              | NO   |     | NULL    |       |
         strcat(query, ", ''");
         //| departure             | char(5)              | NO   |     | NULL    |       |
         EXTRACT_APPEND(10, 5);
         //| pass                  | char(5)              | NO   |     | NULL    |       |
         strcat(query, ", ''");
         //| public_arrival        | char(4)              | NO   |     | NULL    |       |
         strcat(query, ", ''");
         //| public_departure      | char(4)              | NO   |     | NULL    |       |
         if(strcmp(extract_field_s(c, 15, 4), "0000")) EXTRACT_APPEND(15, 4)
            else strcat(query, ", ''");
         //| sort_time             | smallint(5) unsigned | NO   |     | NULL    |       |
         //| next_day              | tinyint(1)           | NO   |     | NULL    |       |
         sort_time = get_sort_time(extract_field_s(c, 10, 5));
         sprintf(zs, ", %d, 0", sort_time);
         strcat(query, zs);
         origin_sort_time = sort_time;
         //| platform              | char(3)              | NO   |     | NULL    |       |
         EXTRACT_APPEND(19, 3);
         //| line                  | char(3)              | NO   |     | NULL    |       |
         EXTRACT_APPEND(22, 3);
         //| path                  | char(3)              | NO   |     | NULL    |       |
         strcat(query, ", ''");
         //| engineering_allowance | char(2)              | NO   |     | NULL    |       |
         EXTRACT_APPEND(25, 2);
         //| pathing_allowance     | char(2)              | NO   |     | NULL    |       |
         EXTRACT_APPEND(27, 2);
         //| performance_allowance | char(2)              | NO   |     | NULL    |       |
         EXTRACT_APPEND(41, 2);
         strcat(query, ")");
         if(db_query(query)) return 1;
         stats[ScheduleLocCreate]++;
      }
      else if(c[1] == 'I')
      {
         //| update_id             | smallint(5) unsigned | NO   |     | NULL    |       |
         //| cif_schedule_id       | int(10) unsigned     | NO   | MUL | NULL    |       |
         sprintf(query, "INSERT INTO cif_schedule_locations VALUES(%ld, %ld", update_id, schedule_id);
         //| location_type         | char(12)             | NO   |     | NULL    |       |
         EXTRACT_APPEND(42, 12);
         //| record_identity       | char(2)              | NO   |     | NULL    |       |
         EXTRACT_APPEND(0, 2);
         //| tiploc_code           | char(7)              | NO   | MUL | NULL    |       |
         EXTRACT_APPEND(2, 7);
         //| tiploc_instance       | char(1)              | NO   |     | NULL    |       |
         EXTRACT_APPEND(9, 1);
         //| arrival               | char(5)              | NO   |     | NULL    |       |
         EXTRACT_APPEND(10, 5);
         //| departure             | char(5)              | NO   |     | NULL    |       |
         EXTRACT_APPEND(15, 5);
         //| pass                  | char(5)              | NO   |     | NULL    |       |
         EXTRACT_APPEND(20, 5);
         //| public_arrival        | char(4)              | NO   |     | NULL    |       |
         if(strcmp(extract_field_s(c, 25, 4), "0000")) EXTRACT_APPEND(25, 4)
            else strcat(query, ", ''");
         //| public_departure      | char(4)              | NO   |     | NULL    |       |
         if(strcmp(extract_field_s(c, 29, 4), "0000")) EXTRACT_APPEND(29, 4)
            else strcat(query, ", ''");
         //| sort_time             | smallint(5) unsigned | NO   |     | NULL    |       |
         //| next_day              | tinyint(1)           | NO   |     | NULL    |       |
         sort_arrive = get_sort_time(extract_field_s(c, 10, 5));
         sort_depart = get_sort_time(extract_field_s(c, 15, 5));
         sort_pass   = get_sort_time(extract_field_s(c, 20, 5));
         if(sort_arrive < INVALID_SORT_TIME) sort_time = sort_arrive;
         else if(sort_depart < INVALID_SORT_TIME) sort_time = sort_depart;
         else sort_time = sort_pass;
         sprintf(zs, ", %d, %d", sort_time, (sort_time < origin_sort_time)?1:0);
         strcat(query, zs);
         //| platform              | char(3)              | NO   |     | NULL    |       |
         EXTRACT_APPEND(33, 3);
         //| line                  | char(3)              | NO   |     | NULL    |       |
         EXTRACT_APPEND(36, 3);
         //| path                  | char(3)              | NO   |     | NULL    |       |
         EXTRACT_APPEND(39, 3);
         //| engineering_allowance | char(2)              | NO   |     | NULL    |       |
         EXTRACT_APPEND(54, 2);
         //| pathing_allowance     | char(2)              | NO   |     | NULL    |       |
         EXTRACT_APPEND(56, 2);
         //| performance_allowance | char(2)              | NO   |     | NULL    |       |
         EXTRACT_APPEND(58, 2);
         strcat(query, ")");
         if(db_query(query)) return 1;
         stats[ScheduleLocCreate]++;
      }
      else if(c[1] == 'T')
      {
         //| update_id             | smallint(5) unsigned | NO   |     | NULL    |       |
         //| cif_schedule_id       | int(10) unsigned     | NO   | MUL | NULL    |       |
         sprintf(query, "INSERT INTO cif_schedule_locations VALUES(%ld, %ld", update_id, schedule_id);
         //| location_type         | char(12)             | NO   |     | NULL    |       |
         EXTRACT_APPEND(25, 12);
         //| record_identity       | char(2)              | NO   |     | NULL    |       |
         EXTRACT_APPEND(0, 2);
         //| tiploc_code           | char(7)              | NO   | MUL | NULL    |       |
         EXTRACT_APPEND(2, 7);
         //| tiploc_instance       | char(1)              | NO   |     | NULL    |       |
         EXTRACT_APPEND(9, 1);
         //| arrival               | char(5)              | NO   |     | NULL    |       |
         EXTRACT_APPEND(10, 5);
         //| departure             | char(5)              | NO   |     | NULL    |       |
         strcat(query, ", ''");
         //| pass                  | char(5)              | NO   |     | NULL    |       |
         strcat(query, ", ''");
         //| public_arrival        | char(4)              | NO   |     | NULL    |       |
         if(strcmp(extract_field_s(c, 15, 4), "0000")) EXTRACT_APPEND(15, 4)
            else strcat(query, ", ''");
         //| public_departure      | char(4)              | NO   |     | NULL    |       |
         strcat(query, ", ''");
         //| sort_time             | smallint(5) unsigned | NO   |     | NULL    |       |
         //| next_day              | tinyint(1)           | NO   |     | NULL    |       |
         sort_time = get_sort_time(extract_field_s(c, 10, 5));
         sprintf(zs, ", %d, %d", sort_time, (sort_time < origin_sort_time)?1:0);
         strcat(query, zs);
         //| platform              | char(3)              | NO   |     | NULL    |       |
         EXTRACT_APPEND(19, 3);
         //| line                  | char(3)              | NO   |     | NULL    |       |
         strcat(query, ", ''");
         //| path                  | char(3)              | NO   |     | NULL    |       |
         EXTRACT_APPEND(22, 3);
         //| engineering_allowance | char(2)              | NO   |     | NULL    |       |
         strcat(query, ", ''");
         //| pathing_allowance     | char(2)              | NO   |     | NULL    |       |
         strcat(query, ", ''");
         //| performance_allowance | char(2)              | NO   |     | NULL    |       |
         strcat(query, ", ''");
         strcat(query, ")");
         if(db_query(query)) return 1;
         stats[ScheduleLocCreate]++;
      }

      if(conf[conf_huyton_alerts][0])
      {
         if((!strcasecmp(extract_field_s(c, 2, 7), "HUYTON ")) ||
            (!strcasecmp(extract_field_s(c, 2, 7), "HUYTJUN")))
         {
            if(home_report_index < HOME_REPORT_SIZE)
            {
               word i;
               for(i = 0; i < home_report_index && home_report_id[i] != schedule_id; i++);
               if(i == home_report_index)
               {
                  home_report_id[home_report_index++] = schedule_id;
               }
            }
            else
            {
               home_report_index++;
            }
         }
      }
      return 0;
   }

   if(c[0] == 'C' && c[1] == 'R')
   {
      //| cif_schedule_id               | int(10) unsigned | NO   | MUL | NULL    |       |
      sprintf(query, "INSERT INTO cif_changes_en_route VALUES(%ld", schedule_id);
      //| tiploc_code                   | char(7)          | NO   | MUL | NULL    |       |
      EXTRACT_APPEND(2, 7);
      //| tiploc_instance               | char(1)          | NO   |     | NULL    |       |
      EXTRACT_APPEND(9, 1);
      //| CIF_train_category            | char(2)          | NO   |     | NULL    |       |
      EXTRACT_APPEND(10, 2);
      //| signalling_id                 | char(4)          | NO   |     | NULL    |       |
      EXTRACT_APPEND(12, 4);
      //| CIF_headcode                  | char(4)          | NO   |     | NULL    |       |
      EXTRACT_APPEND(16, 4);
      //| CIF_train_service_code        | char(8)          | NO   |     | NULL    |       |
      EXTRACT_APPEND(21, 8);
      //| CIF_power_type                | char(3)          | NO   |     | NULL    |       |
      EXTRACT_APPEND(30, 3);
      //| CIF_timing_load               | char(4)          | NO   |     | NULL    |       |
      EXTRACT_APPEND(33, 4);
      //| CIF_speed                     | char(3)          | NO   |     | NULL    |       |
      EXTRACT_APPEND(37, 3);
      //| CIF_operating_characteristics | char(6)          | NO   |     | NULL    |       |
      EXTRACT_APPEND(40, 6);
      //| CIF_train_class               | char(1)          | NO   |     | NULL    |       |
      EXTRACT_APPEND(46, 1);
      //| CIF_sleepers                  | char(1)          | NO   |     | NULL    |       |
      EXTRACT_APPEND(47, 1);
      //| CIF_reservations              | char(1)          | NO   |     | NULL    |       |
      EXTRACT_APPEND(48, 1);
      //| CIF_connection_indicator      | char(1)          | NO   |     | NULL    |       |
      EXTRACT_APPEND(49, 1);
      //| CIF_catering_code             | char(4)          | NO   |     | NULL    |       |
      EXTRACT_APPEND(50, 4);
      //| CIF_service_branding          | char(4)          | NO   |     | NULL    |       |
      EXTRACT_APPEND(54, 4);
      //| uic_code                      | char(5)          | NO   |     | NULL    |       |
      EXTRACT_APPEND(62, 5);
      strcat(query, ")");
      if(db_query(query)) return 1;
      stats[ScheduleCR]++;
      return 0;
   }

   _log(MAJOR, "Unexpected card \"%s\".", c);
   return 1;
}

static word process_schedule_delete(const char * const c)
{
   MYSQL_RES * result0, * result1;
   MYSQL_ROW row0;
   char query[512];
   char CIF_train_uid[8], CIF_stp_indicator[2];
   time_t schedule_start_date;
   word deleted = 0;

   extract_field(c, 3, 6, CIF_train_uid);
   schedule_start_date = parse_CIF_datestamp(extract_field_s(c, 9, 6));
   extract_field(c, 79, 1, CIF_stp_indicator);

   // Find the id
   sprintf(query, "SELECT id FROM cif_schedules WHERE update_id != 0 AND CIF_train_uid = '%s' AND schedule_start_date = '%ld' AND CIF_stp_indicator = '%s' AND deleted > %ld",
           CIF_train_uid, schedule_start_date, CIF_stp_indicator, start_time);
 
   if (db_query(query))
   {
      return 1;
   }

   result0 = db_store_result();
   word num_rows = mysql_num_rows(result0);

   if(num_rows > 1)
   {
      _log(MINOR, "Delete (%c) schedule CIF_train_uid = \"%s\", schedule_start_date = %s, CIF_stp_indicator = %s found %d matches.  All deleted.", c[2], CIF_train_uid, date_text(schedule_start_date, false), CIF_stp_indicator, num_rows);
      if(debug)
      {
         _log(DEBUG, "\"%s\"", c);

         // Bodge!
         query[7] = '*'; query[8] = ' ';
         dump_mysql_result_query(query);
      }
   }
 
   while((row0 = mysql_fetch_row(result0)) && row0[0]) 
   {
      dword id = atol(row0[0]);
      if(num_rows > 1) _log(MINOR, "   Schedule ID %ld.", id);
      sprintf(query, "UPDATE cif_schedules SET deleted = %ld where id = %ld", start_time, id);
   
      if(!db_query(query))
      {
         deleted++;
      }
      else return 1;
      if(conf[conf_huyton_alerts][0])
      {
         sprintf(query, "SELECT * FROM cif_schedule_locations WHERE cif_schedule_id = %ld AND (tiploc_code = 'HUYTON' OR tiploc_code = 'HUYTJUN')", id);
         if(!db_query(query))
         {
            result1 = db_store_result();
            if(mysql_num_rows(result1))
            { 
               if(home_report_index < HOME_REPORT_SIZE)
               {
                  home_report_id[home_report_index++] = id;
               }
               else
               {
                  home_report_index++;
               }
            }
            mysql_free_result(result1);
         }
      }
   }
   mysql_free_result(result0);

   if(deleted) 
   {
      stats[ScheduleDeleteHit] += deleted;
   }
   else
   {
      stats[ScheduleDeleteMiss]++;
      _log(MAJOR, "Delete schedule miss: \"%s\".", c);
   }
   return 0;
}

static word process_tiploc(const char * const c)
{
   if(!tiploc_ignored)
   {
      _log(MINOR, "TIPLOC card(s) ignored.");
      tiploc_ignored = true;
   }
   return 0;
}

static char * tiploc_name(const char * const tiploc)
{
   // Not re-entrant
   char query[256];
   MYSQL_RES * result;
   MYSQL_ROW row;
   static char name[128];

   sprintf(query, "select fn from corpus where tiploc = '%s'", tiploc);
   db_query(query);
   result = db_store_result();
   if((row = mysql_fetch_row(result)) && row[0][0]) 
   {
      strncpy(name, row[0], 127);
      name[127] = '\0';
   }
   else
   {
      strcpy(name, tiploc);
   }
   mysql_free_result(result);
   return name;
}

static void extract_field(const char * const c, size_t s, size_t l, char * d)
{
   // ASSUMES l and d are big enough !!!

   size_t i;

   for(i = 0; i < l; i++) d[i] = c[s + i];
   d[i] = '\0';
} 

static char * extract_field_s(const char * const c, size_t s, size_t l)
{
   static char result[128];
   if(s > 80) s = 80;
   extract_field(c, s, l, result);

   return result;
}

static time_t parse_CIF_datestamp(const char * const s)
{
   // Only works for yymmdd
   char zs[128];

   sprintf(zs, "20%c%c-%c%c-%c%c", s[0],s[1],s[2],s[3],s[4],s[5]);
   return parse_datestamp(zs);
}
