/*
    Copyright (C) 2020, 2022 Phil Wieland

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
#include <dirent.h>
#include <errno.h>
#include <sys/prctl.h>

#include "misc.h"
#include "db.h"
#include "database.h"
#include "build.h"

#define NAME "cifmerge"

#ifndef RELEASE_BUILD
#define BUILD "3721p"
#else
#define BUILD RELEASE_BUILD
#endif

static word debug, run, opt_merge, opt_print, opt_insecure, used_insecure, opt_force;
static char * opt_filename;
static dword update_id;
static time_t start_time, last_reported_time;
#define INVALID_SORT_TIME 9999

#define NOT_DELETED 0xffffffffL

#define TEMP_DIRECTORY "/var/tmp"

// Stats
enum stats_categories {Fetches,  
                       CIFRecords,
                       ScheduleExamined, ScheduleOld, ScheduleMissing, ScheduleMatch1, ScheduleMatchM, ScheduleUnmatched,
                       ScheduleLocCreate, ScheduleCR,
                       AssocCreate, AssocDeleteHit, AssocDeleteMiss,
                       TIPLOCCreate, TIPLOCAmendHit, TIPLOCAmendMiss, TIPLOCDeleteHit, TIPLOCDeleteMiss, 
                       HeadcodeDeduced,
                       MAXStats };
static qword stats[MAXStats];
static const char * const stats_category[MAXStats] = 
   {
      "File fetch",  
      "CIF record", 
      "Download Schedule examined", "Discarded, ended", "Schedule missing", "Schedule present", "Schedule multiple match", "Schedule unmatched",
      "Schedule location create", "Schedule CR create",
      "Association create", "Association delete hit", "Association delete miss",
      "TIPLOC create", "TIPLOC amend hit", "TIPLOC amend miss", "TIPLOC delete hit", "TIPLOC delete miss",
      "Deduced schedule headcode",
   };
#define HOME_REPORT_SIZE 512
static unsigned long home_report_id[HOME_REPORT_SIZE];
static char home_report_action[HOME_REPORT_SIZE];
static word home_report_index;

static FILE * fp;

static size_t fetch_total_bytes;
static time_t fetch_extract_time;
static char fetch_filepath[512];

#define REPORT_SIZE 16384
static char report[REPORT_SIZE];

// List of unmatched schedules in database
#define MAX_UNMATCHED 64000000
static byte unmatched[MAX_UNMATCHED];
static dword unmatched_base;

// Incoming CIF cards
static char CIF_BS[128];
static char CIF_BX[128];
#define MAX_CIF_L 512
static word CIF_L_next, CIF_schedule_next;
static char CIF_L[MAX_CIF_L][128];
static char CIF_schedule[MAX_CIF_L][128];

// Output file
static FILE * fp_out;

static word fetch_file(void);
static size_t cif_write_data(void *buffer, size_t size, size_t nmemb, void *userp);
static word process_file(void);
static void final_report(void);
static word process_HD(const char * const c);
static word process_association(const char * const c);
static word process_schedule(const char * const c);
static word create_schedule(void);
static word process_schedule_delete(const char * const c);
static word merge_schedule(void);
static word process_tiploc(const char * const c);
static word create_tiploc(const char * const c);
static word get_sort_time(const char * const buffer);
static char * tiploc_name(const char * const tiploc);
static void extract_field(const char * const c, const size_t s, const size_t l, char * const d);
static char * extract_field_s(const char * const c, const size_t s, const size_t l);
static const char * const CIF_BS_CIF_train_uid(void);
static const time_t CIF_BS_schedule_start_date(void);
static const time_t CIF_BS_schedule_end_date(void);
static const char * const CIF_BS_CIF_stp_indicator(void);
static const char * const CIF_L_record_identity(const word i);
static const char * const CIF_L_tiploc_code(const word i);
static const char * const CIF_L_tiploc_instance(const word i);
static const char * const CIF_L_arrival(const word i);
static const char * const CIF_L_departure(const word i);
static const char * const CIF_L_pass(const word i);
static const char * const CIF_L_platform(const word i);
static const char * const CIF_L_line(const word i);
static const char * const CIF_L_path(const word i);
static time_t parse_CIF_datestamp(const char * const s);
static void dump_schedule(void);

int main(int argc, char **argv)
{
   char config_file_path[256];
   opt_filename = NULL;
   opt_merge = false;
   opt_print = false;
   opt_insecure = false;
   opt_force = false;
   used_insecure = false;

   strcpy(config_file_path, "/etc/openrail.conf");
   word usage = false;
   int c;
   while ((c = getopt (argc, argv, ":c:f:opihm")) != -1)
      switch (c)
      {
      case 'c':
         strcpy(config_file_path, optarg);
         break;
      case 'f':
         opt_filename = optarg;
         break;
      case 'o':
         opt_force = true;
         break;
      case 'm':
         opt_merge = true;
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
             "default    Fetch Friday full timetable.\n"
             "-f <file>  Use specified file.  (Must already be decompressed.)\n"
             "Actions:\n"
             "default    Report only, do not alter database.\n"
             "-m         Update database.\n"
             "-o         Override day of week check.\n"
             "Options:\n"
             "-i         Insecure.  Circumvent certificate checks if necessary.\n"
             "-p         Print activity as well as logging.\n"
             );
      exit(1);
   }

   start_time = time(NULL);

   _log_init(debug?"/tmp/cifmerge.log":"/var/log/garner/cifmerge.log", (debug?1:(opt_print?4:0)));
   
   _log(GENERAL, "");
   _log(GENERAL, "%s %s", NAME, BUILD);

   // SPECIAL FOR TEST
   //start_time = 1588417200;
   //_log(GENERAL, "******** Test version fake run time %s. ********", time_text(start_time, true));

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
 
   // Check core dumps
   if(prctl(PR_GET_DUMPABLE, 0, 0, 0, 0) != 1)
   {
      _log(MAJOR, "PR_GET_DUMPABLE not 1.");
   }
   else
   {
      _log(DEBUG, "PR_GET_DUMPABLE is 1.");
   }
   if(!getrlimit(RLIMIT_CORE, &limit))
   {
      if(limit.rlim_cur != RLIM_INFINITY)
      {
         _log(MAJOR, "RLIMIT_CORE not RLIM_INFINITY.");
      }
      else
      {
         _log(DEBUG, "RLIMIT_CORE is RLIM_INFINITY.");
      }
   }
   else
   {
      _log(MAJOR, "Unable to determine RLIMIT_CORE.");
   }

   // Initialise database
   if(db_init(conf[conf_db_server], conf[conf_db_user], conf[conf_db_password], conf[conf_db_name], DB_MODE_NORMAL)) exit(1);

   {
      word e;
      if((e = database_upgrade(cifdb)))
      {
         _log(CRITICAL, "Error %d in database_upgrade().  Aborting.", e);
         exit(1);
      }
   }
   
   run = true;
   
   // Zero the stats
   {
      word i;
      for(i = 0; i < MAXStats; i++) { stats[i] = 0; }
   }
   home_report_index = 0;
   fetch_total_bytes = 0;
   fetch_extract_time = 0;
   fetch_filepath[0] = '\0';

   // Check day of week
   struct tm * broken = localtime(&start_time);
   if(broken->tm_wday != 6 && !opt_force)
   {
      _log(GENERAL, "This program should normally be run on a Saturday.  Use -o to override this restriction.");
      exit(1);
   }

   if(fetch_file())
   {
      exit(1);
   }
   

   if(process_file()) exit(1);

   // All done.  Send Report
   final_report();
   db_disconnect();

   // Leave tidying of temp files to next cifdb run.
   
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
      strcat(report, "*** Warning: Insecure download used.\n\n");
      _log(GENERAL, "*** Warning: Insecure download used.");
   }

   sprintf(zs, "               Data extract time: %s", fetch_extract_time?time_text(fetch_extract_time, true):"Unknown");
   _log(GENERAL, zs);
   strcat(report, zs); strcat(report, "\n");
   if(opt_filename)
   {
      sprintf(zs, "                  Data from file: %s", opt_filename);
      _log(GENERAL, zs);
      strcat(report, zs); strcat(report, "\n");
   }

   sprintf(zs, "                    Elapsed time: %ld minutes", (time(NULL) - start_time + 30) / 60);
   _log(GENERAL, zs);
   strcat(report, zs); strcat(report, "\n");
   for(i=0; i<MAXStats; i++)
   {
      sprintf(zs, "%32s: %s", stats_category[i], commas_q(stats[i]));
      _log(GENERAL, zs);
      strcat(report, zs);
      strcat(report, "\n");
   }
   
   email_alert(NAME, BUILD, "Timetable Merge Report", report);
}

static word get_sort_time(const char * const buffer)
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

static word fetch_file(void)
{
   // Returns 0=Success or error code.
   // In success case ONLY, returns following info in globals:
   // fetch_total_bytes
   // fetch_extract_time
   // fetch_filepath 

   char zs[256], filepathz[256], filepath[256], url[256];

   stats[Fetches]++;

   fp = NULL;

   if(!opt_filename)
   {
      // Build URL
      {
         sprintf(url, "https://%s/ntrod/CifFileAuthenticate?type=CIF_ALL_FULL_DAILY&day=toc-full.CIF.gz", conf[conf_nr_server]);
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
         
         sprintf(filepathz, "%s/cifmerge-cif-fetch-%ld.gz", TEMP_DIRECTORY, start_time);
         sprintf(filepath,  "%s/cifmerge-cif-fetch-%ld",    TEMP_DIRECTORY, start_time);
         
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
         
         _log(GENERAL, "Received %s bytes of compressed CIF cards.",  commas(fetch_total_bytes));
         
         if(fetch_total_bytes == 0) return 1;
         
         _log(GENERAL, "Decompressing data...");
         sprintf(zs, "/bin/gunzip -f %.200s", filepathz);
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
      _log(DEBUG, "First card \"%s\".", card);
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
         sprintf(fetch_filepath, "%s/cifdb-cif-%s-extracted-%04d-%02d-%02d", TEMP_DIRECTORY, true?"-full-":"update", broken.tm_year + 1900, broken.tm_mon + 1, broken.tm_mday);

         struct stat z;
         word duplicates = 0;
         while(!stat(fetch_filepath, &z))
         {
            _log(DEBUG, "stat(\"%s\") found a file.", fetch_filepath);
            sprintf(fetch_filepath, "%s/cifdb-cif-%s-extracted-%04d-%02d-%02d-%05d", TEMP_DIRECTORY, true?"-full-":"update", broken.tm_year + 1900, broken.tm_mon + 1, broken.tm_mday, ++duplicates);
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
   _log(PROC, "cif_write_data()");

   size_t bytes = size * nmemb;

   _log(DEBUG, "Bytes received = %zd", bytes);
   
   fwrite(buffer, size, nmemb, fp);

   fetch_total_bytes += bytes;

   return bytes;
}

static word process_file(void)
{
   qword cards;
   word fail;
   char card[128];
   char query[1024];
   int n;
   qword z;
   
   if(!(fp = fopen(fetch_filepath, "r")))
   {
      _log(MAJOR, "process_file():  Failed to open \"%s\" for reading.", fetch_filepath);
      return 1;
   }

   sprintf(query, "%s/cifmerge-output.cif", TEMP_DIRECTORY);
   if(!(fp_out = fopen(query, "w")))
   {
      _log(MAJOR, "process_file():  Failed to open \"%s\" for writing.", query);
      return 1;
   }

   // Initialise card stores
   CIF_BS[0] = '\0';
   CIF_BX[0] = '\0';
   for(n = 0; n < MAX_CIF_L; n++) { CIF_L[n][0] = '\0'; CIF_schedule[n][0] = '\0'; }
   CIF_L_next = CIF_schedule_next = 0;

   // Initialise unmatched
   for(z = 0; z < MAX_UNMATCHED; z++) unmatched[z] = false;
   
   fail = false;
   cards = 0;
   while(fgets(card, sizeof(card), fp) && !fail)
   {
      cards++;
   }
   fseeko(fp, 0, SEEK_SET);

   _log(GENERAL, "%s CIF cards downloaded.", commas_q(cards));

   last_reported_time = time(NULL);

   update_id = 0;

   if(db_start_transaction()) return 1;

   {
      MYSQL_RES * result;
      MYSQL_ROW row;

      _log(GENERAL, "Analysing schedules in database...");
      //                                                                                               Exclude VSTP
      sprintf(query, "SELECT MIN(id), MAX(id) FROM cif_schedules WHERE deleted > %ld AND schedule_end_date > %ld AND update_id > 0", start_time, start_time);
      if(db_query(query)) return 1;
      result = db_store_result();
      if(result && (row = mysql_fetch_row(result)))
      {
         unmatched_base = atol(row[0]);
      }
      else
      {
         _log(CRITICAL, "Failed to determine unmatched base.");
         return 1;
      }
      mysql_free_result(result);
      
      _log(GENERAL, "   Unmatched list: Base  %s.", commas(unmatched_base));
      _log(GENERAL, "                   Range %s.", commas(atol(row[1]) - unmatched_base));
      
      //                                                                                               Exclude VSTP
      sprintf(query, "SELECT id FROM cif_schedules WHERE deleted > %ld AND schedule_end_date > %ld AND update_id > 0", start_time, start_time);
      if(db_query(query)) return 1;
      result = db_store_result();
      z = 0;
      while(result && (row = mysql_fetch_row(result)))
      {
         z++;
         dword id = atol(row[0]);
         if(id - unmatched_base >= MAX_UNMATCHED)
         {
            _log(CRITICAL, "Run out of schedule list space.");
            return 1;
         }
         unmatched[id - unmatched_base] = true;
      }
      mysql_free_result(result);
      _log(GENERAL, "   Found %s active schedules in database.", commas_q(z));
   }

   _log(GENERAL, "Reading download and comparing with database...");
   
   while(fgets(card, sizeof(card), fp) && !fail)
   {
      // Comfort report
#define COMFORT_REPORT_PERIOD ((opt_print?1:10) * 60)
      time_t now = time(NULL);
      if(now - last_reported_time > COMFORT_REPORT_PERIOD)
      {
         char zs[256], zs1[256];
         sprintf(zs, "Progress:  Processed %s (%lld%%) of ", commas_q(stats[CIFRecords]), ((100*stats[CIFRecords]) + (cards/2)) / cards);
         sprintf(zs1, "%s CIF cards.  ", commas_q(cards));
         strcat(zs, zs1);
         sprintf(zs1, "Examined %s downloaded schedules.  Working...", commas_q(stats[ScheduleExamined]));
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
      else if(card[0] == 'A' && card[1] == 'A') /* fail = process_association(card) */;
      else if(card[0] == 'T' && card[1] == 'I') /* fail = process_tiploc(card) */;
      else if(card[0] == 'T' && card[1] == 'A') /* fail = process_tiploc(card) */;
      else if(card[0] == 'T' && card[1] == 'D') /* fail = process_tiploc(card) */;
      else if(card[0] == 'Z' && card[1] == 'Z') ;
      else _log(MINOR, "Unexpected record %c%c ignored.", card[0], card[1]);
      stats[CIFRecords]++;
   }

   // Process left-overs
   if(!fail) fail = merge_schedule();

   // Look for schedules in database not in download.
   _log(GENERAL, "Checking for database schedules not in download...");
   for(z = 0; z < MAX_UNMATCHED && !fail; z++)
   {
      if(unmatched[z])
      {
         _log(GENERAL, "Unmatched record in database, id %lld.", z + unmatched_base);
         stats[ScheduleUnmatched]++;
         // Delete it
      }
   }
   
   fclose(fp);
   fclose(fp_out);
   
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

   if(c[46] != 'F' )
   {
      _log(CRITICAL, "Expected a full extract.");
      return 1;
   }

   if(opt_merge)
   {
      sprintf(query, "INSERT INTO updates_processed VALUES(0, %ld, 2)", fetch_extract_time);
      if(db_query(query)) return 1;
      update_id = db_insert_id();
      _log(GENERAL, "Update id %ld", update_id);
   }
   return 0;
}

#define EXTRACT_APPEND(a,b) { strcat(query, ", '"); strcat(query, extract_field_s(c, a, b)); strcat(query, "'"); }
#define EXTRACT_APPEND_ESCAPE(a,b) { strcat(query, ", '"); strcpy(zs, extract_field_s(c, a, b)); db_real_escape_string(zs1, zs, strlen(zs)); strcat(query, zs1); strcat(query, "'"); }

static word process_association(const char * const c)
{
   MYSQL_RES * result;
   char query[1024], query1[1024], zs[128];

   // Record AA
   _log(DEBUG, "AA card \"%s\".", c);

   if(c[2] == 'R' || c[2] == 'D')
   {
      // Delete an association
      sprintf(query1, " WHERE main_train_uid = '%s'", extract_field_s(c, 3, 6));
      sprintf(query,  " AND assoc_train_uid = '%s'",  extract_field_s(c, 9, 6));
      strcat(query1, query);
      sprintf(query,  " AND assoc_start_date = %ld",  parse_CIF_datestamp(extract_field_s(c, 15, 6)));
      strcat(query1, query);
      sprintf(query,  " AND location = '%s'",         extract_field_s(c, 37, 7));
      strcat(query1, query);
      sprintf(query,  " AND base_location_suffix = '%s'", extract_field_s(c, 44,1));
      strcat(query1, query);
      sprintf(query,  " AND deleted > %ld", start_time);
      strcat(query1, query);

      sprintf(query, "SELECT * FROM cif_associations %s", query1);
      
      if(db_query(query)) return 1;
      result = db_store_result();
      word num_rows = mysql_num_rows(result);
      mysql_free_result(result);

      if(num_rows > 1)
      {
         _log(MINOR, "AA card \"%s\".", c);
         _log(MINOR, "   Delete (%c) association found %d matches.  All deleted.", c[2], num_rows);
      }

      if(num_rows < 1)
      {
         _log(MINOR, "AA card \"%s\".", c);
         _log(MINOR, "   Delete (%c) association found no matches.", c[2]);
         stats[AssocDeleteMiss]++;
      }

      sprintf(query, "UPDATE cif_associations set deleted = %ld %s", start_time, query1);
      if(db_query(query)) return 1;

      stats[AssocDeleteHit] += num_rows;      
   }
   if(c[2] == 'D')
   {
      return 0;
   }

   // Create an association
   sprintf(query, "INSERT INTO cif_associations values(%d, %ld, %lu", update_id, start_time, NOT_DELETED);

   // main_train_uid        | char(6)              | NO   |     | NULL    |       |
   EXTRACT_APPEND( 3, 6);
   // assoc_train_uid       | char(6)              | NO   |     | NULL    |       |
   EXTRACT_APPEND( 9, 6);
   // assoc_start_date      | int(10) unsigned     | NO   |     | NULL    |       |
   sprintf(zs, ", %ld", parse_CIF_datestamp(extract_field_s(c, 15, 6)));
   strcat(query, zs);
   // assoc_end_date        | int(10) unsigned     | NO   |     | NULL    |       |
   sprintf(zs, ", %ld", parse_CIF_datestamp(extract_field_s(c, 21, 6)));
   strcat(query, zs);
   // assoc_days            | char(7)              | NO   |     | NULL    |       |
   EXTRACT_APPEND(27, 7);
   // category              | char(2)              | NO   |     | NULL    |       |
   EXTRACT_APPEND(34, 2);
   // date_indicator        | char(1)              | NO   |     | NULL    |       |
   EXTRACT_APPEND(36, 1);
   // location              | char(7)              | NO   |     | NULL    |       |
   EXTRACT_APPEND(37, 7);
   // base_location_suffix  | char(1)              | NO   |     | NULL    |       |
   EXTRACT_APPEND(44, 1);
   // assoc_location_suffix | char(1)              | NO   |     | NULL    |       |
   EXTRACT_APPEND(45, 1);
   // diagram_type          | char(1)              | NO   |     | NULL    |       |
   // N.B. Databse column misnamed, we store Association Type here.
   // Diagram Type always comes as 'T'.
   EXTRACT_APPEND(47, 1);
   // CIF_stp_indicator     | char(1)              | NO   |     | NULL    |       |
   EXTRACT_APPEND(79, 1);

   strcat(query, ")");

   if(db_query(query))
      return 1;

   stats[AssocCreate]++;

   return 0;
}

static word process_schedule(const char * const c)
{
   char query[1024];

   if(c[0] == 'B' && c[1] == 'S')
   {
      _log(DEBUG, "BS card \"%s\".", c);

      // If there's already a schedule stored, process it.
      if(merge_schedule()) return 1;
      
      if(c[2] == 'R' || c[2] == 'D')
      {
         // Delete a schedule
         if(process_schedule_delete(c)) return 1;
      }

      strcpy(CIF_BS, c);
      return 0;
   }

   if(CIF_schedule_next >= MAX_CIF_L)
   {
      _log(ABEND, "Run out of CIF_schedule storage.");
      exit(1);
   }
   strcpy(CIF_schedule[CIF_schedule_next++], c);
   
   if(c[0] == 'B' && c[1] == 'X')
   {
      strcpy(CIF_BX, c);
      return 0;
   }

   if(c[0] == 'L')
   {

      if(CIF_L_next >= MAX_CIF_L)
      {
         _log(ABEND, "Run out of CIF_L storage.");
         exit(1);
      }
      strcpy(CIF_L[CIF_L_next++], c);

      return 0;
   }

   if(c[0] == 'C' && c[1] == 'R')
   {
      return 0;
   }

   _log(MAJOR, "Unexpected card \"%s\".", c);
   return 1;
}

static word process_schedule_delete(const char * const c)
{
   _log(CRITICAL, "Unexpected delete or revise schedule card received:\n%s", c);

   return 1;
}

static word merge_schedule(void)
{
   MYSQL_RES * result[4];
   MYSQL_ROW row[4];
   char query[2048];
   int n, m;
   dword schedule_id;
   word match;
   
   // See if there's one to process.
   if(!CIF_BS[0]) return 0;

   stats[ScheduleExamined]++;

   if(CIF_BS_schedule_end_date() <= start_time)
   {
      // Schedule already ended, ignore it
      stats[ScheduleOld]++;
      // Clear out the card stores
      CIF_BS[0] = '\0';
      CIF_BX[0] = '\0';
      for(n = 0; n < MAX_CIF_L; n++) CIF_L[n][0] = '\0';
      CIF_L_next = CIF_schedule_next = 0;
      return 0;
   }

   sprintf(query, "SELECT id FROM cif_schedules WHERE CIF_train_uid = '%s' AND update_id > 0 AND deleted > %ld AND schedule_start_date = %ld AND CIF_stp_indicator = '%s' AND schedule_end_date > %ld", CIF_BS_CIF_train_uid(), start_time, CIF_BS_schedule_start_date(), CIF_BS_CIF_stp_indicator(), start_time);
   //_log(GENERAL, query);
   if(db_query(query)) return 1;
   result[1] = db_store_result();
   if(!result[1] || !(n = mysql_num_rows(result[1])))
   {
      _log(GENERAL, "Schedule %s(%s) in download is not in database.", CIF_BS_CIF_train_uid(), CIF_BS_CIF_stp_indicator());
      stats[ScheduleMissing]++;
      _log(GENERAL, "   %s", CIF_BS);
      //_log(GENERAL, "   End date %ld %s.", CIF_BS_schedule_end_date(), time_text(CIF_BS_schedule_end_date(), true));
      // Create missing entry
      if(opt_merge) create_schedule();
   }
   else
   {
      if(n > 1)
      {
         _log(GENERAL, "%s has %d matches in database.", CIF_BS_CIF_train_uid(), n);
         while((row[1] = mysql_fetch_row(result[1])))
         {
            _log(GENERAL, "   %s", row[1][0]);
         }
         stats[ScheduleMatchM]++;
      }
      else
      {
         // Apparently one match.  Examine further.
         match = true;
         row[1] = mysql_fetch_row(result[1]);
         schedule_id = atol(row[1][0]);
         _log(DEBUG, "Apparently one match:  Database %u.", schedule_id);
         sprintf(query, "SELECT COUNT(*) FROM cif_schedule_locations WHERE cif_schedule_id = %u", schedule_id);
         db_query(query);
         result[0] = db_store_result();
         row[0] = mysql_fetch_row(result[0]);
         n = atoi(row[0][0]);
         if(n != CIF_L_next)
         {
            match = false;
            _log(GENERAL, "Miss due to different location count, database %d, download %d.  Database schedule %u (%s).", n, CIF_L_next, schedule_id, CIF_BS_CIF_train_uid());
         }
         mysql_free_result(result[0]);
         for(n = 0; match && n < CIF_L_next; n++)
         {
            // For each CIF_L search for a cif_schedule_locations record, s/b exactly one.  If not, miss.
            sprintf(query, "SELECT COUNT(*) FROM cif_schedule_locations WHERE cif_schedule_id = %u AND record_identity = '%s' AND tiploc_code = '%s' AND tiploc_instance = '%s' AND arrival = '%s' AND departure = '%s' AND pass = '%s' AND platform = '%s' AND line = '%s' AND path = '%s'",
                    schedule_id, CIF_L_record_identity(n), CIF_L_tiploc_code(n), CIF_L_tiploc_instance(n), CIF_L_arrival(n), CIF_L_departure(n), CIF_L_pass(n), CIF_L_platform(n), CIF_L_line(n), CIF_L_path(n));
            db_query(query);
            result[0] = db_store_result();
            row[0] = mysql_fetch_row(result[0]);
            m = atoi(row[0][0]);
            if(m != 1)
            {
               match = false;
               _log(GENERAL, "Miss due to %d matching schedule locations.  Database schedule %u (%s).", m, schedule_id, CIF_BS_CIF_train_uid());
               _log(GENERAL, "   Downloaded: \"%s\".", CIF_L[n]);
               
            }
            mysql_free_result(result[0]);
         }
         
         if(match)
         {
            if(!unmatched[schedule_id - unmatched_base])
            {
               _log(GENERAL, "Database schedule %u (%s) matches download but is not in unmatched list.", schedule_id, CIF_BS_CIF_train_uid());
            }
            
            stats[ScheduleMatch1]++;
            unmatched[schedule_id - unmatched_base] = false;
         }
         else
         {
            // TODO log schedule_id to be deleted.  Leave it to the sweep-up at the end.
            // TODO do a create schedule for current card set.
            dump_schedule();
            //stats[]++;
         }
            
      }
   }
   
   mysql_free_result(result[1]);
   
   // Clear out the card stores
   CIF_BS[0] = '\0';
   CIF_BX[0] = '\0';
   for(n = 0; n < MAX_CIF_L; n++) CIF_L[n][0] = '\0';
   CIF_L_next = CIF_schedule_next = 0;
   
   return 0;
}

static word create_schedule(void)
{
   // Create a schedule in the database from the currently stored cards.
   static dword schedule_id;
   static char action;
   char query[1024], zs[128];
   word sort_arrive, sort_depart, sort_pass, sort_time, n; 
   static word origin_sort_time;
   char c[128];

   _log(PROC, "create_schedule(void)");
   
   // See if there's one to process.
   if(!CIF_BS[0]) return 0;

   // BS Card
   strcpy(c, CIF_BS);
   if(c[0] == 'B' && c[1] == 'S')
   {
      _log(DEBUG, "BS card \"%s\".", c);

      // Create a schedule
      sprintf(query, "INSERT INTO cif_schedules values(%d, %ld, %lu", update_id, start_time, NOT_DELETED);

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

   }

   for(n = 0; n < CIF_schedule_next; n++)
   {
      strcpy(c, CIF_schedule[n]);
   
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
         sprintf(zs, "' WHERE id = %d", schedule_id);
         strcat(query, zs);
         if(db_query(query)) return 1;
      }
      
      if(c[0] == 'L')
      {
         if(c[1] == 'O')
         {
            //| update_id             | smallint(5) unsigned | NO   |     | NULL    |       |
            //| cif_schedule_id       | int(10) unsigned     | NO   | MUL | NULL    |       |
            sprintf(query, "INSERT INTO cif_schedule_locations VALUES(%d, %d", update_id, schedule_id);
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
            sprintf(query, "INSERT INTO cif_schedule_locations VALUES(%d, %d", update_id, schedule_id);
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
            sprintf(query, "INSERT INTO cif_schedule_locations VALUES(%d, %d", update_id, schedule_id);
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
      }

      
      if(c[0] == 'C' && c[1] == 'R')
      {
         //| cif_schedule_id               | int(10) unsigned | NO   | MUL | NULL    |       |
         sprintf(query, "INSERT INTO cif_changes_en_route VALUES(%d", schedule_id);
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
      }
   }

   _log(GENERAL, "   Created schedule %ld.", schedule_id);
   
   // Clear cards
   CIF_BS[0] = '\0';
   CIF_BX[0] = '\0';
   for(n = 0; n < MAX_CIF_L; n++) CIF_L[n][0] = '\0';
   CIF_L_next = CIF_schedule_next = 0;
   return 0;
}
static word process_tiploc(const char * const c)
{
   char query[1024], zs[128], zs1[256];
   MYSQL_RES * result;
   word num_rows;

   switch(c[1])
   {
   case 'I': // TIPLOC Insert
      return create_tiploc(c);
      break;

   case 'A': // TIPLOC Amend
      // Delete existing one.
      strcpy(zs, extract_field_s(c, 2, 7));
      sprintf(query, "SELECT * from cif_tiplocs WHERE tiploc_code = '%s' AND deleted > %ld", zs, start_time);
      if(db_query(query)) return 1;
      result = db_store_result();
      num_rows = mysql_num_rows(result);
      mysql_free_result(result);

      if(num_rows)
      {
         if(num_rows > 1)
         {
            _log(MINOR, "TD card \"%s\".", c);
            _log(MINOR, "   Delete (As part of amend) TIPLOC found %d matches.  All deleted.", num_rows);
         }
         sprintf(query, "UPDATE cif_tiplocs SET deleted = %ld WHERE tiploc_code = '%s' AND deleted > %ld", start_time, zs, start_time);
         if(db_query(query)) return 1;
         stats[TIPLOCAmendHit]++;
      }
      else
      {
         _log(MINOR, "TD card \"%s\".", c);
         _log(MINOR, "   Delete (As part of amend) TIPLOC found no matches.");
         stats[TIPLOCAmendMiss]++;
      }

      strcpy(zs, c);
      strcpy(zs1, extract_field_s(c, 72, 7));
      if(zs1[0] != ' ')
      {
         // Amend, changing TIPLOC
         strncpy(zs + 2, zs1, 7);
      } 
      stats[TIPLOCCreate]--;
      return create_tiploc(zs);
      break;

   case 'D': // TIPLOC Delete
      strcpy(zs, extract_field_s(c, 2, 7));
      sprintf(query, "SELECT * from cif_tiplocs WHERE tiploc_code = '%s' AND deleted > %ld", zs, start_time);
      if(db_query(query)) return 1;
      result = db_store_result();
      num_rows = mysql_num_rows(result);
      mysql_free_result(result);

      if(num_rows)
      {
         if(num_rows > 1)
         {
            _log(MINOR, "TD card \"%s\".", c);
            _log(MINOR, "   Delete TIPLOC found %d matches.  All deleted.", num_rows);
         }
         sprintf(query, "UPDATE cif_tiplocs SET deleted = %ld WHERE tiploc_code = '%s' AND deleted > %ld", start_time, zs, start_time);
         if(db_query(query)) return 1;
         stats[TIPLOCDeleteHit]++;
      }
      else
      {
         _log(MINOR, "TD card \"%s\".", c);
         _log(MINOR, "   Delete TIPLOC found no matches.");
         stats[TIPLOCDeleteMiss]++;
      }
      break;

   default:
      _log(MAJOR, "Unexpected card \"%s\".", c);
      return 1;
   }

   return 0;
}


static word create_tiploc(const char * const c)
{
   char query[1024], zs[128], zs1[256];

   //"update_id                     SMALLINT UNSIGNED NOT NULL,  "
   //"created                       INT UNSIGNED NOT NULL, "
   //"deleted                       INT UNSIGNED NOT NULL, "
   sprintf(query, "INSERT INTO cif_tiplocs values(%d, %ld, %lu", update_id, start_time, NOT_DELETED);

   //"tiploc_code                   CHAR(7) NOT NULL, "
   EXTRACT_APPEND(2, 7);
   //"capitals                      CHAR(2) NOT NULL, "
   EXTRACT_APPEND(9, 2);
   //"nalco                         CHAR(6) NOT NULL, "
   EXTRACT_APPEND(11, 6);
   //"nlc_check                     CHAR(1) NOT NULL, "
   EXTRACT_APPEND(17, 1);
   //"tps_description               CHAR(26) NOT NULL, "
   EXTRACT_APPEND_ESCAPE(18, 26);
   //"stanox                        CHAR(5) NOT NULL, "
   EXTRACT_APPEND(44, 5);
   // PO MCP Code not used
   //"CRS                           CHAR(3) NOT NULL, "
   EXTRACT_APPEND_ESCAPE(53, 3);
   //"CAPRI_description             CHAR(16) NOT NULL, "
   EXTRACT_APPEND(56, 16);
   strcat(query, ")");
   if(db_query(query)) return 1;
   stats[TIPLOCCreate]++;
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

static void extract_field(const char * const c, const size_t s, const size_t l, char * const d)
{
   // Assumes d is big enough.

   size_t i;
   size_t ss = (s>80)?80:s;

   for(i = 0; i < l && i < 80; i++) d[i] = c[ss + i];
   d[i] = '\0';
} 

static char * extract_field_s(const char * const c, const size_t s, size_t l)
{
   static char result[128];
   extract_field(c, s, l, result);

   return result;
}
static const char * const CIF_BS_CIF_train_uid(void)
{
   static char result[8];
   extract_field(CIF_BS,  3, 6, result);
   return result;
}
static const time_t CIF_BS_schedule_start_date(void)
{
   static char result[8];
   extract_field(CIF_BS,  9, 6, result);
   return parse_CIF_datestamp(result);
}
static const time_t CIF_BS_schedule_end_date(void)
{
   static char result[8];
   extract_field(CIF_BS, 15, 6, result);
   return parse_CIF_datestamp(result);
}
static const char * const CIF_BS_CIF_stp_indicator(void)
{
   static char result[8];
   extract_field(CIF_BS,  79, 1, result);
   return result;
}

static const char * const CIF_L_record_identity(const word i)
{
   static char result[8];
   extract_field(CIF_L[i], 0, 2, result);
   return result;
}

static const char * const CIF_L_tiploc_code(const word i)
{
   static char result[8];
   extract_field(CIF_L[i], 2, 7, result);
   return result;
}

static const char * const CIF_L_tiploc_instance(const word i)
{
   static char result[8];
   extract_field(CIF_L[i], 9, 1, result);
   return result;
}

static const char * const CIF_L_arrival(const word i)
{
   static char result[8];
   switch(CIF_L[i][1])
   {
   case 'I': extract_field(CIF_L[i], 10, 5, result); break;
   case 'T': extract_field(CIF_L[i], 10, 5, result); break;
   default:  result[0] = '\0'; break;
   }
   return result;
}

static const char * const CIF_L_departure(const word i)
{
   static char result[8];
   switch(CIF_L[i][1])
   {
   case 'O': extract_field(CIF_L[i], 10, 5, result); break;
   case 'I': extract_field(CIF_L[i], 15, 5, result); break;
   default:  result[0] = '\0'; break;
   }
   return result;
}

static const char * const CIF_L_pass(const word i)
{
   static char result[8];
   switch(CIF_L[i][1])
   {
   case 'I': extract_field(CIF_L[i], 20, 5, result); break;
   default:  result[0] = '\0'; break;
   }
   return result;
}

static const char * const CIF_L_platform(const word i)
{
   static char result[8];
   switch(CIF_L[i][1])
   {
   case 'O': extract_field(CIF_L[i], 19, 3, result); break;
   case 'I': extract_field(CIF_L[i], 33, 3, result); break;
   case 'T': extract_field(CIF_L[i], 19, 3, result); break;
   default:  result[0] = '\0'; break;
   }
   return result;
}

static const char * const CIF_L_line(const word i)
{
   static char result[8];
   switch(CIF_L[i][1])
   {
   case 'O': extract_field(CIF_L[i], 22, 3, result); break;
   case 'I': extract_field(CIF_L[i], 36, 3, result); break;
   default:  result[0] = '\0'; break;
   }
   return result;
}

static const char * const CIF_L_path(const word i)
{
   static char result[8];
   switch(CIF_L[i][1])
   {
   case 'I': extract_field(CIF_L[i], 39, 3, result); break;
   case 'T': extract_field(CIF_L[i], 22, 3, result); break;
   default:  result[0] = '\0'; break;
   }
   return result;
}

static time_t parse_CIF_datestamp(const char * const s)
{
   // Only works for yymmdd
   char zs[128];

   if(strcmp(s, "999999"))
   {
      sprintf(zs, "20%c%c-%c%c-%c%c", s[0],s[1],s[2],s[3],s[4],s[5]);
      return parse_datestamp(zs);
   }
   
   return NOT_DELETED;
}

static void dump_schedule(void)
{
   char BS_mod[128];
   int i;
   strcpy(BS_mod, CIF_BS);
   BS_mod[2] = 'R';
   fprintf(fp_out, "%s\n", BS_mod);
   
   for(i=0; i < CIF_schedule_next; i++)
   {
      fprintf(fp_out, "%s\n", CIF_schedule[i]);
   }
}
