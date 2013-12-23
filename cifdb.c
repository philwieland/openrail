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
#include <regex.h>
#include <time.h>
#include <mysql.h>
#include <unistd.h>
#include <sys/resource.h>
#include <curl/curl.h>

#include "misc.h"
#include "jsmn.h"
#include "db.h"
#include "private.h"

#define NAME  "cifdb"
#define BUILD "UC09"

static void process_object(const char * object);
static void process_timetable(const char * string, const jsmntok_t * tokens);
static void process_association(const char * string, const jsmntok_t * tokens);
static void process_delete_association(const char * string, const jsmntok_t * tokens);
static void process_create_association(const char * string, const jsmntok_t * tokens);
static void process_schedule(const char * string, const jsmntok_t * tokens);
static void process_delete_schedule(const char * string, const jsmntok_t * tokens);
static void process_create_schedule(const char * string, const jsmntok_t * tokens);
static word process_schedule_location(const char * string, const jsmntok_t * tokens, const int index, const unsigned long id);
static void process_tiploc(const char * string, const jsmntok_t * tokens);
static word get_sort_time(const char const * buffer);
static void reset_database(void);
static void jsmn_dump_tokens(const char * string, const jsmntok_t * tokens, const word object_index);
static word fetch_file(void);
static size_t cif_write_data(void *buffer, size_t size, size_t nmemb, void *userp);
void extract_match(const char * source, const regmatch_t * matches, const unsigned int match, char * result, const size_t max_length);

static word debug, reset_db, fetch_all, run, tiploc_ignored, test_mode;
static char * opt_filename;
static char * opt_url;
static dword update_id;
static time_t start_time, last_reported_time;
#define INVALID_SORT_TIME 9999

#define NOT_DELETED 0xffffffffL

// Stats
static dword count_schedule_create, count_schedule_delete_hit, count_schedule_delete_miss;
static dword count_schedule_loc_create, count_schedule_loc_delete;
static dword count_assoc_create, count_assoc_delete_hit, count_assoc_delete_miss;
static dword count_fetches;

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

#define MATCHES 1
static regex_t match[MATCHES];
static char* match_strings[MATCHES] =
   {
      "\"timestamp\":([[:digit:]]{1,12})",  // 0
   };

// Blank database

static const char const * create_table_updates_processed =
"CREATE TABLE updates_processed"
"("
"id                            SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT, "
"time                          INT UNSIGNED NOT NULL,"
"PRIMARY KEY (id) "
") ENGINE = InnoDB";

static const char const * create_table_cif_associations =
"CREATE TABLE cif_associations                   "
"(                                               "
"update_id               SMALLINT UNSIGNED NOT NULL,        "
"created                 INT UNSIGNED NOT NULL,       "
"deleted                 INT UNSIGNED NOT NULL,       "
"main_train_uid          CHAR(6) NOT NULL,       "
"assoc_train_uid         CHAR(6) NOT NULL,       "
"assoc_start_date        INT UNSIGNED NOT NULL,       "
"assoc_end_date          INT UNSIGNED NOT NULL,       "
"assoc_days              CHAR(7) NOT NULL,       "
"category                CHAR(2) NOT NULL,       "
"date_indicator          CHAR(1) NOT NULL,       "
"location                CHAR(7) NOT NULL,       "
"base_location_suffix    CHAR(1) NOT NULL,       "
"assoc_location_suffix   CHAR(1) NOT NULL,       "
"diagram_type            CHAR(1) NOT NULL,       "
"CIF_stp_indicator       CHAR(1) NOT NULL        "
") ENGINE = InnoDB";


static const char const * create_table_cif_schedules =
"CREATE TABLE cif_schedules                      "
"(                                               "
"update_id                     SMALLINT UNSIGNED NOT NULL,  "
"created                       INT UNSIGNED NOT NULL, "
"deleted                       INT UNSIGNED NOT NULL, "
"CIF_bank_holiday_running      CHAR(1) NOT NULL, "
"CIF_stp_indicator             CHAR(1) NOT NULL, "
"CIF_train_uid                 CHAR(6) NOT NULL, "
"applicable_timetable          char(1) NOT NULL, "
"atoc_code                     CHAR(2) NOT NULL, "
   //"traction_class                CHAR(8) NOT NULL, " // "Not used" in CIF spec so omitted here.
"uic_code                      CHAR(5) NOT NULL, "
"runs_mo                       BOOLEAN NOT NULL, "
"runs_tu                       BOOLEAN NOT NULL, "
"runs_we                       BOOLEAN NOT NULL, "
"runs_th                       BOOLEAN NOT NULL, "
"runs_fr                       BOOLEAN NOT NULL, "
"runs_sa                       BOOLEAN NOT NULL, "
"runs_su                       BOOLEAN NOT NULL, "
"schedule_end_date             INT UNSIGNED NOT NULL, "
"signalling_id                 CHAR(4) NOT NULL, "
"CIF_train_category            CHAR(2) NOT NULL, "
"CIF_headcode                  CHAR(4) NOT NULL, "
   //"CIF_course_indicator          CHAR(8) NOT NULL, " // "Not used" in CIF spec, so omitted here.
"CIF_train_service_code        CHAR(8) NOT NULL, "
"CIF_business_sector           CHAR(1) NOT NULL, "
"CIF_power_type                CHAR(3) NOT NULL, "
"CIF_timing_load               CHAR(4) NOT NULL, "
"CIF_speed                     CHAR(3) NOT NULL, "
"CIF_operating_characteristics CHAR(6) NOT NULL, "
"CIF_train_class               CHAR(1) NOT NULL, "
"CIF_sleepers                  CHAR(1) NOT NULL, "
"CIF_reservations              CHAR(1) NOT NULL, "
"CIF_connection_indicator      CHAR(1) NOT NULL, "
"CIF_catering_code             CHAR(4) NOT NULL, "
"CIF_service_branding          CHAR(4) NOT NULL, "
"schedule_start_date           INT UNSIGNED NOT NULL, "
"train_status                  CHAR(1) NOT NULL, "
"id                            INT UNSIGNED NOT NULL AUTO_INCREMENT, "
"PRIMARY KEY (id), INDEX(schedule_end_date), INDEX(schedule_start_date), INDEX(CIF_train_uid), INDEX(CIF_stp_indicator) "
") ENGINE = InnoDB";

static const char * const create_table_cif_schedule_locations = 
"CREATE TABLE cif_schedule_locations             "
"(                                               "
"update_id                     SMALLINT UNSIGNED NOT NULL, "
"cif_schedule_id               INT UNSIGNED NOT NULL, "
"location_type                 CHAR(12) NOT NULL, " // Expected to become activity ?????
"record_identity               CHAR(2) NOT NULL, " // LO, LI, LT, etc
"tiploc_code                   CHAR(7) NOT NULL, "
"tiploc_instance               CHAR(1) NOT NULL, "
"arrival                       CHAR(5) NOT NULL, "
"departure                     CHAR(5) NOT NULL, "
"pass                          CHAR(5) NOT NULL, "
"public_arrival                CHAR(4) NOT NULL, "
"public_departure              CHAR(4) NOT NULL, "
"sort_time                     SMALLINT UNSIGNED NOT NULL, "
"next_day                      BOOLEAN NOT NULL, "
"platform                      CHAR(3) NOT NULL, "
"line                          CHAR(3) NOT NULL, "
"path                          CHAR(3) NOT NULL, "
"engineering_allowance         CHAR(2) NOT NULL, "
"pathing_allowance             CHAR(2) NOT NULL, "
"performance_allowance         CHAR(2) NOT NULL, "
"id                            INT UNSIGNED NOT NULL AUTO_INCREMENT, "
"PRIMARY KEY (id), INDEX(cif_schedule_id), INDEX(tiploc_code) "
") ENGINE = InnoDB";

int main(int argc, char **argv)
{
   char zs[1024];

   start_time = time(NULL);
   last_reported_time = start_time;

   // Determine debug mode
   if(geteuid() == 0)
   {
      debug = 0;
   }
   else
   {
      debug = 1;
   }

   _log_init(debug?"/tmp/cifdb.log":"/var/log/garner/cifdb.log", debug?1:0);

   _log(GENERAL, "");
   _log(GENERAL, "%s %s", NAME, BUILD);

   if(debug)
   {
      _log(GENERAL, "Debug mode selected.  Using TEST database.");
      _log(GENERAL, "To use live database, run as root.");
   }

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
   db_init(DB_SERVER, DB_USER, DB_PASSWORD, debug?"rail_test":"rail");

   reset_db = false;
   opt_filename = NULL;
   opt_url = NULL;
   fetch_all = false;
   test_mode = false;
   word usage = false;
   int c;
   while ((c = getopt (argc, argv, "u:f:atrh")) != -1)
      switch (c)
      {
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
      case 'r':
         reset_db = true;
         break;
      case 'h':
         usage = true;
         break;
      case '?':
      default:
         usage = true;
         break;
      }

   if(usage) 
   {
      printf(
"Usage:\n"
"Data source:\n"
"default    Fetch latest update.\n"
"-u <url>   Fetch from specified URL.\n"
"-f <file>  Use specified file.  (Must already be decompressed.)\n"
"-a         Fetch latest full timetable.\n"
"Actions:\n"
"default    Apply data to database.\n"
"-t         Report datestamp on file, do not apply to database.\n"
"-r         Reset database.  Do not process any data.\n"
             );
      exit(0);
   }

   if(reset_db) 
   {
      reset_database();
      exit(0);
   }

   run = 1;
   tiploc_ignored = false;
   count_schedule_create      = 0;
   count_schedule_delete_hit  = 0;
   count_schedule_delete_miss = 0;
   count_schedule_loc_create  = 0;
   count_schedule_loc_delete  = 0;
   count_assoc_create         = 0;
   count_assoc_delete_hit     = 0;
   count_assoc_delete_miss    = 0;
   count_fetches              = 0;

   struct tm * broken = localtime(&start_time);
   word date = broken->tm_mday;
   while(broken->tm_mday == date && fetch_file())
   {
      if(opt_url || opt_filename)
      {
         _log(GENERAL, "Failed to find data.");
         exit(1);
      }
      {
         char report[256];
         sprintf(report, "Attempt %ld failed to fetch file.  Pausing before retry...", count_fetches);
         _log(GENERAL, report);
         if(count_fetches == 4)
         {
            sprintf(report, "Failed to collect timetable update after %ld attempts.\n\nContinuing to retry.", count_fetches);
            email_alert(NAME, BUILD, "Timetable Update Failure Report", report);
         }
      }
      sleep(32*60);
      time_t now = time(NULL);
      broken = localtime(&now);
   }

   char in_q = 0;
   char b_depth = 0;

   size_t ibuf = 0;
   size_t iobj = 0;
   size_t buf_end;

   if(broken->tm_mday != date)
   {
      _log(CRITICAL, "Abandoned file fetch.");
   }
   else if(test_mode)
   {
   }
   else
   {
      char c, pc;
      pc = 0;
      // Run through the file splitting off each JSON object and passing it on for processing.
      while((buf_end = fread(buffer, 1, MAX_BUF, fp_result)) && run)
      {
         // Comfort report
         time_t now = time(NULL);
         if(now - last_reported_time > 10*60)
         {
            char zs[128], zs1[128];
            sprintf(zs, "Progress: Created %s associations, ", commas(count_assoc_create));
            sprintf(zs1, "%s schedules and ", commas(count_schedule_create));
            strcat(zs, zs1);
            sprintf(zs1, "%s schedule locations.  Working...", commas(count_schedule_loc_create));
            strcat(zs, zs1);
            _log(GENERAL, zs);
            last_reported_time += 10*60;
         }
         
         for(ibuf = 0; ibuf < buf_end && run; ibuf++)
         {
            c = buffer[ibuf];
            if(c != '\r' && c != '\n') 
            {
               obj[iobj++] = c;
               if(iobj >= MAX_OBJ)
               {
                  sprintf(zs, "Object buffer overflow!");
                  _log(CRITICAL, zs);
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
   }

   db_disconnect();

   char report[8192];
   report[0] = '\0';

   _log(GENERAL, "");
   _log(GENERAL, "End of run:");
   sprintf(zs, "Elapsed time             : %ld minutes", (time(NULL) - start_time + 30) / 60);
   _log(GENERAL, zs);
   strcat(report, zs); strcat(report, "\n");

   sprintf(zs, "File download attempts   : %s", commas(count_fetches));
   _log(GENERAL, zs);
   strcat(report, zs); strcat(report, "\n");

   sprintf(zs, "Schedule create          : %s", commas(count_schedule_create));
   _log(GENERAL, zs);
   strcat(report, zs); strcat(report, "\n");

   sprintf(zs, "Schedule delete hit      : %s", commas(count_schedule_delete_hit));
   _log(GENERAL, zs);
   strcat(report, zs); strcat(report, "\n");

   sprintf(zs, "Schedule delete miss     : %s", commas(count_schedule_delete_miss));
   _log(GENERAL, zs);
   strcat(report, zs); strcat(report, "\n");

   sprintf(zs, "Schedule location create : %s", commas(count_schedule_loc_create));
   _log(GENERAL, zs);
   strcat(report, zs); strcat(report, "\n");

   sprintf(zs, "Schedule location delete : %s", commas(count_schedule_loc_delete));
   _log(GENERAL, zs);
   strcat(report, zs); strcat(report, "\n");

   sprintf(zs, "Association create       : %s", commas(count_assoc_create));
   _log(GENERAL, zs);
   strcat(report, zs); strcat(report, "\n");

   sprintf(zs, "Association delete hit   : %s", commas(count_assoc_delete_hit));
   _log(GENERAL, zs);
   strcat(report, zs); strcat(report, "\n");

   sprintf(zs, "Association delete miss  : %s", commas(count_assoc_delete_miss));
   _log(GENERAL, zs);
   strcat(report, zs); strcat(report, "\n");

#if 0
   sprintf(zs, "Schedule purged          : %s", commas(count_purge_sched));
   _log(GENERAL, zs);
   strcat(report, zs); strcat(report, "\n");

   sprintf(zs, "Schedule location purged : %s", commas(count_purge_sched_loc));
   _log(GENERAL, zs);
   strcat(report, zs); strcat(report, "\n");

   sprintf(zs, "Association purged       : %s", commas(count_purge_assoc));
   _log(GENERAL, zs);
   strcat(report, zs); strcat(report, "\n");
#endif

   email_alert(NAME, BUILD, "Timetable Update Report", report);

#if 0
   if(!test_mode)
   {
      char filename[256];
      time_t now = time(NULL);
      struct tm * broken = gmtime(&now);
      
      sprintf(filename, "/tmp/%s-database-%04d-%02d-%02d-%02d-%02d-%02dZ.sql", (debug?"rail_test":"rail"), 
              broken->tm_year + 1900,
              broken->tm_mon + 1, 
              broken->tm_mday,
              broken->tm_hour,
              broken->tm_min,
              broken->tm_sec
              );
      sprintf(zs, "Dumping copy of database to \"%s\"", filename);
      _log(GENERAL, zs);
      sprintf(zs, "/usr/bin/mysqldump --single-transaction --quick -u garnerd -pRA31GAkvpn %s > %s", (debug?"rail_test":"rail"), filename);
      if(system(zs))
      {
      }
      _log(GENERAL, "Dump completed.");
   }
#endif

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
      jsmn_extract_token(object_string, tokens, 1, message_name, sizeof(message_name));
      if(!strcmp(message_name, "JsonTimetableV1"))        process_timetable(object_string, tokens);
      else if(!strcmp(message_name, "JsonAssociationV1")) process_association(object_string, tokens);
      else if(!strcmp(message_name, "JsonScheduleV1"))    process_schedule(object_string, tokens);
      else if(!strcmp(message_name, "TiplocV1"))          process_tiploc(object_string, tokens);
      else if(!strcmp(message_name, "EOF"))               ;
      else
      {
         sprintf(zs, "Unrecognised message name \"%s\".", message_name);
         _log(MINOR, zs);
         jsmn_dump_tokens(object_string, tokens, 0);
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
      strcat(zs, time_text(stamp, 1));

      _log(GENERAL, zs);

      // Check if we've already seen it
      {
         sprintf(zs, "SELECT count(*) from updates_processed where time = %ld", stamp);
         MYSQL_RES * result;
         MYSQL_ROW row;
         if(db_connect()) return;
         
         if (db_query(zs))
         {
            db_disconnect();
            return;
         }

         result = db_store_result();
         row = mysql_fetch_row(result);

         int rows = atoi(row[0]);

         mysql_free_result(result);

         if(rows > 0)
         {
            _log(CRITICAL, "A file with this timestamp has already been processed.  Processing aborted.");
            run = 0;
         }
         else
         {
            sprintf(zs, "INSERT INTO updates_processed VALUES(0, %ld)", stamp);
            db_query(zs);
            update_id = db_insert_id();
            sprintf(zs, "Update_id = %ld", update_id);
            _log(GENERAL, zs);
         }
      }
   }
   else
   {
      strcat(zs, "Timestamp not found.  Processing aborted.");
      _log(CRITICAL, zs);
      run = 0;
   }
}

static void process_association(const char * string, const jsmntok_t * tokens)
{
   char zs[128], zs1[128];

   jsmn_find_extract_token(string, tokens, 0, "transaction_type", zs, sizeof(zs));
   if(zs[0])
   {
      //      printf("   Transaction type: \"%s\"", zs);
      if(!strcasecmp(zs, "Delete")) process_delete_association(string, tokens);
      else if(!strcasecmp(zs, "Create")) process_create_association(string, tokens);
      else 
      {
         sprintf(zs1, "process_association():  Unrecognised transaction type \"%s\".", zs);
         _log(MAJOR, zs1);
      }
   }
   else
   {
      _log(MAJOR, "process_association():  Failed to determine transaction type.");
   }
}

#define EXTRACT(a,b) jsmn_find_extract_token(string, tokens, 0, a, b, sizeof( b ))

static void process_delete_association(const char * string, const jsmntok_t * tokens)
{
   char main_train_uid[32], assoc_train_uid[32], assoc_start_date[32], location[32], suffix[32], diagram_type[32], cif_stp_indicator[32];

   EXTRACT("main_train_uid", main_train_uid);
   EXTRACT("assoc_train_uid", assoc_train_uid);
   EXTRACT("assoc_start_date", assoc_start_date);
   EXTRACT("location", location);
   EXTRACT("base_location_suffix", suffix);
   EXTRACT("diagram_type", diagram_type);
   EXTRACT("cif_stp_indicator", cif_stp_indicator);

   time_t start_stamp = parse_timestamp(assoc_start_date);
   
   char query[1024];

   sprintf(query, "SELECT * FROM cif_associations WHERE main_train_uid = '%s' AND assoc_train_uid = '%s' AND assoc_start_date = %ld AND location = '%s' AND base_location_suffix = '%s' AND diagram_type = '%s' AND deleted > %ld",
           main_train_uid, assoc_train_uid, start_stamp, location, suffix, diagram_type, time(NULL));

   if(cif_stp_indicator[0])
   {
      char zs[128];
      sprintf(zs, " AND cif_stp_indicator = '%s'", cif_stp_indicator);
      strcat(query, zs);
   }
   else
   {
      _log(MINOR, "Processing delete association transaction with empty cif_stp_indicator field.");
   }

   if(!db_query(query))
   {
      MYSQL_RES * result = db_store_result();

      word found = mysql_num_rows(result);
      if(found) 
      {
         count_assoc_delete_hit++;
         if(found > 1)
         {
            char zs[256];
            sprintf(zs, "Delete association found %d matches.", found);
            _log(MAJOR, zs);
            jsmn_dump_tokens(string, tokens, 0);
            dump_mysql_result(result);
         }

         char query2[1024];
         sprintf(query2, "UPDATE cif_associations SET deleted = %ld %s", time(NULL), query + 31);
         (void) db_query(query2);
      }
      else
      {
         count_assoc_delete_miss++;
         _log(MAJOR, "Delete association miss.");
         jsmn_dump_tokens(string, tokens, 0);
      }
   }
}

static void process_create_association(const char * string, const jsmntok_t * tokens)
{

   char main_train_uid[32], assoc_train_uid[32], start_date[32], end_date[32], assoc_days[32], category[32], date_indicator[32], location[32], base_location_suffix[32], assoc_location_suffix[32], diagram_type[32], cif_stp_indicator[32];

   EXTRACT("main_train_uid", main_train_uid);
   EXTRACT("assoc_train_uid", assoc_train_uid);
   EXTRACT("assoc_start_date", start_date);
   EXTRACT("assoc_end_date", end_date);
   EXTRACT("assoc_days", assoc_days);
   EXTRACT("category", category);
   EXTRACT("date_indicator", date_indicator);
   EXTRACT("location", location);
   EXTRACT("base_location_suffix", base_location_suffix);
   EXTRACT("assoc_location_suffix", assoc_location_suffix);
   EXTRACT("diagram_type", diagram_type);
   EXTRACT("cif_stp_indicator", cif_stp_indicator);

   time_t start_stamp = parse_timestamp(start_date);
   time_t end_stamp   = parse_timestamp(end_date  );
   time_t now = time(NULL);
   char query[1024];
   sprintf(query, "INSERT INTO cif_associations VALUES(%ld, %ld, %lu, '%s', '%s', %ld, %ld, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')",
           update_id, now, NOT_DELETED, main_train_uid, assoc_train_uid, start_stamp, end_stamp, assoc_days,category, date_indicator, location, base_location_suffix, assoc_location_suffix, diagram_type, cif_stp_indicator);

   (void) db_query(query); 
   count_assoc_create++;
}

static void process_schedule(const char * string, const jsmntok_t * tokens)
{
   char zs[128], zs1[1024];

   jsmn_find_extract_token(string, tokens, 0, "transaction_type", zs, sizeof(zs));
   if(zs[0])
   {
      // printf("   Transaction type: \"%s\"", zs);
      if(!strcasecmp(zs, "Delete"))      process_delete_schedule(string, tokens);
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

static void process_delete_schedule(const char * string, const jsmntok_t * tokens)
{
   char query[1024], CIF_train_uid[16], schedule_start_date[16], CIF_stp_indicator[8]; 
   dword id;
   MYSQL_RES * result0;
   MYSQL_ROW row0;

   word deleted = 0;

   EXTRACT("CIF_train_uid", CIF_train_uid);
   EXTRACT("schedule_start_date", schedule_start_date);
   EXTRACT("CIF_stp_indicator", CIF_stp_indicator);

   time_t schedule_start_date_stamp = parse_datestamp(schedule_start_date);

   // Find the id
   sprintf(query, "SELECT id FROM cif_schedules WHERE update_id != 0 AND CIF_train_uid = '%s' AND schedule_start_date = '%ld' AND CIF_stp_indicator = '%s' AND deleted > %ld",
           CIF_train_uid, schedule_start_date_stamp, CIF_stp_indicator, time(NULL));
   if(db_connect()) return;
   
   if (db_query(query))
   {
      db_disconnect();
      return;
   }

   result0 = db_store_result();
   word num_rows = mysql_num_rows(result0);

   if(num_rows > 1)
   {
      char zs[256];
      sprintf(zs, "Delete schedule found %d matches.", num_rows);
      _log(MAJOR, zs);
      jsmn_dump_tokens(string, tokens, 0);

      // Bodge!
      query[7] = '*'; query[8] = ' ';
      dump_mysql_result_query(query);
   }
 
   while((row0 = mysql_fetch_row(result0)) && row0[0]) 
   {
      id = atol(row0[0]);
      sprintf(query, "UPDATE cif_schedules SET deleted = %ld where id = %ld", time(NULL), id);
   
      if(!db_query(query))
      {
         deleted++;
      }
   }
   mysql_free_result(result0);

   if(deleted) 
   {
      count_schedule_delete_hit += deleted;
   }
   else
   {
      count_schedule_delete_miss++;
      _log(MAJOR, "Delete schedule miss.");
      jsmn_dump_tokens(string, tokens, 0);         
   }
}

static void process_create_schedule(const char * string, const jsmntok_t * tokens)
{
   char zs[128], zs1[128];
   char query[2048];
   word i;

   time_t now = time(NULL);

   sprintf(query, "INSERT INTO cif_schedules VALUES(%ld, %ld, %lu",
           update_id, now, NOT_DELETED);

   EXTRACT_APPEND_SQL("CIF_bank_holiday_running");
   EXTRACT_APPEND_SQL("CIF_stp_indicator");
   EXTRACT_APPEND_SQL("CIF_train_uid");

   EXTRACT_APPEND_SQL("applicable_timetable");

   EXTRACT_APPEND_SQL("atoc_code");
   // EXTRACT_APPEND_SQL("traction_class");
   EXTRACT_APPEND_SQL("uic_code");
   EXTRACT("schedule_days_runs", zs);
   for(i=0; i<7; i++)
   {
      strcat(query, ", ");
      strcat(query, (zs[i]=='1')?"1":"0");
   }

   EXTRACT("schedule_end_date", zs);
   time_t z = parse_datestamp(zs);
   sprintf(zs1, ", %ld", z);
   strcat(query, zs1);

   EXTRACT_APPEND_SQL("signalling_id");
   EXTRACT_APPEND_SQL("CIF_train_category");
   EXTRACT_APPEND_SQL("CIF_headcode");
   //EXTRACT_APPEND_SQL("CIF_course_indicator");
   EXTRACT_APPEND_SQL("CIF_train_service_code");
   EXTRACT_APPEND_SQL("CIF_business_sector");
   EXTRACT_APPEND_SQL("CIF_power_type");
   EXTRACT_APPEND_SQL("CIF_timing_load");
   EXTRACT_APPEND_SQL("CIF_speed");
   EXTRACT_APPEND_SQL("CIF_operating_characteristics");
   EXTRACT_APPEND_SQL("CIF_train_class");

   EXTRACT_APPEND_SQL("CIF_sleepers");

   EXTRACT_APPEND_SQL("CIF_reservations");
   EXTRACT_APPEND_SQL("CIF_connection_indicator");
   EXTRACT_APPEND_SQL("CIF_catering_code");
   EXTRACT_APPEND_SQL("CIF_service_branding");
   
   EXTRACT("schedule_start_date", zs);
   z = parse_datestamp(zs);
   sprintf(zs1, ", %ld", z);
   strcat(query, zs1);

   EXTRACT_APPEND_SQL("train_status");

   strcat(query, ", 0)");

   (void) db_query(query); 
      
   count_schedule_create++;

   dword id = db_insert_id();

   word index = jsmn_find_name_token(string, tokens, 0, "schedule_location");
   word locations = tokens[index+1].size;

   index += 2;
   for(i = 0; i < locations; i++)
   {
      index = process_schedule_location(string, tokens, index, id);
   }
}

#define EXTRACT_APPEND_SQL_OBJECT(a) { jsmn_find_extract_token(string, tokens, index, a, zs, sizeof( zs )); sprintf(zs1, ", \"%s\"", zs); strcat(query, zs1); }

static word process_schedule_location(const char * string, const jsmntok_t * tokens, const int index, const unsigned long schedule_id)
{
   char query[2048], zs[1024], zs1[1024];

   word sort_arrive, sort_depart, sort_pass, sort_time;
   static word origin_sort_time;

   byte type_LO;

   sprintf(zs, "process_schedule_location(%d, %ld)", index, schedule_id);
   _log(PROC, zs);

   sprintf(query, "INSERT INTO cif_schedule_locations VALUES(%ld, %ld",
           update_id, schedule_id);

   EXTRACT_APPEND_SQL_OBJECT("location_type");
   EXTRACT_APPEND_SQL_OBJECT("record_identity");
   type_LO = (zs[0] == 'L' && zs[1] == 'O');
   EXTRACT_APPEND_SQL_OBJECT("tiploc_code");
   EXTRACT_APPEND_SQL_OBJECT("tiploc_instance");
   EXTRACT_APPEND_SQL_OBJECT("arrival");
   sort_arrive = get_sort_time(zs);
   EXTRACT_APPEND_SQL_OBJECT("departure");
   sort_depart = get_sort_time(zs) + 1;
   EXTRACT_APPEND_SQL_OBJECT("pass");
   sort_pass = get_sort_time(zs) + 1;
   EXTRACT_APPEND_SQL_OBJECT("public_arrival");
   EXTRACT_APPEND_SQL_OBJECT("public_departure");

   // Evaluate the sort_time and next_day fields
   if(sort_arrive < INVALID_SORT_TIME) sort_time = sort_arrive;
   else if(sort_depart < INVALID_SORT_TIME) sort_time = sort_depart;
   else sort_time = sort_pass;
   if(type_LO) origin_sort_time = sort_time;
   // N.B. Calculation of next_day field assumes that the LO record will be processed before the others.  Can we assume this?
   sprintf(zs, ", %d, %d", sort_time, (sort_time < origin_sort_time)?1:0);
   strcat(query, zs);
   EXTRACT_APPEND_SQL_OBJECT("platform");
   EXTRACT_APPEND_SQL_OBJECT("line");
   EXTRACT_APPEND_SQL_OBJECT("path");
   EXTRACT_APPEND_SQL_OBJECT("engineering_allowance");
   EXTRACT_APPEND_SQL_OBJECT("pathing_allowance");
   EXTRACT_APPEND_SQL_OBJECT("performance_allowance");
   strcat(query, ", 0)");

   (void) db_query(query); 
      
   count_schedule_loc_create++;

   return (index + tokens[index].size + 1);
}

static void process_tiploc(const char * string, const jsmntok_t * tokens)
{
   if(!tiploc_ignored)
   {
      _log(MINOR, "TiplocV1 record(s) ignored.");
      tiploc_ignored = true;
   }
}

static word get_sort_time(const char const * buffer)
{
   word result;
   char zs[8];
   if(!buffer[0]) return INVALID_SORT_TIME;

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
   _log(GENERAL, "Resetting CIF database.");

   db_query("DROP TABLE IF EXISTS updates_processed");
   db_query(create_table_updates_processed);
   db_query("DROP TABLE IF EXISTS cif_associations");
   db_query(create_table_cif_associations);
   db_query("DROP TABLE IF EXISTS cif_schedules");
   db_query(create_table_cif_schedules);
   db_query("DROP TABLE IF EXISTS cif_schedule_locations");
   db_query(create_table_cif_schedule_locations);

   _log(GENERAL, "CIF database reset.");
   db_disconnect();
}

#if 0
static void purge_database(void)
{
   char query[256];
   MYSQL_RES * result0;
   MYSQL_ROW row0;
   dword id;
   time_t threshold = time(NULL) - 64*24*60*60;

   _log(GENERAL, "Comencing database purge.");
   sprintf(query, "DELETE FROM cif_associations WHERE assoc_end_date < %ld", threshold);
   if(!db_query(query))
   {
      count_purge_assoc += db_row_count();
   }

   sprintf(query, "SELECT id FROM cif_schedules WHERE schedule_end_date < %ld", threshold);
   if(!db_query(query))
   {
      result0 = db_store_result();
      while((row0 = mysql_fetch_row(result0)))
      {
         id = atol(row0[0]);
         sprintf(query, "DELETE FROM cif_schedule_locations WHERE cif_schedule_id = %ld", id);
         if(!db_query(query))
         {
            count_purge_sched_loc += db_row_count();
         }
         sprintf(query, "DELETE FROM cif_schedules WHERE id = %ld", id);
         if(!db_query(query))
         {
            count_purge_sched++;
         }
      }
      mysql_free_result(result0);
   }
   _log(GENERAL, "Database purge completed.");
}
#endif

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
      _log(MINOR, zs);
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

   count_fetches++;

   if(!opt_filename)
   {
      static CURL * curlh;
      struct curl_slist * slist;

      if(!(curlh = curl_easy_init())) 
      {
         _log(CRITICAL, "scraper_cif():  Failed to obtain libcurl easy handle.");
         return 1;
      }
      curl_easy_setopt(curlh, CURLOPT_WRITEFUNCTION, cif_write_data);

      slist = NULL;
      slist = curl_slist_append(slist, "Cache-Control: no-cache");
      if(!slist)
      {
         _log(MAJOR,"scraper_cif():  Failed to create slist.");
         return 1;
      }

      // Build URL
      if(opt_url)
      {
         strcpy(url, opt_url);
      }
      else
      {
         now = time(NULL);
         when = now - 24*60*60;
         broken = localtime(&when);
         if(fetch_all)
         {
            sprintf(url, "https://datafeeds.networkrail.co.uk/ntrod/CifFileAuthenticate?type=CIF_ALL_FULL_DAILY&day=toc-full");
         }
         else
         {
            sprintf(url, "https://datafeeds.networkrail.co.uk/ntrod/CifFileAuthenticate?type=CIF_ALL_UPDATE_DAILY&day=toc-update-%s", weekdays[broken->tm_wday]);
         }
      }
      sprintf(zs, "Fetching \"%s\"", url);
      _log(GENERAL, zs);

      if(opt_url || debug)
      {
         now = time(NULL);
         sprintf(filepathz, "/tmp/cifdb-cif-fetch-%ld.gz", now);
         sprintf(filepath,  "/tmp/cifdb-cif-fetch-%ld",    now);
      }
      else if(fetch_all)
      {
         sprintf(filepathz, "/tmp/cifdb-cif-all-%02d-%02d-%02d-%s.gz", broken->tm_year%100, broken->tm_mon + 1, broken->tm_mday, weekdays[broken->tm_wday]);
         sprintf(filepath,  "/tmp/cifdb-cif-all-%02d-%02d-%02d-%s",    broken->tm_year%100, broken->tm_mon + 1, broken->tm_mday, weekdays[broken->tm_wday]);
      }
      else
      {
         sprintf(filepathz, "/tmp/cifdb-cif-%02d-%02d-%02d-%s.gz", broken->tm_year%100, broken->tm_mon + 1, broken->tm_mday, weekdays[broken->tm_wday]);
         sprintf(filepath,  "/tmp/cifdb-cif-%02d-%02d-%02d-%s",    broken->tm_year%100, broken->tm_mon + 1, broken->tm_mday, weekdays[broken->tm_wday]);
      }

      if(!(fp_result = fopen(filepathz, "w")))
      {
         sprintf(zs, "Failed to open \"%s\" for writing.", filepathz);
         _log(MAJOR, zs);
         return 1;
      }

      curl_easy_setopt(curlh, CURLOPT_HTTPHEADER, slist);

      // Set timeouts
      curl_easy_setopt(curlh, CURLOPT_NOSIGNAL,              1);
      curl_easy_setopt(curlh, CURLOPT_FTP_RESPONSE_TIMEOUT, 64);
      curl_easy_setopt(curlh, CURLOPT_TIMEOUT,              64);
      curl_easy_setopt(curlh, CURLOPT_CONNECTTIMEOUT,       64);

      // Debugging prints.
      // curl_easy_setopt(curlh, CURLOPT_VERBOSE,               1);

      // URL and login
      curl_easy_setopt(curlh, CURLOPT_URL,     url);
      sprintf(zs, "%s:%s", NATIONAL_RAIL_USERNAME, NATIONAL_RAIL_PASSWORD);
      curl_easy_setopt(curlh, CURLOPT_USERPWD, zs);
      curl_easy_setopt(curlh, CURLOPT_FOLLOWLOCATION,        1);

      total_bytes = 0;

      CURLcode result;
      if((result = curl_easy_perform(curlh)))
      {
         sprintf(zs, "fetch_file(): curl_easy_perform() returned error %d: %s.", result, curl_easy_strerror(result));
         _log(MAJOR, zs);
         if(fp_result) fclose(fp_result);
         return 1;
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
   
      sprintf(zs, "/bin/gunzip -f %s", filepathz);
      int rc;
      if((rc = system(zs)))
      {
         // Seems to return -1 but work OK when root?????
         //         sprintf(zs, "gunzip returned error %d", rc);
         //garner_log(MAJOR, zs);
         //failed = true;
      }
   }
   else
   {
      strcpy(filepath, opt_filename);
   }

   if(!(fp_result = fopen(filepath, "r")))
   {
      sprintf(zs, "Failed to open \"%s\" for reading.", filepath);
      _log(MAJOR, zs);
      return 1;
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
         sprintf(zs, "Recovered timestamp: %s", time_text(stamp, 0));
         _log(test_mode?GENERAL:DEBUG, zs);
         sprintf(zs, "Stored in file \"%s\"", filepath);
         _log(test_mode?GENERAL:DEBUG, zs);

         if(!test_mode && !opt_url && !opt_filename && (now < stamp || now - stamp > 36*60*60))
         {
            sprintf(zs, "Timestamp %s is incorrect.", time_text(stamp, 0));
            _log(MAJOR, zs);
            fclose(fp_result);
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

void extract_match(const char * source, const regmatch_t * matches, const unsigned int match, char * result, const size_t max_length)
{
   size_t size;

   size = matches[match].rm_eo - matches[match].rm_so;
   if(size > max_length - 1) 
   {
      char zs[128];
      sprintf(zs, "extract_match():  String length 0x%x truncated to 0x%x.", size, max_length - 1);
      _log(MAJOR, zs);
      size = max_length - 1;
   }

   strncpy(result, source + matches[match].rm_so, size);
   result[size] = '\0';
}

