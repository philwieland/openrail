/*
    Copyright (C) 2014 Phil Wieland

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
#include <signal.h>

#include "misc.h"
#include "db.h"

#define NAME  "archdb"
#define BUILD "V322"

static int smart_loop(const word initial_quantity, int (*f)(word));
static void create_database(void);
static void perform(void);
static int cif_associations(const word quantity);
static int cif_schedules(const word quantity);
static int trust_activation(const word quantity);
static int trust_cancellation(const word quantity);
static int trust_movement(const word quantity);
static int trust_changeorigin(const word quantity);
static int trust_changeid(const word quantity);
static int trust_generic(const char * const table, const word quantity, const word stat);

static word debug, run;
static time_t start_time, last_reported_time;

#define NONE_FOUND -98

#define DAY_ROTATION 4

// Stats
enum stats_categories {CIFAssociation, CIFSchedule, CIFScheduleLocation, TrustActivation, TrustCancellation, TrustMovement, TrustChangeOrigin, TrustChangeID, MAXstats};
qword stats[MAXstats];
const char * stats_category[MAXstats] = 
   {
      "CIF association records archived",
      "CIF schedule records archived",
      "CIF schedule location records archived",
      "Trust activation records archived", 
      "Trust cancellation records archived", 
      "Trust movement records archived", 
      "Trust change origin records archived", 
      "Trust change ID records archived", 
   };

// Days age threshold
int age;
// Threshold in unix time
time_t threshold;

// Signal handling
void termination_handler(int signum)
{
   run = false;
}

int main(int argc, char **argv)
{
   word i;

   age = 0;

   word usage = true;
   int c;
   while ((c = getopt (argc, argv, "c:a:")) != -1)
      switch (c)
      {
      case 'c':
         if(load_config(optarg))
         {
            printf("Failed to read config file \"%s\".\n", optarg);
            usage = true;
         }
         else
         {
            usage = false;
         }
         break;
      case 'a':
         age = atoi(optarg);
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
             "-c <file>  Path to config file.\n"
             "-a <days>  Number of days to retain.  Omit this to skip the archiving.\n"
             );
      exit(1);
   }


   start_time = time(NULL);
   last_reported_time = start_time;

   if(signal(SIGTERM, termination_handler) == SIG_IGN) signal(SIGTERM, SIG_IGN);
   if(signal(SIGINT,  termination_handler) == SIG_IGN) signal(SIGINT,  SIG_IGN);
   if(signal(SIGHUP,  termination_handler) == SIG_IGN) signal(SIGHUP,  SIG_IGN);

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
   
   _log_init(debug?"/tmp/archdb.log":"/var/log/garner/archdb.log", debug?1:0);
   
   _log(GENERAL, "");
   _log(GENERAL, "%s %s", NAME, BUILD);
   
   if(debug == 1)
   {
      _log(GENERAL, "Debug mode selected.  Using TEST database.");
      _log(GENERAL, "To use live database, change the debug flag in the config file to 'false'");
   }
   
   // Enable core dumps
   struct rlimit limit;
   if(!getrlimit(RLIMIT_CORE, &limit))
   {
      limit.rlim_cur = RLIM_INFINITY;
      setrlimit(RLIMIT_CORE, &limit);
   }
   
   // Initialise database
   if(db_init(conf.db_server, conf.db_user, conf.db_pass, conf.db_name)) exit(1);

   // Zero the stats
   {
      for(i=0; i < MAXstats; i++) { stats[i] = 0; }
   }

   create_database();

   run = true;

   perform();

   db_disconnect();

   char report[8192];
   char zs[1024];
   report[0] = '\0';

   _log(GENERAL, "");
   _log(GENERAL, "End of run:");

   if(!run)
   {
      _log(GENERAL,  "*** Run interrupted. ***\n");
      strcat(report, "*** Run interrupted. ***\n");
   }

   sprintf(zs, "%-40s: %-12s ", "Database", conf.db_name);
   _log(GENERAL, zs);
   strcat(report, zs);
   strcat(report, "\n");

   sprintf(zs, "%-40s: %ld minutes", "Elapsed time", (time(NULL) - start_time + 30) / 60);
   _log(GENERAL, zs);
   strcat(report, zs); strcat(report, "\n");

   for(i=0; i<MAXstats; i++)
   {
      sprintf(zs, "%-40s: %-12s ", stats_category[i], commas_q(stats[i]));
      _log(GENERAL, zs);
      strcat(report, zs);
      strcat(report, "\n");
   }

   email_alert(NAME, BUILD, "Database Archive Report", report);

   exit(0);
}

#define MAX_QUANTITY 4096
#define MIN_QUANTITY 1
#define THRESHOLD_VL   4000
#define THRESHOLD_L    8000
#define THRESHOLD_H   10000
#define THRESHOLD_VH  14000

static int smart_loop(const word initial_quantity, int (*f)(word))
{
   qword elapsed;
   word quantity = initial_quantity;
   int reply;

   while (run)  
   {
      elapsed = time_ms();
      reply = f(quantity);
      if(reply) 
      {
         _log(GENERAL, "smart_loop():  Final quantity was %d.", quantity);
         return reply;
      }
      elapsed = time_ms() - elapsed;
      if(elapsed < THRESHOLD_VL) quantity = (quantity * 6 / 5) + 1;
      else if(elapsed < THRESHOLD_L) quantity = (quantity * 21 / 20) + 1;
      else if(elapsed < THRESHOLD_H);
      else if(elapsed < THRESHOLD_VH) quantity = (quantity * 19 / 20) - 1;
      else                            quantity = quantity / 2;
      if(quantity > MAX_QUANTITY) quantity = MAX_QUANTITY;
      if(quantity < MIN_QUANTITY) quantity = MIN_QUANTITY;
      _log(DEBUG, "smart_loop():  %lld ms elapsed.  New quantity is %d", elapsed, quantity);
      if(run) sleep(1);
      if(run && debug) sleep(1);
      if(run && debug) sleep(1);
      if(run && debug) sleep(1);
      if(run && debug) sleep(1);
      if(run && debug) sleep(1);
      if(run && debug) sleep(1);
      if(run && debug) sleep(1);
   }
   _log(GENERAL, "smart_loop():  Final quantity was %d.", quantity);
   return 0;
}

static void create_database(void)
{
   MYSQL_RES * result0;
   MYSQL_ROW row0;
   word create_trust_activation, create_trust_cancellation, create_trust_movement;
   word create_trust_changeorigin, create_trust_changeid;
   word create_cif_associations, create_cif_schedules, create_cif_schedule_locations;

   _log(PROC, "create_database()");

   create_trust_activation = create_trust_cancellation = create_trust_movement = true;
   create_trust_changeorigin = create_trust_changeid = true;
   create_cif_associations = create_cif_schedules = create_cif_schedule_locations = true;

   if(db_query("show tables")) return;
   
   result0 = db_store_result();
   while((row0 = mysql_fetch_row(result0))) 
   {
      if(!strcasecmp(row0[0], "trust_activation_arch")) create_trust_activation = false;
      if(!strcasecmp(row0[0], "trust_cancellation_arch")) create_trust_cancellation = false;
      if(!strcasecmp(row0[0], "trust_movement_arch")) create_trust_movement = false;
      if(!strcasecmp(row0[0], "trust_changeorigin_arch")) create_trust_changeorigin = false;
      if(!strcasecmp(row0[0], "trust_changeid_arch")) create_trust_changeid = false;
      if(!strcasecmp(row0[0], "cif_associations_arch")) create_cif_associations = false;
      if(!strcasecmp(row0[0], "cif_schedules_arch")) create_cif_schedules = false;
      if(!strcasecmp(row0[0], "cif_schedule_locations_arch")) create_cif_schedule_locations = false;
   }
   mysql_free_result(result0);

   if(create_trust_activation)
   {
      db_query(
"CREATE TABLE trust_activation_arch "
"(created INT UNSIGNED NOT NULL, "
"trust_id VARCHAR(16) NOT NULL, "
"cif_schedule_id INT UNSIGNED NOT NULL, "
"deduced         TINYINT UNSIGNED NOT NULL, "
"INDEX(cif_schedule_id), INDEX(trust_id), INDEX(created)"
") ENGINE = InnoDB"
               );
      _log(GENERAL, "Created database table trust_activation_arch.");
   }
   if(create_trust_cancellation)
   {
      db_query(
"CREATE TABLE trust_cancellation_arch "
"(created INT UNSIGNED NOT NULL, "
"trust_id VARCHAR(16) NOT NULL, "
"reason VARCHAR(8) NOT NULL, "
"type   VARCHAR(32) NOT NULL, "
"loc_stanox VARCHAR(8) NOT NULL, "
"reinstate TINYINT UNSIGNED NOT NULL, "
"INDEX(trust_id), INDEX(created) "
") ENGINE = InnoDB"
               );
      _log(GENERAL, "Created database table trust_cancellation_arch.");
   }
   if(create_trust_movement)
   {
      db_query(
"CREATE TABLE trust_movement_arch "
"(created            INT UNSIGNED NOT NULL, "
"trust_id            VARCHAR(16) NOT NULL, "
"event_type          VARCHAR(16) NOT NULL, "
"planned_event_type  VARCHAR(16) NOT NULL, "
"platform            VARCHAR(8) NOT NULL, "
"loc_stanox          VARCHAR(8) NOT NULL, "
"actual_timestamp    INT UNSIGNED NOT NULL, "
"gbtt_timestamp      INT UNSIGNED NOT NULL, "
"planned_timestamp   INT UNSIGNED NOT NULL, "
"timetable_variation INT NOT NULL, "
"event_source        VARCHAR(16) NOT NULL, "
"offroute_ind        BOOLEAN NOT NULL, "
"train_terminated    BOOLEAN NOT NULL, "
"variation_status    VARCHAR(16) NOT NULL, "
"next_report_stanox  VARCHAR(8) NOT NULL, "
"next_report_run_time INT UNSIGNED NOT NULL, "
"INDEX(trust_id), INDEX(created) "
") ENGINE = InnoDB"
               );
      _log(GENERAL, "Created database table trust_movement_arch.");
   }
   if(create_trust_changeorigin)
   {
      db_query(
"CREATE TABLE trust_changeorigin_arch "
"(created INT UNSIGNED NOT NULL, "
"trust_id VARCHAR(16) NOT NULL, "
"reason VARCHAR(8) NOT NULL, "
"loc_stanox VARCHAR(8) NOT NULL, "
"INDEX(trust_id), INDEX(created) "
") ENGINE = InnoDB"
               );
      _log(GENERAL, "Created database table trust_changeorigin_arch.");
   }
   if(create_trust_changeid)
   {
      db_query(
"CREATE TABLE trust_changeid_arch "
"(created INT UNSIGNED NOT NULL, "
"trust_id VARCHAR(16) NOT NULL, "
"new_trust_id VARCHAR(16) NOT NULL, "
"INDEX(trust_id), INDEX(new_trust_id), INDEX(created) "
") ENGINE = InnoDB"
               );
      _log(GENERAL, "Created database table trust_changeid_arch.");
   }
   if(create_cif_associations)
   {
      db_query(
"CREATE TABLE cif_associations_arch              "
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
") ENGINE = InnoDB"
               );
      _log(GENERAL, "Created database table cif_associations_arch.");
   }
   if(create_cif_schedules)
   {
      db_query(
"CREATE TABLE cif_schedules_arch                 "
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
"id                            INT UNSIGNED NOT NULL, "
"INDEX(schedule_end_date), INDEX(schedule_start_date), INDEX(CIF_train_uid), INDEX(CIF_stp_indicator) "
") ENGINE = InnoDB"
               );
      _log(GENERAL, "Created database table cif_schedules_arch.");
   }
   if(create_cif_schedule_locations)
   {
      db_query(
"CREATE TABLE cif_schedule_locations_arch        "
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
"INDEX(cif_schedule_id), INDEX(tiploc_code) "
") ENGINE = InnoDB"
               );
      _log(GENERAL, "Created database table cif_schedule_locations_arch.");
   }
}

static void perform(void)
{
   struct tm * broken;
   if(age)
   {
      // Move threshold to 02:00
      threshold = time(NULL) - 60*60*24*age;
      broken = localtime(&threshold);
      broken->tm_hour = 2; broken->tm_min = 0; broken->tm_sec = 0;
      threshold = timelocal(broken);

      _log(GENERAL, "Removing all records more than %d days old.  (Cut-off threshold is %s.)", age, time_text(threshold, true));

      int result;

      _log(GENERAL, "Processing table cif_associations...");
      result = smart_loop(1, &cif_associations);
      if(run && result != NONE_FOUND)
      {
         _log(CRITICAL, "Processing cif_associations failed, error %d.", result);
         return;
      }
      if(!run) return;

      _log(GENERAL, "Processing tables cif_schedules and cif_schedule_locations...");
      result = smart_loop(10, &cif_schedules);
      if(run && result != NONE_FOUND)
      {
         _log(CRITICAL, "Processing cif_schedules and cif_schedule_locations failed, error %d.", result);
         return;
      }
      if(!run) return;

      _log(GENERAL, "Processing table trust_activation...");
      result = smart_loop(10, &trust_activation);
      if(run && result != NONE_FOUND)
      {
         _log(CRITICAL, "Processing trust_activation failed, error %d.", result);
         return;
      }
      if(!run) return;
      
      _log(GENERAL, "Processing table trust_cancellation...");
      result = smart_loop(10, &trust_cancellation);
      if(run && result != NONE_FOUND)
      {
         _log(CRITICAL, "Processing trust_cancellation failed, error %d.", result);
         return;
      }
      if(!run) return;

      _log(GENERAL, "Processing table trust_movement...");
      result = smart_loop(10, &trust_movement);
      if(run && result != NONE_FOUND)
      {
         _log(CRITICAL, "Processing trust_movement failed, error %d.", result);
         return;
      }
      if(!run) return;

      _log(GENERAL, "Processing table trust_changeorigin...");
      result = smart_loop(10, &trust_changeorigin);
      if(run && result != NONE_FOUND)
      {
         _log(CRITICAL, "Processing trust_changeorigin failed, error %d.", result);
         return;
      }
      if(!run) return;

      _log(GENERAL, "Processing table trust_changeid...");
      result = smart_loop(10, &trust_changeid);
      if(run && result != NONE_FOUND)
      {
         _log(CRITICAL, "Processing trust_changeid failed, error %d.", result);
         return;
      }
      if(!run) return;
   }

   _log(GENERAL, "Dumping database...");

   time_t now = time(NULL);
   time_t day_number = now / 3600;  // Warning
   day_number /= 24;                // We will get the wrong day number if running between 00:00 and 00:59
   day_number %= DAY_ROTATION;                 // when summner time is active
   broken = localtime(&now);
   char day_filepath[256], mon_filepath[256];
   sprintf(day_filepath, "/var/backup/%s-database-day-%ld.sql.gz", conf.db_name, day_number);
   sprintf(mon_filepath, "/var/backup/%s-database-monthly-%d-%02d.sql.gz", conf.db_name, broken->tm_year + 1900, broken->tm_mon + 1);

   char c[1024];
   sprintf(c, "/usr/bin/nice /usr/bin/mysqldump --single-transaction --quick -h %s -u %s -p%s %s | /usr/bin/nice gzip --best > %s",
           conf.db_server, conf.db_user, conf.db_pass, conf.db_name, day_filepath);
   int i = system(c);
   sprintf(c, "/bin/cp %s %s", day_filepath, mon_filepath);
   i = system(c);
}

static int cif_associations(const word quantity)
{
   // There is nothing to order these by, so use created and main_train_uid as the 'key'
   char q[1024];
   int r;
   int count = 0;
   MYSQL_RES * result;
   MYSQL_ROW row;

   sprintf(q, "SELECT DISTINCT(concat(created, main_train_uid)), created, main_train_uid FROM cif_associations WHERE deleted < %ld OR assoc_end_date < %ld LIMIT %d", threshold, threshold, quantity);
   r = db_query(q);
   if(r) return r;

   result = db_store_result();
   while(run && (row = mysql_fetch_row(result)))
   {

      sprintf(q, "INSERT INTO cif_associations_arch SELECT * FROM cif_associations WHERE created = %s AND main_train_uid = '%s'", row[1], row[2]);

      r = db_query(q);
      if(r) 
      {
         mysql_free_result(result);
         return r;
      }

      sprintf(q, "DELETE FROM cif_associations WHERE created = %s AND main_train_uid = '%s'", row[1], row[2]);
      r = db_query(q);
      if(r) 
      {
         mysql_free_result(result);
         return r;
      }

      word deleted = db_row_count();
      stats[CIFAssociation] += deleted;
      count += deleted;
      _log(DEBUG, "Deleted %d records.", deleted);
   }

   _log(DEBUG, "cif_associations(%d) has archived %d records.", quantity, count);

   if(count) return 0;


   return NONE_FOUND;
}

static int cif_schedules(const word quantity)
{
   char q[1024];
   int r;
   int count = 0;
   int countl = 0;
   MYSQL_RES * result;
   MYSQL_ROW row;

   sprintf(q, "SELECT id FROM cif_schedules WHERE deleted < %ld OR schedule_end_date < %ld LIMIT %d", threshold, threshold, quantity);
   r = db_query(q);
   if(r) return r;

   result = db_store_result();
   while(run && (row = mysql_fetch_row(result)))
   {

      sprintf(q, "INSERT INTO cif_schedules_arch SELECT * FROM cif_schedules WHERE id = %s", row[0]);

      r = db_query(q);
      if(r) 
      {
         mysql_free_result(result);
         return r;
      }

      sprintf(q, "DELETE FROM cif_schedules WHERE id = %s", row[0]);
      r = db_query(q);
      if(r) 
      {
         mysql_free_result(result);
         return r;
      }

      word deleted = db_row_count();
      stats[CIFSchedule] += deleted;
      count += deleted;

      sprintf(q, "INSERT INTO cif_schedule_locations_arch SELECT * FROM cif_schedule_locations WHERE cif_schedule_id = %s", row[0]);

      r = db_query(q);
      if(r) 
      {
         mysql_free_result(result);
         return r;
      }

      sprintf(q, "DELETE FROM cif_schedule_locations WHERE cif_schedule_id = %s", row[0]);
      r = db_query(q);
      if(r) 
      {
         mysql_free_result(result);
         return r;
      }

      deleted = db_row_count();
      stats[CIFScheduleLocation] += deleted;
      countl += deleted;
   }

   _log(DEBUG, "cif_schedules(%d) has archived %d and %d records.", quantity, count, countl);

   if(count) return 0;

   return NONE_FOUND;
}

static int trust_activation(const word quantity)
{
   return trust_generic("trust_activation", quantity, TrustActivation);
}

static int trust_cancellation(const word quantity)
{
   return trust_generic("trust_cancellation", quantity, TrustCancellation);
}

static int trust_movement(const word quantity)
{
   return trust_generic("trust_movement", quantity, TrustMovement);
}

static int trust_changeorigin(const word quantity)
{
   return trust_generic("trust_changeorigin", quantity, TrustChangeOrigin);
}

static int trust_changeid(const word quantity)
{
   return trust_generic("trust_changeid", quantity, TrustChangeID);
}

static int trust_generic(const char * const table, const word quantity, const word stat)
{
   char q[1024];
   int r;
   int count = 0;
   MYSQL_RES * result;
   MYSQL_ROW row;
   time_t last_timestamp = 0;

   sprintf(q, "SELECT DISTINCT created FROM %s WHERE created < %ld ORDER BY created LIMIT %d", table, threshold, quantity);
   r = db_query(q);
   if(r) return r;

   result = db_store_result();
   while(run && (row = mysql_fetch_row(result)))
   {
      last_timestamp = atol(row[0]);

      sprintf(q, "INSERT INTO %s_arch SELECT * FROM %s WHERE created = %s", table, table, row[0]);

      r = db_query(q);
      if(r) 
      {
         mysql_free_result(result);
         return r;
      }

      sprintf(q, "DELETE FROM %s WHERE created = %s", table, row[0]);
      r = db_query(q);
      if(r) 
      {
         mysql_free_result(result);
         return r;
      }

      word deleted = db_row_count();
      stats[stat] += deleted;
      count += deleted;
      _log(DEBUG, "Deleted %d records dated %s.", deleted, time_text(atol(row[0]), true));
   }

   _log(DEBUG, "trust_generic(\"%s\", %d) has archived %d records.  Last datestamp was %s.", table, quantity, count, time_text(last_timestamp, true));

   if(count) return 0;

   return NONE_FOUND;
}
