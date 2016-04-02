 /*
    Copyright (C) 2014, 2015 Phil Wieland

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
#include <sys/vfs.h>

#include "misc.h"
#include "db.h"
#include "database.h"

#define NAME  "archdb"
#define BUILD "X328"

static int smart_loop(const word initial_quantity, int (*f)(word), const word stat);
static int perform(void);
static int cif_associations(const word quantity);
static int cif_schedules(const word quantity);
static int trust_activation(const word quantity);
static int trust_cancellation(const word quantity);
static int trust_movement(const word quantity);
static int trust_changeorigin(const word quantity);
static int trust_changeid(const word quantity);
static int trust_generic(const char * const table, const word quantity, const word stat);
static word check_space(void);

static word debug, run, report_only, verbose, archive;
static time_t start_time, last_reported_time;

#define NONE_FOUND  -98
#define DISC_SPACE  -97
#define RUN_ABORTED -96
#define TRUST_TIME_RANGE (15L * 24L * 60L * 60L)

// Stats
enum stats_categories {CIFAssociation, CIFSchedule, CIFScheduleLocation, CIFChangeEnRoute, 
TrustActivation, TrustCancellation, TrustMovement, TrustChangeOrigin, TrustChangeID, OrphanTrustCancellation, OrphanTrustMovement, OrphanTrustChangeOrigin, OrphanTrustChangeID, MAXstats};
qword stats[MAXstats];
const char * stats_category[MAXstats] = 
   {
      "CIF association records processed",
      "CIF schedule records processed",
      "CIF schedule location records processed",
      "CIF change en-route records processed",
      "Trust activation records processed", 
      "Trust cancellation records processed", 
      "Trust movement records processed", 
      "Trust change origin records processed", 
      "Trust change ID records processed", 
      "Trust cancellation orphan records processed", 
      "Trust movement orphan records processed", 
      "Trust change origin orphan records processed", 
      "Trust change ID orphan records processed", 
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
   int result;
   char config_file_path[256];

   age = 0;
   report_only = false;
   verbose = false;
   archive = true;

   word usage = false;
   int c;
   strcpy(config_file_path, "/etc/openrail.conf");
   while ((c = getopt (argc, argv, ":rc:a:pd")) != -1)
   {
      switch (c)
      {
      case 'c':
         strcpy(config_file_path, optarg);
         break;
      case 'r':
         report_only = true;
         break;
      case 'a':
         age = atoi(optarg);
         break;
      case 'p':
         verbose = true;
         break;
      case 'd':
         archive = false;
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

   if(report_only)
   {
      printf("Sorry.  Report only mode not implemented.\n");
      usage = true;
   }

   if(usage) 
   {
      printf(
             "Usage: %s [-c <file>] [-a <days>] [-p]\n"
             "   -c <file>  Path to config file.\n"
             "   -a <days>  Number of days to retain.  Omit this to skip the archiving process.\n"
             "   -d         Delete old data instead of archiving.\n"
             //"   -r         Report what would be archived, do not alter database.\n"
             "   -p         Print activity as well as logging.\n"
             , argv[0]);
      exit(1);
   }

   start_time = time(NULL);
   last_reported_time = start_time;

   if(signal(SIGTERM, termination_handler) == SIG_IGN) signal(SIGTERM, SIG_IGN);
   if(signal(SIGINT,  termination_handler) == SIG_IGN) signal(SIGINT,  SIG_IGN);
   if(signal(SIGHUP,  termination_handler) == SIG_IGN) signal(SIGHUP,  SIG_IGN);

   _log_init(debug?"/tmp/archdb.log":"/var/log/garner/archdb.log", (debug?1:(verbose?4:0)));
   
   _log(GENERAL, "");
   _log(GENERAL, "%s %s", NAME, BUILD);
   
   if(debug)
   {
      _log(GENERAL, "Debug mode selected.");
   }
   
   // Enable core dumps
   struct rlimit limit;
   if(!getrlimit(RLIMIT_CORE, &limit))
   {
      limit.rlim_cur = RLIM_INFINITY;
      setrlimit(RLIMIT_CORE, &limit);
   }
   
   // Initialise database
   {
      db_init(conf[conf_db_server], conf[conf_db_user], conf[conf_db_password], conf[conf_db_name]);

      word e;
      if((e=database_upgrade(archdb)))
      {
         _log(CRITICAL, "Error %d in upgrade_database().  Aborting.", e);
         exit(1);
      }
   }

   // Zero the stats
   {
      for(i=0; i < MAXstats; i++) { stats[i] = 0; }
   }

   run = true;

   result = perform();

   char report[8192];
   char zs[1024];
   report[0] = '\0';

   _log(GENERAL, "");
   _log(GENERAL, "End of run:");

   if(!run)
   {
      _log(GENERAL,  "*** Processing interrupted. ***");
      strcat(report, "*** Processing interrupted. ***\n");
   }

   if(result == DISC_SPACE)
   {
      _log(GENERAL,  "*** Processing aborted due to shortage of disc space. ***");
      strcat(report, "*** Processing aborted due to shortage of disc space. ***\n");
   }


   sprintf(zs, "%48s: %s", "Database", conf[conf_db_name]);
   _log(GENERAL, zs);
   strcat(report, zs);
   strcat(report, "\n");

   sprintf(zs, "%48s: %d days (Threshold %ld %s)", "Cut-off age", age, threshold, time_text(threshold, true));
   _log(GENERAL, zs);
   strcat(report, zs);
   strcat(report, "\n");

   sprintf(zs, "%48s: %s", "Old records were", archive?"Archived":"Deleted");
   _log(GENERAL, zs);
   strcat(report, zs);
   strcat(report, "\n");

   sprintf(zs, "%48s: %ld minutes", "Elapsed time", (time(NULL) - start_time + 30) / 60);
   _log(GENERAL, zs);
   strcat(report, zs); strcat(report, "\n");

   for(i=0; i<MAXstats; i++)
   {
      sprintf(zs, "%48s: %s", stats_category[i], commas_q(stats[i]));
      _log(GENERAL, zs);
      strcat(report, zs);
      strcat(report, "\n");
   }

   {
#define DB_REPORT_FORMAT "%30s: %s"
      MYSQL_RES * result;
      MYSQL_ROW row;
      // Database report
      _log(GENERAL, "Size of live database after archiving:");
      strcat(report, "\nSize of live database after archiving:\n");

      if(!db_query("SELECT COUNT(*) FROM cif_associations"))
      {
         result = db_store_result();
         if((row = mysql_fetch_row(result))) 
         {
            sprintf(zs, DB_REPORT_FORMAT, "CIF association records", commas(atol(row[0])));
            _log(GENERAL, zs);
            strcat(report, zs);
            strcat(report, "\n");
         }
         mysql_free_result(result);
      }
      if(!db_query("SELECT COUNT(*) FROM cif_schedules"))
      {
         result = db_store_result();
         if((row = mysql_fetch_row(result))) 
         {
            sprintf(zs, DB_REPORT_FORMAT, "CIF schedule records", commas(atol(row[0])));
            _log(GENERAL, zs);
            strcat(report, zs);
            strcat(report, "\n");
         }
         mysql_free_result(result);
      }
      if(!db_query("SELECT COUNT(*) FROM cif_schedule_locations"))
      {
         result = db_store_result();
         if((row = mysql_fetch_row(result))) 
         {
            sprintf(zs, DB_REPORT_FORMAT, "CIF schedule location records", commas(atol(row[0])));
            _log(GENERAL, zs);
            strcat(report, zs);
            strcat(report, "\n");
         }
         mysql_free_result(result);
      }
      if(!db_query("SELECT COUNT(*) FROM cif_changes_en_route"))
      {
         result = db_store_result();
         if((row = mysql_fetch_row(result))) 
         {
            sprintf(zs, DB_REPORT_FORMAT, "CIF change en route records", commas(atol(row[0])));
            _log(GENERAL, zs);
            strcat(report, zs);
            strcat(report, "\n");
         }
         mysql_free_result(result);
      }
      if(!db_query("SELECT COUNT(*) FROM trust_activation"))
      {
         result = db_store_result();
         if((row = mysql_fetch_row(result))) 
         {
            sprintf(zs, DB_REPORT_FORMAT, "Trust activation records", commas(atol(row[0])));
            _log(GENERAL, zs);
            strcat(report, zs);
            strcat(report, "\n");
         }
         mysql_free_result(result);
      }
      if(!db_query("SELECT COUNT(*) FROM trust_cancellation"))
      {
         result = db_store_result();
         if((row = mysql_fetch_row(result))) 
         {
            sprintf(zs, DB_REPORT_FORMAT, "Trust cancellation records", commas(atol(row[0])));
            _log(GENERAL, zs);
            strcat(report, zs);
            strcat(report, "\n");
         }
      }
      if(!db_query("SELECT COUNT(*) FROM trust_movement"))
      {
         result = db_store_result();
         if((row = mysql_fetch_row(result))) 
         {
            sprintf(zs, DB_REPORT_FORMAT, "Trust movement records", commas(atol(row[0])));
            _log(GENERAL, zs);
            strcat(report, zs);
            strcat(report, "\n");
         }
         mysql_free_result(result);
      }
      if(!db_query("SELECT COUNT(*) FROM trust_changeid"))
      {
         result = db_store_result();
         if((row = mysql_fetch_row(result))) 
         {
            sprintf(zs, DB_REPORT_FORMAT, "Trust change id records", commas(atol(row[0])));
            _log(GENERAL, zs);
            strcat(report, zs);
            strcat(report, "\n");
         }
         mysql_free_result(result);
      }
      if(!db_query("SELECT COUNT(*) FROM trust_changeorigin"))
      {
         result = db_store_result();
         if((row = mysql_fetch_row(result))) 
         {
            sprintf(zs, DB_REPORT_FORMAT, "Trust change origin records", commas(atol(row[0])));
            _log(GENERAL, zs);
            strcat(report, zs);
            strcat(report, "\n");
         }
         mysql_free_result(result);
      }

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

static int smart_loop(const word initial_quantity, int (*f)(word), const word stat)
{
   qword elapsed;
   word quantity = initial_quantity;
   int reply;

   time_t last_report = time(NULL);

   if(check_space()) return DISC_SPACE;

   while (run)  
   {
      if(time(NULL) - last_report > 10*60)
      {
         _log(GENERAL, "   Archived %s records so far.  Current batch size is %s.", commas_q(stats[stat]), commas(quantity));
         last_report += 10*60;
      }
      elapsed = time_ms();
      reply = f(quantity);
      if(reply) 
      {
         _log(GENERAL, "   Final batch size was %s.", commas(quantity));
         return reply;
      }
      elapsed = time_ms() - elapsed;
      if(elapsed < THRESHOLD_VL) quantity = (quantity * 6 / 5) + 1;
      else if(elapsed < THRESHOLD_L) quantity = (quantity * 21 / 20) + 1;
      else if(elapsed < THRESHOLD_H);
      else if(elapsed < THRESHOLD_VH) { quantity = (quantity * 19 / 20); if(quantity > MIN_QUANTITY) quantity--; }
      else                            quantity = quantity / 2;
      if(quantity > MAX_QUANTITY) quantity = MAX_QUANTITY;
      if(quantity < MIN_QUANTITY) quantity = MIN_QUANTITY;
      _log(DEBUG, "smart_loop():  %lld ms elapsed.  New quantity is %d", elapsed, quantity);
      if(run) sleep(debug?10:1);
   }
   _log(GENERAL, "   Final batch size was %d.", quantity);
   return 0;
}

static int perform(void)
{
   struct tm * broken;
   if(age)
   {
      // Move threshold to 02:30
      // There must be a better way of doing this?
      threshold = time(NULL);
      broken = localtime(&threshold);
      broken->tm_hour = 12;
      threshold = timelocal(broken);
      threshold -= 60*60*24*age;
      broken = localtime(&threshold);
      broken->tm_hour = 2; broken->tm_min = 30; broken->tm_sec = 0;
      threshold = timelocal(broken);

      if(archive)
         _log(GENERAL, "Archiving all records more than %d days old.  (Cut-off threshold is %s.)", age, time_text(threshold, true));
      else
         _log(GENERAL, "Deleting all records more than %d days old.  (Cut-off threshold is %s.)", age, time_text(threshold, true));


      int result;

      _log(GENERAL, "Processing table cif_associations...");
      result = smart_loop(1, &cif_associations, CIFAssociation);
      if(run && result != NONE_FOUND)
      {
         _log(CRITICAL, "Processing cif_associations failed, error %d.", result);
         return result;
      }
      if(!run) return RUN_ABORTED;

      _log(GENERAL, "Processing cif schedule tables...");
      result = smart_loop(10, &cif_schedules, CIFSchedule);
      if(run && result != NONE_FOUND)
      {
         _log(CRITICAL, "Processing cif schedule tables failed, error %d.", result);
         return result;
      }
      if(!run) return RUN_ABORTED;

      _log(GENERAL, "Processing TRUST records...");
      result = smart_loop(10, &trust_activation, TrustActivation);
      if(run && result != NONE_FOUND)
      {
         _log(CRITICAL, "Processing TRUST records failed, error %d.", result);
         return result;
      }
      if(!run) return RUN_ABORTED;
      
      _log(GENERAL, "Processing table trust_cancellation orphans...");
      result = smart_loop(10, &trust_cancellation, OrphanTrustCancellation);
      if(run && result != NONE_FOUND)
      {
         _log(CRITICAL, "Processing trust_cancellation failed, error %d.", result);
         return result;
      }
      if(!run) return RUN_ABORTED;

      _log(GENERAL, "Processing table trust_movement orphans...");
      result = smart_loop(10, &trust_movement, OrphanTrustMovement);
      if(run && result != NONE_FOUND)
      {
         _log(CRITICAL, "Processing trust_movement failed, error %d.", result);
         return result;
      }
      if(!run) return RUN_ABORTED;

      _log(GENERAL, "Processing table trust_changeorigin orphans...");
      result = smart_loop(10, &trust_changeorigin, OrphanTrustChangeOrigin);
      if(run && result != NONE_FOUND)
      {
         _log(CRITICAL, "Processing trust_changeorigin failed, error %d.", result);
         return result;
      }
      if(!run) return RUN_ABORTED;

      _log(GENERAL, "Processing table trust_changeid orphans...");
      result = smart_loop(10, &trust_changeid, OrphanTrustChangeID);
      if(run && result != NONE_FOUND)
      {
         _log(CRITICAL, "Processing trust_changeid failed, error %d.", result);
         return result;
      }
      if(!run) return RUN_ABORTED;
   }
   return 0;
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

   db_errored = false;
   while(run && !db_errored && (row = mysql_fetch_row(result)))
   {
      {
         db_start_transaction();
         if(archive)
         {
            sprintf(q, "INSERT INTO cif_associations_arch SELECT * FROM cif_associations WHERE created = %s AND main_train_uid = '%s'", row[1], row[2]);

            r = db_query(q);
            if(r) 
            {
               mysql_free_result(result);
               db_rollback_transaction();
               return r;
            }
         }

         sprintf(q, "DELETE FROM cif_associations WHERE created = %s AND main_train_uid = '%s'", row[1], row[2]);
         r = db_query(q);
         if(r) 
         {
            mysql_free_result(result);
            db_rollback_transaction();
            return r;
         }

         word deleted = db_row_count();
         db_commit_transaction();
         stats[CIFAssociation] += deleted;
         count += deleted;
         _log(DEBUG, "Deleted %d records.", deleted);
      }
   }

   _log(DEBUG, "cif_associations(%d) has archived %d records.", quantity, count);

   mysql_free_result(result);
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
   db_errored = false;
   while(run && !db_errored && (row = mysql_fetch_row(result)))
   {
      db_start_transaction();

      if(archive)
      {
         sprintf(q, "INSERT INTO cif_schedules_arch SELECT * FROM cif_schedules WHERE id = %s", row[0]);

         r = db_query(q);
         if(r) 
         {
            mysql_free_result(result);
            db_rollback_transaction();
            return r;
         }
      }

      sprintf(q, "DELETE FROM cif_schedules WHERE id = %s", row[0]);
      r = db_query(q);
      if(r) 
      {
         mysql_free_result(result);
         db_rollback_transaction();
         return r;
      }

      word deleted = db_row_count();

      stats[CIFSchedule] += deleted;
      count += deleted;

      if(archive)
      {
         sprintf(q, "INSERT INTO cif_schedule_locations_arch SELECT * FROM cif_schedule_locations WHERE cif_schedule_id = %s", row[0]);

         r = db_query(q);
         if(r) 
         {
            mysql_free_result(result);
            db_rollback_transaction();
            return r;
         }
      }

      sprintf(q, "DELETE FROM cif_schedule_locations WHERE cif_schedule_id = %s", row[0]);
      r = db_query(q);
      if(r) 
      {
         mysql_free_result(result);
         db_rollback_transaction();
         return r;
      }

      deleted = db_row_count();
      stats[CIFScheduleLocation] += deleted;
      countl += deleted;

      if(archive)
      {
         sprintf(q, "INSERT INTO cif_changes_en_route_arch SELECT * FROM cif_changes_en_route WHERE cif_schedule_id = %s", row[0]);

         r = db_query(q);
         if(r) 
         {
            mysql_free_result(result);
            db_rollback_transaction();
            return r;
         }
      }

      sprintf(q, "DELETE FROM cif_schedule_locations WHERE cif_schedule_id = %s", row[0]);
      r = db_query(q);
      if(r) 
      {
         mysql_free_result(result);
         db_rollback_transaction();
         return r;
      }

      deleted = db_row_count();
      db_commit_transaction();
      stats[CIFChangeEnRoute] += deleted;
      countl += deleted;
   }

   _log(DEBUG, "cif_schedules(%d) has archived %d and %d records.", quantity, count, countl);

   mysql_free_result(result);
   if(count) return 0;

   return NONE_FOUND;
}

static int trust_activation(const word quantity)
{
   char q[1024];
   int r;
   int count = 0;
   MYSQL_RES * result;
   MYSQL_ROW row;
   char trust_id[128];

   sprintf(q, "SELECT trust_id, created FROM trust_activation WHERE created < %ld ORDER BY created LIMIT %d", threshold, quantity);
   r = db_query(q);
   if(r) return r;

   result = db_store_result();
   db_errored = false;
   while(run && !db_errored && (row = mysql_fetch_row(result)))
   {
      db_start_transaction();

      time_t created = atol(row[1]);
      strcpy(trust_id, row[0]);

      if(archive)
      {
         sprintf(q, "INSERT INTO trust_activation_arch SELECT * FROM trust_activation WHERE created < %ld AND created > %ld AND trust_id = '%s'", created + TRUST_TIME_RANGE, created - TRUST_TIME_RANGE, trust_id);

         r = db_query(q);
         if(r) 
         {
            mysql_free_result(result);
            db_rollback_transaction();
            return r;
         }
      }

      sprintf(q, "DELETE FROM trust_activation WHERE created < %ld AND created > %ld AND trust_id = '%s'", created + TRUST_TIME_RANGE, created - TRUST_TIME_RANGE, trust_id);
      r = db_query(q);
      if(r) 
      {
         mysql_free_result(result);
         db_rollback_transaction();
         return r;
      }

      word deleted = db_row_count();
      stats[TrustActivation] += deleted;
      count += deleted;

      if(archive)
      { 
         sprintf(q, "INSERT INTO trust_cancellation_arch SELECT * FROM trust_cancellation WHERE created < %ld AND created > %ld AND trust_id = '%s'", created + TRUST_TIME_RANGE, created - TRUST_TIME_RANGE, trust_id);

         r = db_query(q);
         if(r) 
         {
            mysql_free_result(result);
            db_rollback_transaction();
            return r;
         }
      }

      sprintf(q, "DELETE FROM trust_cancellation WHERE created < %ld AND created > %ld AND trust_id = '%s'", created + TRUST_TIME_RANGE, created - TRUST_TIME_RANGE, trust_id);
      r = db_query(q);
      if(r) 
      {
         mysql_free_result(result);
         db_rollback_transaction();
         return r;
      }

      deleted = db_row_count();
      stats[TrustCancellation] += deleted;

      if(archive)
      { 
         sprintf(q, "INSERT INTO trust_movement_arch SELECT * FROM trust_movement WHERE created < %ld AND created > %ld AND trust_id = '%s'", created + TRUST_TIME_RANGE, created - TRUST_TIME_RANGE, trust_id);
         
         r = db_query(q);
         if(r) 
         {
            mysql_free_result(result);
            db_rollback_transaction();
            return r;
         }
      }

      sprintf(q, "DELETE FROM trust_movement WHERE created < %ld AND created > %ld AND trust_id = '%s'", created + TRUST_TIME_RANGE, created - TRUST_TIME_RANGE, trust_id);
      r = db_query(q);
      if(r) 
      {
         mysql_free_result(result);
         db_rollback_transaction();
         return r;
      }

      deleted = db_row_count();
      stats[TrustMovement] += deleted;

      if(archive)
      { 
         sprintf(q, "INSERT INTO trust_changeorigin_arch SELECT * FROM trust_changeorigin WHERE created < %ld AND created > %ld AND trust_id = '%s'", created + TRUST_TIME_RANGE, created - TRUST_TIME_RANGE, trust_id);

         r = db_query(q);
         if(r) 
         {
            mysql_free_result(result);
            db_rollback_transaction();
            return r;
         }
      }

      sprintf(q, "DELETE FROM trust_changeorigin WHERE created < %ld AND created > %ld AND trust_id = '%s'", created + TRUST_TIME_RANGE, created - TRUST_TIME_RANGE, trust_id);
      r = db_query(q);
      if(r) 
      {
         mysql_free_result(result);
         db_rollback_transaction();
         return r;
      }

      deleted = db_row_count();
      stats[TrustChangeOrigin] += deleted;

      if(archive)
      {
         sprintf(q, "INSERT INTO trust_changeid_arch SELECT * FROM trust_changeid WHERE created < %ld AND created > %ld AND trust_id = '%s'", created + TRUST_TIME_RANGE, created - TRUST_TIME_RANGE, trust_id);

         r = db_query(q);
         if(r) 
         {
            mysql_free_result(result);
            db_rollback_transaction();
            return r;
         }
      }

      sprintf(q, "DELETE FROM trust_changeid WHERE created < %ld AND created > %ld AND trust_id = '%s'", created + TRUST_TIME_RANGE, created - TRUST_TIME_RANGE, trust_id);
      r = db_query(q);
      if(r) 
      {
         mysql_free_result(result);
         db_rollback_transaction();
         return r;
      }

      deleted = db_row_count();
      stats[TrustChangeID] += deleted;
      db_commit_transaction();
   }

   mysql_free_result(result);
   if(count) return 0;

   return NONE_FOUND;
}

static int trust_cancellation(const word quantity)
{
   return trust_generic("trust_cancellation", quantity, OrphanTrustCancellation);
}

static int trust_movement(const word quantity)
{
   return trust_generic("trust_movement", quantity, OrphanTrustMovement);
}

static int trust_changeorigin(const word quantity)
{
   return trust_generic("trust_changeorigin", quantity, OrphanTrustChangeOrigin);
}

static int trust_changeid(const word quantity)
{
   return trust_generic("trust_changeid", quantity, OrphanTrustChangeID);
}

static int trust_generic(const char * const table, const word quantity, const word stat)
{
   char q[1024];
   int r;
   int count = 0;
   MYSQL_RES * result;
   MYSQL_ROW row;
   time_t last_timestamp = 0;

   // N.B. Time threshold here will be one hour out if it crosses a GMT/BST transition.
   sprintf(q, "SELECT DISTINCT created FROM %s WHERE created < %ld ORDER BY created LIMIT %d", table, threshold - TRUST_TIME_RANGE, quantity);
   r = db_query(q);
   if(r) return r;

   result = db_store_result();
   db_errored = false;
   while(run && !db_errored && (row = mysql_fetch_row(result)))
   {
      db_start_transaction();
      last_timestamp = atol(row[0]);

      if(archive)
      {
         sprintf(q, "INSERT INTO %s_arch SELECT * FROM %s WHERE created = %s", table, table, row[0]);

         r = db_query(q);
         if(r) 
         {
            mysql_free_result(result);
            db_rollback_transaction();
            return r;
         }
      }

      sprintf(q, "DELETE FROM %s WHERE created = %s", table, row[0]);
      r = db_query(q);
      if(r) 
      {
         mysql_free_result(result);
         db_rollback_transaction();
         return r;
      }

      word deleted = db_row_count();
      db_commit_transaction();
      stats[stat] += deleted;
      count += deleted;
      _log(DEBUG, "Deleted %d records dated %s.", deleted, time_text(atol(row[0]), true));
   }

   _log(DEBUG, "trust_generic(\"%s\", %d) has archived %d records.  Last datestamp was %s.", table, quantity, count, time_text(last_timestamp, true));

   mysql_free_result(result);
   if(count) return 0;

   return NONE_FOUND;
}

static word check_space(void)
{
   struct statfs fstatus;
   if(!statfs("/var", &fstatus))
   {
      qword free = fstatus.f_bavail * fstatus.f_bsize;
      // Archive mode: 2GB, Delete mode: 200MB (Decimal units)
      if((free < 2000000000LL && archive) || free < 200000000LL)
      {
         _log(CRITICAL, "Disc free space %s bytes is too low.  Terminating run.", commas_q(free));
         return 1;
      }
   }
   else
   {
      _log(CRITICAL, "Failed to read disc free space data.");
      return 2;
   }
   return 0;
}
 
