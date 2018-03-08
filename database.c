/*
    Copyright (C) 2015 Phil Wieland

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

#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <signal.h>
#include <time.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/prctl.h>
#include <sys/resource.h>
#include <mysql.h>
#include <sys/sysinfo.h>
#include <netinet/in.h>
#include <errno.h>
#include <netdb.h>
#include <stdarg.h>

#include "misc.h"
#include "db.h"
#include "database.h"

#define NAME  "database"
#define BUILD "X320"

#define NEW_VERSION 1

static word table_exists(const char * const table_like);

word database_upgrade(const word caller)
{
   char query[256];
   MYSQL_RES * res;
   MYSQL_ROW row;
   word lock, result, old_version;

   result = 0;

   // Lock the upgrade process.
   // NOTE - This DOES NOT lock the database from normal data processing, it merely prevents
   // simultaneous database_upgrade processing.
   lock = 0;
   while(!lock)
   {
      if(!db_query("SELECT GET_LOCK('openrail.database_upgrade', 1)"))
      {
         res = db_store_result();
         row = mysql_fetch_row(res);
         lock = atoi(row[0]);
         mysql_free_result(res);
      }
      if(!lock)
      {
         _log(MINOR, "Waiting for database upgrade lock.");
         sleep(8);
      }
   }

   old_version = 0;
   if(table_exists("database_version"))
   {
      sprintf(query, "SELECT * FROM database_version");
      if(!db_query(query))
      {
         res = db_store_result();
         row = mysql_fetch_row(res);
         old_version = atoi(row[0]);
         mysql_free_result(res);
      }
   }

   // Note:  old > NEW is no-op, not error.  If a particular program is not changed during an upgrade, it will 
   // not need to be re-compiled just for the database change.
   // Note:  If a change creates a new table, there's no need to do it in this upgrade section, just leave it
   // to the table creation below.
   if(old_version < NEW_VERSION)
   {
      _log(GENERAL, "Upgrading database from version %d to version %d.", old_version, NEW_VERSION);

      // Upgrade to 1
      if(old_version < 1)
      {
         db_query("CREATE TABLE database_version (v INT UNSIGNED NOT NULL ) ENGINE = InnoDB");
         db_query("INSERT INTO  database_version VALUES(0)");
         _log(GENERAL, "Created database table \"database_version\".");
      }

      // Upgrade to x
      // if(old_version < x)
      //    if(table_exists(y)  You must have this in case the table hasn't been created yet, e.g. on a brand new platform.
      //        ALTER TABLE

      sprintf(query, "UPDATE database_version SET v = %d", NEW_VERSION);
      db_query(query);
      _log(GENERAL, "Database upgraded to version %d.", NEW_VERSION);     
   }

   // Upgrade completed.  Now create any missing tables

   ///////////////////////////////////////////////////////////////////////////////////////////////////
   // corpus table
   if(!table_exists("corpus"))
   {
      db_query(
"CREATE TABLE corpus(                         "
"id INT(10) UNSIGNED NOT NULL AUTO_INCREMENT, "
"fn                     VARCHAR(255) NOT NULL,"
"stanox                 CHAR(16) NOT NULL,    "
"uic                    CHAR(16) NOT NULL,    "
"3alpha                 CHAR(16) NOT NULL,    "
"nlcdesc16              CHAR(30) NOT NULL,    "
"tiploc                 CHAR(16) NOT NULL,    "
"nlc                    CHAR(16) NOT NULL,    "
"nlcdesc                VARCHAR(255) NOT NULL,"
"PRIMARY KEY (id), INDEX(TIPLOC)              "
") ENGINE = InnoDB"
               );
      _log(GENERAL, "Created database table \"corpus\".");
   }

   ///////////////////////////////////////////////////////////////////////////////////////////////////
   // smart table
   if(caller == smartdb && !table_exists("smart"))
   {
      db_query(
"CREATE TABLE smart(                          "
"id INT(10) UNSIGNED NOT NULL AUTO_INCREMENT, "
"  `fromberth` char(4) NOT NULL,"
"  `td` char(2) NOT NULL,"
"  `stanox` int NOT NULL,"
"  `route` smallint NOT NULL,"
"  `steptype` char(1) NOT NULL,"
"  `toberth` char(4) NOT NULL,"
"  `toline` char(1) NOT NULL,"
"  `berthoffset` int NOT NULL,"
"  `platform` smallint NOT NULL,"
"  `event` char(1) NOT NULL,"
"  `comment` varchar(64) NOT NULL,"
"  `stanme` char(9) NOT NULL,"
"  `fromline` char(1) NOT NULL,"
"  PRIMARY KEY (id)"
") ENGINE = InnoDB DEFAULT CHARSET=latin1"
               );
      _log(GENERAL, "Created database table \"smart\".");
   }

   ///////////////////////////////////////////////////////////////////////////////////////////////////
   // Schedule tables
   if((caller == cifdb || caller == vstpdb || caller == trustdb) && !table_exists("updates_processed"))
   {
      db_query(
"CREATE TABLE updates_processed"
"("
"id SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT, "
"time INT UNSIGNED NOT NULL,"
"PRIMARY KEY (id) "
") ENGINE = InnoDB"
               );
      _log(GENERAL, "Created database table \"updates_processed\".");
   }

   if((caller == cifdb || caller == vstpdb || caller == trustdb) && !table_exists("cif_associations"))
   {
      db_query(
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
") ENGINE = InnoDB"
               );
      _log(GENERAL, "Created database table \"cif_associations\".");
   }

   if((caller == cifdb || caller == vstpdb || caller == trustdb) && !table_exists("cif_schedules"))
   {
      db_query(
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
"deduced_headcode              CHAR(4) NOT NULL DEFAULT '', "
"deduced_headcode_status       CHAR(1) NOT NULL DEFAULT '', "
"PRIMARY KEY (id), INDEX(schedule_end_date), INDEX(schedule_start_date), INDEX(CIF_train_uid), INDEX(CIF_stp_indicator) "
") ENGINE = InnoDB"
               );
      _log(GENERAL, "Created database table \"cif_schedules\".");
   }

   if((caller == cifdb || caller == vstpdb || caller == trustdb) && !table_exists("cif_schedule_locations"))
   {
      db_query(
"CREATE TABLE cif_schedule_locations             "
"(                                               "
"update_id                     SMALLINT UNSIGNED NOT NULL, "
"cif_schedule_id               INT UNSIGNED NOT NULL, "
"location_type                 CHAR(12) NOT NULL, " // MISNAMED!  contains activity
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
      _log(GENERAL, "Created database table \"cif_schedule_locations\".");
   }

   if((caller == cifdb || caller == vstpdb || caller == trustdb) && !table_exists("cif_changes_en_route"))
   {
      db_query(
"CREATE TABLE cif_changes_en_route               "
"(                                               "
"cif_schedule_id               INT UNSIGNED NOT NULL, "
"tiploc_code                   CHAR(7) NOT NULL, "
"tiploc_instance               CHAR(1) NOT NULL, "
"CIF_train_category            CHAR(2) NOT NULL, "
"signalling_id                 CHAR(4) NOT NULL, "
"CIF_headcode                  CHAR(4) NOT NULL, "
"CIF_train_service_code        CHAR(8) NOT NULL, "
// Portion ID not used
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
// Traction class Not used.
"uic_code                      CHAR(5) NOT NULL, "
"INDEX(cif_schedule_id), INDEX(tiploc_code)      "
") ENGINE = InnoDB"
               );
      _log(GENERAL, "Created database table \"cif_changes_en_route\".");
   }

   ///////////////////////////////////////////////////////////////////////////////////////////////////
   // trust tables
   if((caller == trustdb) && !table_exists("trust_activation"))
   {
      db_query(
"CREATE TABLE trust_activation "
"(created INT UNSIGNED NOT NULL, "
"trust_id VARCHAR(16) NOT NULL, "
"cif_schedule_id INT UNSIGNED NOT NULL, "
"deduced         TINYINT UNSIGNED NOT NULL, "
"INDEX(cif_schedule_id), INDEX(trust_id), INDEX(created)"
") ENGINE = InnoDB"
               );
      _log(GENERAL, "Created database table \"trust_activation\".");
   }
   if((caller == trustdb) && !table_exists("trust_cancellation"))
   {
      db_query(
"CREATE TABLE trust_cancellation "
"(created INT UNSIGNED NOT NULL, "
"trust_id VARCHAR(16) NOT NULL, "
"reason VARCHAR(8) NOT NULL, "
"type   VARCHAR(32) NOT NULL, "
"loc_stanox VARCHAR(8) NOT NULL, "
"reinstate TINYINT UNSIGNED NOT NULL, "
"INDEX(trust_id), INDEX(created) "
") ENGINE = InnoDB"
               );
      _log(GENERAL, "Created database table \"trust_cancellation\".");
   }

   if((caller == trustdb) && !table_exists("trust_movement"))
   {
      db_query(
"CREATE TABLE trust_movement "
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
      _log(GENERAL, "Created database table \"trust_movement\".");
   }

   if((caller == trustdb) && !table_exists("trust_changeorigin"))
   {
      db_query(
"CREATE TABLE trust_changeorigin "
"(created INT UNSIGNED NOT NULL, "
"trust_id VARCHAR(16) NOT NULL, "
"reason VARCHAR(8) NOT NULL, "
"loc_stanox VARCHAR(8) NOT NULL, "
"INDEX(trust_id), INDEX(created) "
") ENGINE = InnoDB"
               );
      _log(GENERAL, "Created database table \"trust_changeorigin\".");
   }

   if((caller == trustdb) && !table_exists("trust_changeid"))
   {
      db_query(
"CREATE TABLE trust_changeid "
"(created INT UNSIGNED NOT NULL, "
"trust_id VARCHAR(16) NOT NULL, "
"new_trust_id VARCHAR(16) NOT NULL, "
"INDEX(trust_id), INDEX(new_trust_id), INDEX(created) "
") ENGINE = InnoDB"
               );
      _log(GENERAL, "Created database table \"trust_changeid\".");
   }

   if((caller == vstpdb || caller == trustdb || caller == tddb) && !table_exists("status"))
   {
      db_query(
"CREATE TABLE status "
"(last_trust_processed INT UNSIGNED NOT NULL, "
"last_trust_actual     INT UNSIGNED NOT NULL, "
"last_vstp_processed   INT UNSIGNED NOT NULL, "
"last_td_processed     INT UNSIGNED NOT NULL  "
") ENGINE = InnoDB"
               );
      db_query(
"INSERT INTO status VALUES(0, 0, 0, 0)"
               );
      _log(GENERAL, "Created database table \"status\".");
   }

   if((caller == vstpdb || caller == trustdb || caller == tddb) && !table_exists("message_count"))
   {
      db_query(
"CREATE TABLE message_count "
"(application          CHAR(16) NOT NULL,     "
"time                  INT UNSIGNED NOT NULL, "
"count                 INT UNSIGNED NOT NULL, "
"INDEX(time)                                  "
") ENGINE = InnoDB"
               );
      _log(GENERAL, "Created database table \"message_count\".");
   }

   ///////////////////////////////////////////////////////////////////////////////////////////////////
   // td tables
   if(caller == tddb && !table_exists("td_updates"))
   {
      db_query(
"CREATE TABLE td_updates "
"(created INT UNSIGNED NOT NULL, "
"handle   INT UNSIGNED NOT NULL, "
"k        CHAR(8) NOT NULL, "
"v        CHAR(8) NOT NULL, "
"PRIMARY KEY(k) "
") ENGINE = InnoDB"
               );
      _log(GENERAL, "Created database table \"td_updates\".");
   }

   if((caller == tddb || caller == limed) && !table_exists("td_states"))
   {
      db_query(
"CREATE TABLE td_states "
"(updated INT UNSIGNED NOT NULL, "
"k        CHAR(8) NOT NULL, "
"v        CHAR(8) NOT NULL, "
"PRIMARY KEY(k) "
") ENGINE = InnoDB"
               );
      _log(GENERAL, "Created database table \"td_states\".");
   }

   if(caller == tddb && !table_exists("td_status"))
   {
      db_query(
"CREATE TABLE td_status "
"(d             CHAR(4) NOT NULL, "
"last_timestamp INT UNSIGNED NOT NULL "
") ENGINE = InnoDB"
               );
      _log(GENERAL, "Created database table \"td_status\".");
   }

   if(caller == tddb && !table_exists("friendly_names_20"))
   {
      db_query(
"CREATE TABLE friendly_names_20 "
"(tiploc CHAR(8) NOT NULL, "
"name    VARCHAR(32), "
"PRIMARY KEY (tiploc) "
") ENGINE = InnoDB"
               );
      _log(GENERAL, "Created database table \"friendly_names_20\".");
   }

   ///////////////////////////////////////////////////////////////////////////////////////////////////
   // Archive tables
   if(caller == archdb && !table_exists("trust_activation_arch"))
   {
      db_query(
"CREATE TABLE trust_activation_arch "
"(created INT UNSIGNED NOT NULL, "
"trust_id VARCHAR(16) NOT NULL, "
"cif_schedule_id INT UNSIGNED NOT NULL, "
"deduced         TINYINT UNSIGNED NOT NULL "
") ENGINE = InnoDB"
               );
      _log(GENERAL, "Created database table \"trust_activation_arch\".");
   } 

   if(caller == archdb && !table_exists("trust_cancellation_arch"))
   {
      db_query(
"CREATE TABLE trust_cancellation_arch "
"(created INT UNSIGNED NOT NULL, "
"trust_id VARCHAR(16) NOT NULL, "
"reason VARCHAR(8) NOT NULL, "
"type   VARCHAR(32) NOT NULL, "
"loc_stanox VARCHAR(8) NOT NULL, "
"reinstate TINYINT UNSIGNED NOT NULL "
") ENGINE = InnoDB"
               );
      _log(GENERAL, "Created database table \"trust_cancellation_arch\".");
   } 

   if(caller == archdb && !table_exists("trust_movement_arch"))
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
"next_report_run_time INT UNSIGNED NOT NULL "
") ENGINE = InnoDB"               );
      _log(GENERAL, "Created database table \"trust_movement_arch\".");
   } 

   if(caller == archdb && !table_exists("trust_changeorigin_arch"))
   {
      db_query(
"CREATE TABLE trust_changeorigin_arch "
"(created INT UNSIGNED NOT NULL, "
"trust_id VARCHAR(16) NOT NULL, "
"reason VARCHAR(8) NOT NULL, "
"loc_stanox VARCHAR(8) NOT NULL "
") ENGINE = InnoDB"
               );
      _log(GENERAL, "Created database table \"trust_changeorigin_arch\".");
   } 

   if(caller == archdb && !table_exists("trust_changeid_arch"))
   {
      db_query(
"CREATE TABLE trust_changeid_arch "
"(created INT UNSIGNED NOT NULL, "
"trust_id VARCHAR(16) NOT NULL, "
"new_trust_id VARCHAR(16) NOT NULL "
") ENGINE = InnoDB"
               );
      _log(GENERAL, "Created database table \"trust_changeid_arch\".");
   } 

   if(caller == archdb && !table_exists("cif_associations_arch"))
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
      _log(GENERAL, "Created database table \"cif_associations_arch\".");
   } 

   if(caller == archdb && !table_exists("cif_schedules_arch"))
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
"deduced_headcode              CHAR(4) NOT NULL DEFAULT '', "
"deduced_headcode_status       CHAR(1) NOT NULL DEFAULT ''  "
") ENGINE = InnoDB"
               );
      _log(GENERAL, "Created database table \"cif_schedules_arch\".");
   } 

   if(caller == archdb && !table_exists("cif_schedule_locations_arch"))
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
"performance_allowance         CHAR(2) NOT NULL "
") ENGINE = InnoDB"
               );
      _log(GENERAL, "Created database table \"cif_schedule_locations_arch\".");
   } 

   if(caller == archdb && !table_exists("cif_changes_en_route_arch"))
   {
      db_query(
"CREATE TABLE cif_changes_en_route_arch               "
"(                                               "
"cif_schedule_id               INT UNSIGNED NOT NULL, "
"tiploc_code                   CHAR(7) NOT NULL, "
"tiploc_instance               CHAR(1) NOT NULL, "
"CIF_train_category            CHAR(2) NOT NULL, "
"signalling_id                 CHAR(4) NOT NULL, "
"CIF_headcode                  CHAR(4) NOT NULL, "
"CIF_train_service_code        CHAR(8) NOT NULL, "
// Portion ID not used
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
// Traction class Not used.
"uic_code                      CHAR(5) NOT NULL "
") ENGINE = InnoDB"
               );
      _log(GENERAL, "Created database table \"cif_changes_en_route_arch\".");
   } 

   if((caller == trustdb || caller == tddb) && !table_exists("obfus_lookup"))
   {
      db_query(
"CREATE TABLE obfus_lookup                            "
"(                                                    "
"created                       INT UNSIGNED NOT NULL, "
"true_hc                       CHAR(4) NOT NULL,      "
"obfus_hc                      CHAR(4) NOT NULL       "
") ENGINE = InnoDB"
               );
      _log(GENERAL, "Created database table \"obfus_lookup\".");
   } 

   // Unlock the upgrade process
   if(!db_query("SELECT RELEASE_LOCK('openrail.database_upgrade')"))
   {
      res = db_store_result();
      mysql_free_result(res);
   }
   return result;
}

static word table_exists(const char * const table_like)
{
   MYSQL_RES * result;
   char query[256];
   sprintf(query, "SHOW TABLES LIKE '%s'", table_like);
   if(!db_query(query))
   {
      result = db_store_result();
      word num_rows = mysql_num_rows(result);
      mysql_free_result(result);
      if(num_rows) return true;
   }
   return false;
}
