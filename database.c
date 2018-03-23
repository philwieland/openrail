/*
    Copyright (C) 2015, 2016, 2017 Phil Wieland

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
#include "build.h"

#define NAME  "database"

#ifndef RELEASE_BUILD
#define BUILD "Z322p"
#else
#define BUILD RELEASE_BUILD
#endif
#define NEW_VERSION 6

static word table_exists(const char * const table_like);

word database_upgrade(const word caller)
{
   char query[256];
   MYSQL_RES * res;
   MYSQL_ROW row;
   word lock, result, old_version;
   word describers_upgrade = false;

   result = 0;

   // Lock the upgrade process.
   // NOTE - This DOES NOT lock the database for normal data processing or any other activity.
   // It merely prevents simultaneous database_upgrade processing.
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
      _log(GENERAL, "Database build %s:  Upgrading database from version %d to version %d.", BUILD, old_version, NEW_VERSION);

      // Upgrade to 1
      if(old_version < 1)
      {
         db_query("CREATE TABLE database_version (v INT UNSIGNED NOT NULL ) ENGINE = InnoDB");
         db_query("INSERT INTO  database_version VALUES(0)");
         _log(GENERAL, "Created database table \"database_version\".");
      }

      // Upgrade to 2
      if(old_version < 2)
      {
         if(table_exists("corpus"))
         {
            db_query("ALTER TABLE corpus ADD INDEX (stanox)");
            db_query("ALTER TABLE corpus ADD INDEX (3alpha)");
            db_query("ALTER TABLE corpus CHANGE COLUMN nlcdesc16 nlcdesc16 CHAR(32) NOT NULL");
            _log(GENERAL, "Upgraded database table \"corpus\".");
         }
      }

      // Upgrade to 3
      if(old_version < 3)
      {
         if(table_exists("td_status"))
         {
            db_query("DROP TABLE td_status");
            _log(GENERAL, "Dropped database table \"td_status\".");
            describers_upgrade = true;
         }
      }

      // Upgrade to 4
      if(old_version < 4)
      {
         if(table_exists("display_banner"))
         {
            db_query("ALTER TABLE display_banner ADD COLUMN expires INT UNSIGNED NOT NULL");
            _log(GENERAL, "Upgraded database table \"display_banner\".");
         }
      }

      // Upgrade to 5
      if(old_version < 5)
      {
         if(table_exists("describers"))
         {
            db_query("ALTER TABLE describers ADD COLUMN control_mode_cmd INT UNSIGNED NOT NULL AFTER last_timestamp");
            _log(GENERAL, "Upgraded database table \"describers\".");
         }
      }

      if(old_version < 6)
      {
         db_query("DROP TABLE IF EXISTS display_banner");
         if(table_exists("corpus"))
         {
            db_query("UPDATE corpus SET stanox = '0' WHERE stanox = ''");
            db_query("ALTER TABLE corpus CHANGE COLUMN stanox stanox int unsigned not null");
            _log(GENERAL, "Upgraded database table \"corpus\".");
         }
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
"id INT(10) UNSIGNED NOT NULL,                "
"fn                     VARCHAR(255) NOT NULL,"
"stanox                 INT UNSIGNED NOT NULL,"
"uic                    CHAR(16) NOT NULL,    "
"3alpha                 CHAR(16) NOT NULL,    "
"nlcdesc16              CHAR(32) NOT NULL,    "
"tiploc                 CHAR(16) NOT NULL,    "
"nlc                    CHAR(16) NOT NULL,    "
"nlcdesc                VARCHAR(255) NOT NULL,"
"PRIMARY KEY (id), INDEX(tiploc), INDEX(stanox), INDEX(3alpha)              "
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
"diagram_type            CHAR(1) NOT NULL,       "  /* This column stores Association Type, not Diagram Type. */
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

   if((caller == cifdb) && !table_exists("cif_tiplocs"))
   {
      db_query(
"CREATE TABLE cif_tiplocs"
"(                                               "
"update_id                     SMALLINT UNSIGNED NOT NULL,  "
"created                       INT UNSIGNED NOT NULL, "
"deleted                       INT UNSIGNED NOT NULL, "
"tiploc_code                   CHAR(7) NOT NULL, "
"capitals                      CHAR(2) NOT NULL, "
"nalco                         CHAR(6) NOT NULL, "
"nlc_check                     CHAR(1) NOT NULL, "
"tps_description               CHAR(26) NOT NULL, "
"stanox                        CHAR(5) NOT NULL, "
// PO MCP Code not used
"CRS                           CHAR(3) NOT NULL, "
"CAPRI_description             CHAR(16) NOT NULL, "
"INDEX(tiploc_code)      "
") ENGINE = InnoDB"
               );
      _log(GENERAL, "Created database table \"cif_tiplocs\".");
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
   if((caller == trustdb) && !table_exists("trust_activation_extra"))
   {
      db_query(
"CREATE TABLE trust_activation_extra "
"(created                       INT UNSIGNED NOT NULL, "
"trust_id                       CHAR(16) NOT NULL, "
"schedule_source                CHAR(1) NOT NULL, "
"train_file_address             CHAR(8) NOT NULL, "
"schedule_end_date              INT UNSIGNED NOT NULL, "
"tp_origin_timestamp            INT UNSIGNED NOT NULL, "
"creation_timestamp             INT UNSIGNED NOT NULL, "
"tp_origin_stanox               CHAR(8) NOT NULL, "
"origin_dep_timestamp           INT UNSIGNED NOT NULL, "
"train_service_code             CHAR(8) NOT NULL, "
"toc_id                         CHAR(2) NOT NULL, "
"d1266_record_number            CHAR(8) NOT NULL, "
"train_call_type                CHAR(16) NOT NULL, "
"train_uid                      CHAR(6) NOT NULL, "
"train_call_mode                CHAR(16) NOT NULL, "
"schedule_type                  CHAR(16) NOT NULL, "
"sched_origin_stanox            CHAR(8) NOT NULL, "
"schedule_wtt_id                CHAR(8) NOT NULL, "
"schedule_start_date            INT UNSIGNED NOT NULL, "
"INDEX(trust_id), INDEX(created)"
") ENGINE = InnoDB"
               );
      _log(GENERAL, "Created database table \"trust_activation_extra\".");
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
"platform            VARCHAR(8) NOT NULL, "
"loc_stanox          VARCHAR(8) NOT NULL, "
"actual_timestamp    INT UNSIGNED NOT NULL, "
"gbtt_timestamp      INT UNSIGNED NOT NULL, "
"planned_timestamp   INT UNSIGNED NOT NULL, "
"timetable_variation SMALLINT UNSIGNED NOT NULL, "
"next_report_stanox  VARCHAR(8) NOT NULL, "
"next_report_run_time SMALLINT UNSIGNED NOT NULL, "
"flags               SMALLINT UNSIGNED NOT NULL,  "
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

   if((caller == trustdb) && !table_exists("trust_changelocation"))
   {
      db_query(
"CREATE TABLE trust_changelocation "
"(created INT UNSIGNED NOT NULL, "
"trust_id VARCHAR(16) NOT NULL, "
"original_stanox VARCHAR(8) NOT NULL, "
"stanox VARCHAR(8) NOT NULL, "
"INDEX(trust_id), INDEX(created) "
") ENGINE = InnoDB"
               );
      _log(GENERAL, "Created database table \"trust_changelocation\".");
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

   if(caller == tddb && !table_exists("describers"))
   {
      db_query(
"CREATE TABLE describers "
"(id              CHAR(3) NOT NULL, "
"last_timestamp   INT UNSIGNED NOT NULL, "
"control_mode_cmd INT UNSIGNED NOT NULL, "
"control_mode     INT UNSIGNED NOT NULL, "
"no_sig_address   INT UNSIGNED NOT NULL, "
"process_mode     INT UNSIGNED NOT NULL, "
"description      VARCHAR(255) NOT NULL, "
"PRIMARY KEY(id) "
") ENGINE = InnoDB"
               );
      db_query("INSERT INTO describers (id,last_timestamp,control_mode_cmd,control_mode,no_sig_address,process_mode,description) VALUES ('', 0, 0, 0, 0, 0, 'Control record')");
      if(describers_upgrade)
      {
         // If this is an upgrade, provide the same functionality as was previously hard-coded into tddb.c
         db_query("INSERT INTO describers (id,last_timestamp,control_mode_cmd,control_mode,no_sig_address,process_mode,description) VALUES ('M1', 0, 0, 0,  8, 2, 'Liverpool WestCAD')");
         db_query("INSERT INTO describers (id,last_timestamp,control_mode_cmd,control_mode,no_sig_address,process_mode,description) VALUES ('XZ', 0, 0, 0,  0, 2, 'Liverpool Lime Street')");
         db_query("INSERT INTO describers (id,last_timestamp,control_mode_cmd,control_mode,no_sig_address,process_mode,description) VALUES ('WA', 0, 0, 0, 15, 2, 'Warrington PSB')");
      }
      _log(GENERAL, "Created database table \"describers\".");
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

   if((caller == archdb) && !table_exists("trust_activation_extra_arch"))
   {
      db_query(
"CREATE TABLE trust_activation_extra_arch "
"(created                       INT UNSIGNED NOT NULL, "
"trust_id                       CHAR(16) NOT NULL, "
"schedule_source                CHAR(1) NOT NULL, "
"train_file_address             CHAR(8) NOT NULL, "
"schedule_end_date              INT UNSIGNED NOT NULL, "
"tp_origin_timestamp            INT UNSIGNED NOT NULL, "
"creation_timestamp             INT UNSIGNED NOT NULL, "
"tp_origin_stanox               CHAR(8) NOT NULL, "
"origin_dep_timestamp           INT UNSIGNED NOT NULL, "
"train_service_code             CHAR(8) NOT NULL, "
"toc_id                         CHAR(2) NOT NULL, "
"d1266_record_number            CHAR(8) NOT NULL, "
"train_call_type                CHAR(16) NOT NULL, "
"train_uid                      CHAR(6) NOT NULL, "
"train_call_mode                CHAR(16) NOT NULL, "
"schedule_type                  CHAR(16) NOT NULL, "
"sched_origin_stanox            CHAR(8) NOT NULL, "
"schedule_wtt_id                CHAR(8) NOT NULL, "
"schedule_start_date            INT UNSIGNED NOT NULL "
") ENGINE = InnoDB"
               );
      _log(GENERAL, "Created database table \"trust_activation_extra_arch\".");
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
"platform            VARCHAR(8) NOT NULL, "
"loc_stanox          VARCHAR(8) NOT NULL, "
"actual_timestamp    INT UNSIGNED NOT NULL, "
"gbtt_timestamp      INT UNSIGNED NOT NULL, "
"planned_timestamp   INT UNSIGNED NOT NULL, "
"timetable_variation SMALLINT UNSIGNED NOT NULL, "
"next_report_stanox  VARCHAR(8) NOT NULL, "
"next_report_run_time SMALLINT UNSIGNED NOT NULL, "
"flags               SMALLINT UNSIGNED NOT NULL  "
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

   if((caller == archdb) && !table_exists("trust_changelocation_arch"))
   {
      db_query(
"CREATE TABLE trust_changelocation_arch "
"(created INT UNSIGNED NOT NULL, "
"trust_id VARCHAR(16) NOT NULL, "
"original_stanox VARCHAR(8) NOT NULL, "
"stanox VARCHAR(8) NOT NULL "
") ENGINE = InnoDB"
               );
      _log(GENERAL, "Created database table \"trust_changelocation_arch\".");
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

   if(!table_exists("banners"))
   {
      db_query(
"CREATE TABLE banners"
"("
"type                ENUM('diagram', 'webpage','diagram_s', 'webpage_s') NOT NULL,"
"banner              VARCHAR(512) NOT NULL,"
"banner1             CHAR(64) NOT NULL,"
"expires             INT UNSIGNED NOT NULL,"
"id                  INT UNSIGNED NOT NULL AUTO_INCREMENT,"
"PRIMARY KEY(id)"
") ENGINE = InnoDB"
               );
      db_query("INSERT INTO banners VALUES('diagram','','',0,0)");
      db_query("INSERT INTO banners VALUES('webpage','','',0,0)");
      _log(GENERAL, "Created database table \"banners\".");
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
