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

#include <mysql.h>

#define DB_MODE_NORMAL     0
#define DB_MODE_FOUND_ROWS 0x0001

extern word db_errored;

extern word db_init(const char * const s, const char * const u, const char * const p, const char * const d, const word f);
extern word db_query(const char * const query);
extern MYSQL_RES * db_store_result(void);
extern MYSQL_RES * db_use_result(void);
extern qword db_affected_rows(void);
extern word db_row_count(void);
extern word db_connect(void);
extern void db_disconnect(void);

extern void dump_mysql_result_query(const char * const query);
extern void dump_mysql_result(MYSQL_RES * result);
extern dword db_insert_id(void);
extern word db_real_escape_string(char * to, const char * const from, const size_t size);
extern word db_start_transaction(void);
extern word db_commit_transaction(void);
extern word db_rollback_transaction(void);
