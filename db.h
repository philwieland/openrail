#include <mysql.h>
extern word db_init(const char * const server, const char * const user, const char * const password, const char * const database);
extern word db_query(const char * const query);
extern MYSQL_RES * db_store_result(void);
extern word db_row_count(void);
extern word db_connect(void);
extern void db_disconnect(void);

extern void dump_mysql_result_query(const char * const query);
extern void dump_mysql_result(MYSQL_RES * result);
extern dword db_insert_id(void);
extern dword db_real_escape_string(char * to, char* from, size_t size);
