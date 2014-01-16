/*
 * libopenrailconfig.h:
 * routines in libopenrail.so
 */

extern void parse_config(char *buf);
typedef struct Conf {
    char* db_server;
    char* db_pass;
    char* db_name;
    char* db_user;
    char* nr_user;
    char* nr_pass;
    char* debug;
} conf_t;
conf_t conf;
