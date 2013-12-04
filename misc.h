#ifndef __MISC_H
#define __MISC_H
#include <time.h>

typedef unsigned long long qword;
typedef unsigned long      dword;
typedef unsigned short     word;
typedef unsigned char      byte;

#define false 0
#define true ~0

extern char * time_text(const time_t time, const byte local);
extern char * date_text(const time_t time, const byte local);
extern time_t parse_datestamp(const char * string);
extern time_t parse_timestamp(const char * string);

enum log_types {GENERAL, PROC, DEBUG, MINOR, MAJOR, CRITICAL};
extern void _log(const byte level, const char * text);
extern void _log_init(const char * log_file, const word debug);
extern char * commas(const dword n);
extern char * commas_ll(const unsigned long long int n);
extern char * show_spaces(const char * string);
extern word email_alert(const char * const name, const char * const build, const char * const title, const char * const message);
extern char * abbreviated_host_id(void);
extern char * show_time(const char * const input);
extern char * show_time_text(const char * const input);

#endif
