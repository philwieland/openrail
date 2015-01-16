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

#ifndef __MISC_H
#define __MISC_H
#include <time.h>
#include <regex.h>

typedef unsigned long long qword;
typedef unsigned long      dword;
typedef unsigned short     word;
typedef unsigned char      byte;
typedef struct Conf {
   char * db_server;
   char * db_pass;
   char * db_name;
   char * db_user;
   char * nr_user;
   char * nr_pass;
   char * debug;
   char * report_email;
   char * stomp_topics;
   char * stomp_topic_names;
   char * stomp_topic_log;
} conf_t;
extern conf_t conf;

#define false 0
#define true ~0
enum log_types {GENERAL, PROC, DEBUG, MINOR, MAJOR, CRITICAL};

extern char * time_text(const time_t time, const byte local);
extern char * day_date_text(const time_t time, const byte local);
extern char * date_text(const time_t time, const byte local);
extern time_t parse_datestamp(const char * string);
extern time_t parse_timestamp(const char * string);
extern void _log(const byte level, const char * text, ...);
extern void _log_init(const char * log_file, const word debug);
extern char * commas(const dword n);
extern char * commas_q(const qword n);
extern char * show_spaces(const char * string);
extern word email_alert(const char * const name, const char * const build, const char * const title, const char * const message);
extern char * abbreviated_host_id(void);
extern char * show_time(const char * const input);
extern char * show_time_text(const char * const input);
extern int load_config(const char * const filepath);
extern qword time_ms(void);
extern qword time_us(void);
extern ssize_t read_all(const int socket, void * buffer, const size_t size);
extern word open_stompy(const word port);
extern word read_stompy(void * buffer, const size_t max_size, const word seconds);
extern word ack_stompy(void);
extern void close_stompy(void);
extern void extract_match(const char * const source, const regmatch_t * const matches, const unsigned int match, char * result, const size_t max_length);

#endif
