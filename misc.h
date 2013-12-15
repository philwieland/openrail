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
