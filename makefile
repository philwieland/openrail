
CC=gcc -c -g -O2 -Wall -I/usr/include/mysql -DBIG_JOINS=1 -D_GNU_SOURCE -D_FILE_OFFSET_BITS=64 -fno-strict-aliasing -DUNIV_LINUX


all:            cifdb corpusdb vstpdb trustdb liverail.cgi service-report

jsmn.o:		jsmn.c jsmn.h misc.h

stomp.o:        stomp.c stomp.h

db.o:           db.c misc.h db.h

misc.o:         misc.c misc.h

cifdb:          cifdb.o jsmn.o misc.o db.o
		gcc -g -O2 cifdb.o jsmn.o misc.o db.o -lmysqlclient -lcurl -o cifdb

cifdb.o:	cifdb.c jsmn.h misc.h db.h private.h

liverail.cgi:	liverail.o misc.o db.o
		gcc -g -O2 liverail.o misc.o db.o -lmysqlclient -o liverail.cgi

liverail.o:	liverail.c db.h misc.h private.h

#timetable.cgi:	timetable.o misc.o db.o
#		gcc -g -O2 timetable.o misc.o db.o -lmysqlclient -o timetable.cgi

#timetable.o:	timetable.c db.h misc.h

#timetable_live.cgi:	timetable_live.o misc.o db.o
#			gcc -g -O2 timetable_live.o misc.o db.o -lmysqlclient -o timetable_live.cgi

#timetable_live.o:	timetable.c db.h misc.h
#			gcc -c -g -O2 -Wall -I/usr/include/mysql -DTIMETABLE_LIVE -DBIG_JOINS=1 -D_GNU_SOURCE -D_FILE_OFFSET_BITS=64 -fno-strict-aliasing -DUNIV_LINUX timetable.c -o timetable_live.o

corpusdb:       corpusdb.o jsmn.o misc.o db.o
		gcc -g -O2 corpusdb.o jsmn.o misc.o db.o -lcurl -lmysqlclient -o corpusdb

corpusdb.o:     corpusdb.c misc.h db.h private.h

vstpdb:         vstpdb.o jsmn.o stomp.o misc.o db.o
		gcc -g -O2 vstpdb.o jsmn.o stomp.o misc.o db.o -lmysqlclient -o vstpdb

vstpdb.o:       vstpdb.c jsmn.h stomp.h misc.h db.h private.h

trustdb:        trustdb.o jsmn.o stomp.o misc.o db.o
		gcc -g -O2 trustdb.o jsmn.o stomp.o misc.o db.o -lmysqlclient -o trustdb

trustdb.o:      trustdb.c jsmn.h stomp.h misc.h db.h private.h

service-report: service-report.o misc.o db.o
		gcc -g -O2 service-report.o misc.o db.o -lmysqlclient -o service-report

service-report.o: service-report.c misc.h db.h private.h

clean:
		rm  cifdb liverail.cgi corpusdb vstpdb trustdb service-report *.o


