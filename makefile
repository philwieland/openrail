CC=LD_LIBRARY_PATH=./lib gcc -c -g -O2 -Wall -I/usr/include/mysql -DBIG_JOINS=1 -D_GNU_SOURCE -D_FILE_OFFSET_BITS=64 -fno-strict-aliasing -DUNIV_LINUX -I./include -L./lib

all:            libopenrailconfig cifdb corpusdb vstpdb trustdb liverail.cgi service-report 

jsmn.o:		jsmn.c jsmn.h misc.h

stomp.o:        stomp.c stomp.h

db.o:           db.c misc.h db.h

misc.o:         misc.c misc.h

libopenrailconfig: 
		gcc -c -fpic libopenrailconfig.c
		ar rs lib/libopenrailconfig.a libopenrailconfig.o

cifdb:          cifdb.o jsmn.o misc.o db.o
		gcc -g -O2 -I./include -L./lib cifdb.o jsmn.o misc.o db.o -lmysqlclient -lcurl -o cifdb ./lib/libopenrailconfig.a

cifdb.o:	cifdb.c jsmn.h misc.h db.h

liverail.cgi:	liverail.o misc.o db.o
		gcc -g -O2 -I./include -L./lib liverail.o misc.o db.o -lmysqlclient -o liverail.cgi ./lib/libopenrailconfig.a

liverail.o:	liverail.c db.h misc.h

corpusdb:       corpusdb.o jsmn.o misc.o db.o
		gcc -g -O2 -L./lib -I./include corpusdb.o jsmn.o misc.o db.o -lcurl -lmysqlclient -o corpusdb ./lib/libopenrailconfig.a

corpusdb.o:     corpusdb.c misc.h db.h

vstpdb:         vstpdb.o jsmn.o stomp.o misc.o db.o
		gcc -g -O2 -L./lib -I./include vstpdb.o jsmn.o stomp.o misc.o db.o -lmysqlclient -o vstpdb ./lib/libopenrailconfig.a

vstpdb.o:       vstpdb.c jsmn.h stomp.h misc.h db.h

trustdb:        trustdb.o jsmn.o stomp.o misc.o db.o
		gcc -g -O2 -L./lib -I./include trustdb.o jsmn.o stomp.o misc.o db.o -lmysqlclient -o trustdb ./lib/libopenrailconfig.a

trustdb.o:      trustdb.c jsmn.h stomp.h misc.h db.h

service-report: service-report.o misc.o db.o
		gcc -g -O2 -L./lib -I./include service-report.o misc.o db.o -lmysqlclient -o service-report ./lib/libopenrailconfig.a

service-report.o: service-report.c misc.h db.h

clean:
		rm  cifdb liverail.cgi corpusdb vstpdb trustdb service-report *.o lib/libopenrailconfig.a


