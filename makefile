CC=gcc -c -g -O2 -Wall -I/usr/include/mysql -DBIG_JOINS=1 -D_GNU_SOURCE -D_FILE_OFFSET_BITS=64 -fno-strict-aliasing -DUNIV_LINUX

all:            cifdb archdb corpusdb vstpdb trustdb stompy tddb liverail.cgi livesig.cgi service-report livesiglog

jsmn.o:		jsmn.c jsmn.h misc.h

db.o:           db.c misc.h db.h

misc.o:         misc.c misc.h

cifdb:          cifdb.o jsmn.o misc.o db.o 
		gcc -g -O2 -I./include -L./lib cifdb.o jsmn.o misc.o db.o -lmysqlclient -lcurl -o cifdb

cifdb.o:	cifdb.c jsmn.h misc.h db.h

archdb:         archdb.o jsmn.o misc.o db.o 
		gcc -g -O2 -I./include -L./lib archdb.o jsmn.o misc.o db.o -lmysqlclient -lcurl -o archdb

archdb.o:	archdb.c jsmn.h misc.h db.h

liverail.cgi:	liverail.o misc.o db.o 
		gcc -g -O2 -I./include -L./lib liverail.o misc.o db.o -lmysqlclient -o liverail.cgi 

liverail.o:	liverail.c db.h misc.h

livesig.cgi:	livesig.o misc.o db.o 
		gcc -g -O2 -I./include -L./lib livesig.o misc.o db.o -lmysqlclient -o livesig.cgi 

livesig.o:	livesig.c db.h misc.h

corpusdb:       corpusdb.o jsmn.o misc.o db.o 
		gcc -g -O2 -L./lib -I./include corpusdb.o jsmn.o misc.o db.o -lcurl -lmysqlclient -o corpusdb 

corpusdb.o:     corpusdb.c misc.h db.h

vstpdb:         vstpdb.o jsmn.o misc.o db.o 
		gcc -g -O2 -L./lib -I./include vstpdb.o jsmn.o misc.o db.o -lmysqlclient -o vstpdb 

vstpdb.o:       vstpdb.c jsmn.h misc.h db.h

trustdb:        trustdb.o jsmn.o misc.o db.o 
		gcc -g -O2 -L./lib -I./include trustdb.o jsmn.o misc.o db.o -lmysqlclient -o trustdb 

trustdb.o:      trustdb.c jsmn.h misc.h db.h

tddb:       	tddb.o jsmn.o misc.o db.o 
		gcc -g -O2 -L./lib -I./include tddb.o jsmn.o misc.o db.o -lmysqlclient -o tddb 

tddb.o:      	tddb.c jsmn.h misc.h db.h

stompy:         stompy.o misc.o 
		gcc -g -O2 -L./lib -I./include stompy.o misc.o -o stompy 

stompy.o:      stompy.c misc.h

service-report: service-report.o misc.o db.o 
		gcc -g -O2 -L./lib -I./include service-report.o misc.o db.o -lmysqlclient -o service-report

service-report.o: service-report.c misc.h db.h

livesiglog:     livesiglog.o misc.o
		gcc -g -O2 -L./lib -I./include livesiglog.o misc.o -o livesiglog

livesiglog.o:   livesiglog.c misc.h

install:
		mkdir -p $(DESTDIR)$(prefix)/lib/cgi-bin
		install -m 0755 cifdb $(DESTDIR)$(prefix)/sbin
		install -m 0755 archdb $(DESTDIR)$(prefix)/sbin
		install -m 0755 trustdb $(DESTDIR)$(prefix)/sbin
		install -m 0755 vstpdb $(DESTDIR)$(prefix)/sbin
		install -m 0755 corpusdb $(DESTDIR)$(prefix)/sbin
		install -m 0755 stompy $(DESTDIR)$(prefix)/sbin
		install -m 0755 liverail.cgi $(DESTDIR)$(prefix)/lib/cgi-bin
		install -m 0644 liverail.css $(DESTDIR)/var/www
		install -m 0644 liverail.js $(DESTDIR)/var/www

.PHONY: install

clean:
		rm -f cifdb archdb liverail.cgi livesig.cgi corpusdb vstpdb trustdb service-report stompy tddb livesiglog *.o 


