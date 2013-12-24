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

   Warning:  This is NOT a full implementation of the STOMP standard.
*/

#include <netdb.h>
#include <stdio.h>
#include <sys/types.h> 
#include <sys/socket.h>
#include <netinet/in.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include "misc.h"

static int sockfd;

int stomp_connect(const char * const dn, const int port)
{
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) 
    {
       return -1;
    }

    server = gethostbyname(dn);
    if (server == NULL) 
    {
       close(sockfd);
       sockfd = -1;
       return -2;
    }

    bzero((char *) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, 
          (char *)&serv_addr.sin_addr.s_addr,
          server->h_length);
    serv_addr.sin_port = htons(port);

    // Set a receive timeout
    struct timeval timeout;      
    timeout.tv_sec = 60;
    timeout.tv_usec = 0;
    if(setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, (char *)&timeout, sizeof(timeout)) < 0)
    {
       close(sockfd);
       sockfd = -1;
       return -6;
    }

    /* Now connect to the server */
    if (connect(sockfd, (const struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) 
    {
       close(sockfd);
       sockfd = -1;
       return errno;
    }	
    return 0;
}

int stomp_tx(const char * const headers)
{
   if(sockfd < 0) return -11;
   // Headers must already have the appropriate \n in and must be \0 terminated.
   ssize_t sent = write(sockfd, headers, strlen(headers) + 1);
   if(sent < 0) return errno;
   
   return 0;
}

int stomp_rx(char * headers, const size_t header_size, char * body, const size_t body_size)
{
   // Will return two null-terminated strings if successfull.  
   // WARNING:  If return code != 0, strings may be unterminated.
   if(sockfd < 0) return -11;

   word run = true;
   word index = 0;
   char rxchar[1];

   while(run)
   {
      ssize_t rc = read(sockfd, rxchar, 1);
      if(rc < 0) { if(errno) return errno; else return -8; }
      if(!rc) return -7; // End of file
      //printf("%c", rxchar[0]);
      if(index)
      {
         if(rxchar[0] == '\n' && headers[index - 1] == '\n')
         {
            headers[index] = '\0';
            run = false;
         }
         else
         {
            headers[index++] = rxchar[0];
         }
      }
      else
      {
         if(rxchar[0] != '\n')
         {
            headers[index++] = rxchar[0];
         }
         //else printf("Heartbeat %ld.\n", time(NULL));
      }
      if(index >= header_size) return -4;
   }

   run = true;
   index = 0;
   while(run)
   {
      ssize_t rc = read(sockfd, rxchar, 1);
      if(rc < 0) { if(errno) return errno; else return -9; }
      if(!rc) return -10; // End of file
      body[index++] = rxchar[0];
      if(!rxchar[0]) run = false;
      if(index >= body_size) return -5;
   }

   return 0;
}

int stomp_disconnect(void)
{
   // DOES NOT send a disconnect message.

   if(sockfd >= 0) close(sockfd);
   sockfd = -1;
   return 0;
}
