#include <stdio.h>
#include <string.h>

typedef struct Conf {
    char* db_server;
    char* db_name;
    char* db_pass;
    char* db_user;
    char* nr_user;
    char* nr_pass;
    char* debug;
} conf_t;
conf_t conf;

void parse_config(char *buf) {
    char *line_start, *val_start, *val_end, *line_end, *ic;
    buf[1024] = 0;

    line_start=buf;
    while(1) {
        ic = strchr(line_start, ':');
        line_end = strchr(line_start, '\n');

        // config is finished
        if (line_end == NULL || ic == NULL)
            return;

        val_start = ic;
        val_end = line_end;
        while (*(--ic) == ' ');
        *(++ic) = 0;
        while (*(++val_start) == ' ');
        while (*(--val_end) == ' ');
        *(++val_end) = 0;

        if (*val_start == '"' && *(val_end-1) == '"') {
            val_start++;
            val_end--;
            *val_end=0;
        }

        if (strcmp(line_start, "db_server") == 0)
            conf.db_server = val_start;
        else if (strcmp(line_start, "db_name") == 0)
            conf.db_name = val_start;
        else if (strcmp(line_start, "db_username") == 0)
            conf.db_user = val_start;
        else if (strcmp(line_start, "db_password") == 0)
            conf.db_pass = val_start;
        else if (strcmp(line_start, "nr_user") == 0)
            conf.nr_user = val_start;
        else if (strcmp(line_start, "nr_pass") == 0)
            conf.nr_pass = val_start;
        else if (strcmp(line_start, "debug") == 0)
            conf.debug = val_start;

        line_start = line_end+1;
    }
}
