#include "logging.h"
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdint.h>
#include <errno.h>
#include <signal.h>

int fd;
char pipe_name[256];
int messages_read = 0;

#define MESSAGE_SIZE 1025

void handler(){
    close(fd);
    unlink(pipe_name);
    fprintf(stdout, "Sub read %d messages\n", messages_read);
    exit(1);
}

int main(int argc, char **argv) {
    if(argc != 4){
        fprintf(stderr, "[SUBSCRIBER]: invalid number of arguments");
        return -1;
    }
    signal(SIGINT, handler);
    uint8_t code = 2;   
    ssize_t bytes_read;
    char request[289];
    strncpy(pipe_name, argv[2], 256);
    pipe_name[255] = '\0';
    memset(request,0,289);
    memcpy(request, &code, 1);
    memcpy(request + 1, pipe_name, 256);
    memcpy(request + 257, argv[3], 32);
    unlink(pipe_name);

    if(mkfifo(pipe_name, S_IRWXU) == -1){
        return -1;
    }

    fd = open(argv[1], O_WRONLY);
    if (fd == -1){
        unlink(pipe_name);
        return -1;
    }

    // request access to a box
    if(write(fd, request, sizeof(request)) == -1){
        close(fd);
        unlink(pipe_name);
        return -1;
    }
    close(fd);

    char message[MESSAGE_SIZE];
    memset(message, '\0', MESSAGE_SIZE);

    // Open FIFO for read only
    fd = open(pipe_name, O_RDONLY);
    if(fd == -1){
        unlink(pipe_name);
        return -1;
    }

    while(1){
        bytes_read = read(fd, message, MESSAGE_SIZE);
        if(message[0] != 10){
            close(fd);
            unlink(pipe_name);
            return -1;
        }
        fprintf(stdout, "%s\n", message+1);

        if(bytes_read <= 0){
            close(fd);
            unlink(pipe_name);
            return 0;
        }
        messages_read++;

    }
    close(fd);
    unlink(pipe_name);
    return 0;
}