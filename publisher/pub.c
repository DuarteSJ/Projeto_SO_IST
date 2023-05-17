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

#define ARR_SIZE 1025

int main(int argc, char **argv) {
    if(argc != 4){
        fprintf(stderr, "[PUBLISHER]: invalid number of arguments");
        return -1;
    }
    // disable SIGPIPE
    signal(SIGPIPE, SIG_IGN);
    int fd;
    char pup_pipe_name[256], mbroker_pipe_name[256];
    memcpy(mbroker_pipe_name, argv[1], 256);
    mbroker_pipe_name[255] = '\0';
    memcpy(pup_pipe_name, argv[2], 256);
    pup_pipe_name[255] = '\0';
    char request[289];
    uint8_t code = 1;
    memset(request, '\0', 289);
    memcpy(request, &code, 1);
    memcpy(request + 1, pup_pipe_name, 256);
    memcpy(request + 257, argv[3], 32);
    request[288] = '\0';
    unlink(pup_pipe_name);

    // criar um fifo com o nome pup_pipe_name (path)
    if(mkfifo(pup_pipe_name, S_IRWXU) == -1){
        unlink(pup_pipe_name);
        return -1;
    }

    // tenta ligar ao fifo do mbroker
    fd = open(mbroker_pipe_name, O_WRONLY);
    if (fd == -1){
        unlink(pup_pipe_name);
        return -1;
    }

    // request access to a box

    // no fifo do mbroker, escrevo o request
    if(write(fd, request, sizeof(request)) == -1){
        unlink(pup_pipe_name);
        return -1;
    }
    close(fd);

    char arr[ARR_SIZE];
    memset(arr, '\0', ARR_SIZE);
    arr[0] = (uint8_t)9;
    // Open FIFO for write only
    fd = open(pup_pipe_name, O_WRONLY);
    if (fd == -1){
        unlink(pup_pipe_name);
        return -1;
    }

    while(1){
        // Take an input arr from user.
        if(fgets(arr+1, ARR_SIZE-1, stdin) == NULL){
            if(feof(stdin)){
                fprintf(stderr, "[EOF]: closed pub_fifo and terminated publisher process\n");
                close(fd);
                unlink(pup_pipe_name);
                return 0;
            }
            close(fd);
            unlink(pup_pipe_name);
            return -1;
        }
        int c;
        // if not all input was read, discard the rest of the input
        if(strchr(arr+1, '\n') == NULL){
            while ((c = fgetc(stdin)) != '\n' && c != EOF);
        }
        // set the last element to \0 if it isnt already
        if(arr[ARR_SIZE-1] != '\0'){
            arr[ARR_SIZE-1] = '\0';
        }
        // if there is a \n change it to \0
        else if(arr[strlen(arr)-1] == '\n'){
            arr[strlen(arr)-1] = '\0';
        }
        // Write the input arr on FIFO
        if(write(fd, arr, ARR_SIZE) == -1){
            if (errno == EPIPE) {
                close(fd);
                unlink(pup_pipe_name);
                return 0;
            } else {
                unlink(pup_pipe_name);
                close(fd);
                return -1;
            }
        }
    }
    // close the FIFO
    close(fd);
    unlink(pup_pipe_name);

    // print_usage();
    return 0;
}
