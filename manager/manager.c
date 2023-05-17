#include "logging.h"
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdint.h>


#define INITIAL_CAPACITY 8

// static void print_usage() {
//     fprintf(stderr, "usage: \n"
//                     "   manager <register_pipe_name> create <box_name>\n"
//                     "   manager <register_pipe_name> remove <box_name>\n"
//                     "   manager <register_pipe_name> list\n");
// }

int cmp(const void *a, const void *b) {
    return strcmp((char*)a, (char*)b);
}

int main(int argc, char **argv) {

    if(argc == 5){
        int fd;
        char request[289];
        char response[1029];
        char register_pipe_name[256], manager_pipe_name[256], box_name[32];
        memcpy(register_pipe_name, argv[1], 256);
        register_pipe_name[255] = '\0';
        memcpy(manager_pipe_name, argv[2], 256);
        manager_pipe_name[255] = '\0';
        memcpy(box_name, argv[4], 32);
        box_name[31] = '\0';
        uint8_t code;
        int32_t ret_code;
        if(!strcmp(argv[3], "create")){
            code = 3;
        }
        else if(!strcmp(argv[3], "remove")){
            code = 5;
        }

        else{
            fprintf(stderr, "[MANAGER]: argv[3] has to be either \"create\" or \"remove\"\n");
            return -1;
        }

        memset(request,0,289);
        memcpy(request, &code, 1);
        memcpy(request + 1, manager_pipe_name, 256);
        memcpy(request + 257, box_name, 32);
        unlink(manager_pipe_name);

        // create FIFO to receive the answer
        if(mkfifo(manager_pipe_name, S_IRWXU) == -1){
            return -1;
        }
        fd = open(register_pipe_name, O_WRONLY);
        if (fd == -1){
            return -1;
        }
        // request box creation/removal
        if(write(fd, request, sizeof(request)) == -1){
            return -1;
        }
        close(fd);
        // receive response to the request
        fd = open(manager_pipe_name, O_RDONLY);
        if (fd == -1){
            return -1;
        }
        if(read(fd, response, 1029) == -1){
            close(fd);
            return -1;
        }

        close(fd);
        memcpy(&ret_code, response+1, 4);
        if(ret_code == 0){
            fprintf(stdout, "OK\n");
        }
        else{
            char error_message[1024];
            memcpy(error_message, response+5, 1024);
            fprintf(stdout, "ERROR %s\n", error_message);
        }
    }
    else if(argc == 4){
        if(strcmp(argv[3], "list")){
            fprintf(stderr, "[MANAGER]: argv[3] has to be \"list\"\n");
            return -1;
        }
        uint8_t last;
        int fd;
        char request[257], response[58], box_name[32], manager_pipe_name[256], register_pipe_name[256];
        uint8_t code = 7;
        uint64_t box_size, n_publishers, n_subscribers;
        ssize_t bytes_read;
        char (*box_array)[58] = malloc(INITIAL_CAPACITY * sizeof(*box_array));
        size_t capacity = INITIAL_CAPACITY;
        size_t box_count = 0;
        memcpy(manager_pipe_name, argv[2], 256);
        manager_pipe_name[255] = '\0';
        memcpy(register_pipe_name, argv[1], 256);
        register_pipe_name[255] = '\0';
        memset(request, 0, 257);
        memcpy(request, &code, 1);
        memcpy(request + 1, manager_pipe_name, 256);

        // create FIFO to receive the answer
        // cria fifo do manager
        unlink(manager_pipe_name);
        if(mkfifo(manager_pipe_name, S_IRWXU) == -1){
            return -1;
        }

        // abre o fifo do mbroker
        fd = open(register_pipe_name, O_WRONLY);
        if (fd == -1){
            return -1;
        }

        // request box listing
        if(write(fd, request, sizeof(request)) == -1){
            return -1;
        }
        close(fd);
        
        fd = open(manager_pipe_name, O_RDONLY);
        if (fd == -1){
            return -1;
        }
        while(1) {
            bytes_read = read(fd, response, 58);
            if(bytes_read == -1) {
                free(box_array);
                return -1;
            }

            if(bytes_read == 0) {
                if(box_count == 0){
                    fprintf(stdout, "NO BOXES FOUND\n");
                }                
                break;
            }

            memcpy(&last, response + 1, 1);
            memcpy(&box_name, response + 2, 32);
            box_name[31] = '\0';
            memcpy(&box_size, response + 34, 8);
            memcpy(&n_publishers, response + 42, 8);
            memcpy(&n_subscribers, response + 50, 8);

            sprintf(box_array[box_count], "%s %zu %ld %ld", box_name, box_size, n_publishers, n_subscribers);
            box_count++;

            if (box_count == capacity) {
                capacity += 8;
                box_array = realloc(box_array, capacity * sizeof(*box_array));
            }

            if (last) break;
        }

        qsort(box_array, box_count, sizeof(char[58]), cmp);

        for (int i = 0; i < box_count; i++) {
            printf("%s\n", box_array[i]);
        }
        free(box_array);

    }
    else{
        fprintf(stderr, "[MANAGER]: invalid number of arguments");
        return -1;
    }
    // print_usage();
    // WARN("unimplemented"); // TODO: implement
    return 0;
}
