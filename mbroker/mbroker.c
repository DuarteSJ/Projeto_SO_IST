#include "logging.h"
#include "producer-consumer.h"
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdint.h>
#include <errno.h>
#include <operations.h>
#include <pthread.h>
#include <signal.h>

#define BUFFER_SIZE 290
#define BLOCK_SIZE 1000000
#define INITIAL_MAX_BOXES 8

#define BOX_CREATION_FAILED "[SERVER]: Failed to create box"
#define BOX_REMOVAL_FAILED "[SERVER]: Failed to remove box"
#define PUB_REGISTRATION_FAILED "[SERVER]: Failed to register publisher"
#define SUB_REGISTRATION_FAILED "[SERVER]: Failed to register subscriber"
#define BOX_LISTING_FAILED "[SERVER]: Failed to list boxes"

#define BOX_CREATION_SUCCEEDED "[SERVER]: Succeeded to create box"
#define BOX_REMOVAL_SUCCEEDED "[SERVER]: Succeeded to remove box"
#define PUB_REGISTRATION_SUCCEEDED "[SERVER]: Succeeded to register publisher"
#define SUB_REGISTRATION_SUCCEEDED "[SERVER]: Succeeded to register subscriber"
#define BOX_LISTING_SUCCEEDED "[SERVER]: Succeeded to list boxes"

pc_queue_t requests;
pthread_mutex_t boxes_lock;
pthread_mutex_t request_buffer_lock;

typedef struct Box {
    char name[32];
    uint64_t has_publisher;
    uint64_t subscriber_count;
    ssize_t size;
    pthread_cond_t box_condvar;
    pthread_mutex_t box_lock;
} Box;

size_t max_boxes;
size_t boxes_count = 0;
Box* boxes;

int boxes_init(size_t initial_max){
  if(pthread_mutex_init(&boxes_lock, NULL)==-1){
    return -1;
  }
  max_boxes = initial_max;
  boxes = (Box*) malloc(sizeof(Box) * initial_max);
  return 0;
}

int find_box(const char* name) {
    // fprintf(stderr, "pi");
    // if(pthread_mutex_lock(&boxes_lock)==-1){
    //   fprintf(stderr, "kasjdfvjkgfvk");
    //   return -1;
    // }
    // fprintf(stderr, "kasjdfvjkgfvk");
    for (int i = 0; i < boxes_count; i++) {
        if (strcmp(boxes[i].name, name) == 0) {
            // if(pthread_mutex_unlock(&boxes_lock)==-1){
            //   return -1;
            // }
            return i;
        }
    }
    // if(pthread_mutex_unlock(&boxes_lock)==-1){
    //   return -1;
    // }
    return -1;
}

int open_box(const char* name, tfs_file_mode_t mode) {
    char path[34];
    int index = find_box(name);
    int fh = 0;
    if((index == -1) && (mode != TFS_O_CREAT)){
        return -1;
    }

    if((index != -1) && (mode == TFS_O_CREAT)){
        return -1;
    }

    path[0] = '/';
    memcpy(path + 1, name, 32);
    if((fh = tfs_open(path, mode)) == -1){
      return -1;
    }

    return fh;
}

int create_box(const char* name) {
    if(find_box(name) != -1){
        return -1;
    }

    if (boxes_count == max_boxes) {
        if(pthread_mutex_lock(&boxes_lock)==-1){
          return -1;
        }
        max_boxes += 8;
        boxes = (Box*) realloc(boxes, sizeof(Box) * max_boxes);
        if(pthread_mutex_unlock(&boxes_lock)==-1){
          return -1;
        }
    }

    if(pthread_mutex_lock(&boxes_lock)==-1){
      return -1;
    }
    memcpy(boxes[boxes_count].name, name, 32);
    boxes[boxes_count].name[31] = '\0';
    boxes[boxes_count].has_publisher = 0;
    boxes[boxes_count].subscriber_count = 0;
    boxes[boxes_count].size = 0;
    if(pthread_cond_init(&boxes[boxes_count].box_condvar, NULL)==-1){
      return -1;
    }
    if(pthread_mutex_init(&boxes[boxes_count].box_lock, NULL)==-1){
      return -1;
    }

    if(open_box(name, TFS_O_CREAT) == -1){
      if(pthread_mutex_unlock(&boxes_lock)==-1){
        return -1;
      }
      return -1;
    }

    boxes_count++;
    if(pthread_mutex_unlock(&boxes_lock)==-1){
      return -1;
    }
    return 0;
}

int add_publisher(const char* name) {
    int index = find_box(name);
    
    if (index == -1) {
      fprintf(stderr, "Error: Box with name '%s' not found\n", name);
        return -1;
    }
    if(pthread_mutex_lock(&boxes[index].box_lock)==-1){
      return -1;
    }
    if (boxes[index].has_publisher) {
        fprintf(stderr, "Error: Box already has a publisher\n");
        if(pthread_mutex_unlock(&boxes[index].box_lock)==-1){
          return -1;
        }
        return -1;
    }

    boxes[index].has_publisher = 1;
    if(pthread_mutex_unlock(&boxes[index].box_lock)==-1){
      return -1;
    }
    return 0;
}

int add_subscriber(const char* name) {
    int index = find_box(name);
    
    if (index == -1) {
        fprintf(stderr, "Error: Box with name '%s' not found\n", name);
        return -1;
    }
    if(pthread_mutex_lock(&boxes[index].box_lock) == -1){
      return -1;
    }
    boxes[index].subscriber_count += 1;
    if(pthread_mutex_unlock(&boxes[index].box_lock)==-1){
      return -1;
    }
    return 0;
}

int remove_publisher(const char* name) {
    int index = find_box(name);
    
    if (index == -1) {
        fprintf(stderr, "Error: Box with name '%s' not found\n", name);
        return -1;
    }
    if(pthread_mutex_lock(&boxes[index].box_lock)==-1){
      return -1;
    }
    if (!boxes[index].has_publisher) {
        fprintf(stderr, "Error: Box has no publisher\n");
        if(pthread_mutex_unlock(&boxes[index].box_lock)==-1){
          return -1;
        }
        return -1;
    }

    boxes[index].has_publisher = 0;
    if(pthread_mutex_unlock(&boxes[index].box_lock)==-1){
      return -1;
    }
    return 0;
}

int remove_subscriber(const char* name) {
    int index = find_box(name);
    
    if (index == -1) {
        fprintf(stderr, "Error: Box with name '%s' not found\n", name);
        return -1;
    }
    if(pthread_mutex_lock(&boxes[index].box_lock)==-1){
      return -1;
    }
    if (!boxes[index].subscriber_count) {
        fprintf(stderr, "Error: Box has no subscribers\n");
        if(pthread_mutex_unlock(&boxes[index].box_lock)==-1){
          return -1;
        }
        return -1;
    }
    boxes[index].subscriber_count -= 1;
    if(pthread_mutex_unlock(&boxes[index].box_lock)==-1){
      return -1;
    }
    return 0;
}

int remove_box(const char* name) {
    char path[strlen(name) + 2];
    int index = find_box(name);

    if (index == -1) {
        return -1;
    }
    if(pthread_mutex_lock(&boxes[index].box_lock)==-1){
        return -1;
    }
    // Can`t delete boxes that have publishers or subscribers
    if(boxes[index].has_publisher || boxes[index].subscriber_count){
        if(pthread_mutex_unlock(&boxes[index].box_lock)==-1){
            return -1;
        }
        return -1;
    }
    if(pthread_cond_destroy(&boxes[index].box_condvar)==-1){
        if(pthread_mutex_unlock(&boxes[index].box_lock)==-1){
            return -1;
        }
        return -1;
    }
    if(pthread_mutex_unlock(&boxes[index].box_lock)==-1){
        return -1;
    }
    if(pthread_mutex_destroy(&boxes[index].box_lock)==-1){
        return -1;
    }
    if(pthread_mutex_lock(&boxes_lock)==-1){
      return -1;
    }
    // shift all the boxes one position to the left
    for (int i = index; i < boxes_count - 1; i++) {
        boxes[i] = boxes[i + 1];
    }
    boxes_count--;
    if(pthread_mutex_unlock(&boxes_lock)==-1){
      return -1;
    }
    path[0] = '/';
    strcpy(path + 1, name);
    if(tfs_unlink(path) == -1){
      return -1;
    }
    return 0;
}

ssize_t write_to_box(const char* name, const int fd, char* buffer, size_t len){
  ssize_t bytes_written;

  int index = find_box(name);
  if(index == -1){
    return -1;
  }
  bytes_written = tfs_write(fd, buffer, len);
  if(bytes_written == -1){
    return -1;
  }
  if(pthread_mutex_lock(&boxes[index].box_lock) == -1){
    return -1;
  }
  boxes[index].size += 1024;
  pthread_cond_broadcast(&boxes[index].box_condvar);
  if(pthread_mutex_unlock(&boxes[index].box_lock) == -1){
    return -1;
  }
  return bytes_written;
}

ssize_t read_from_box(const char* name, const int fd, char* buffer, size_t len){
  ssize_t bytes_read=0;
  int index = find_box(name);
  if(index == -1){
    return -1;
  }
  if(pthread_mutex_lock(&boxes[index].box_lock)==-1){
      return -1;
  }
  while((bytes_read = tfs_read(fd, buffer, len)) == 0){
      if (pthread_cond_wait(&boxes[index].box_condvar, &boxes[index].box_lock)==-1){
        return -1;
      }
  }
  if (pthread_mutex_unlock(&boxes[index].box_lock)==-1){
    return -1;
  }

  if (bytes_read == -1){
    return -1;
  }
  return bytes_read;
}

uint64_t doesnt_exist_or_has_publisher(const char* name) {
    int index = find_box(name);

    if (index == -1) {
        return 1;
    }
    if (pthread_mutex_lock(&boxes[index].box_lock)==-1){
        exit(1);
    }
    uint64_t has_pub = boxes[index].has_publisher;
    if (pthread_mutex_unlock(&boxes[index].box_lock)==-1){
        exit(1);
    }
    return has_pub;
}

void sigpipe_handler(int sig){
    if(sig == SIGPIPE){
        if(signal(SIGPIPE, sigpipe_handler) == SIG_ERR){
            raise(SIGINT);
        }
        return;
    }
}

int handle_publisher(char* buffer){
    char pub_pipe_name[256], box_name[32], message[1025];
    memset(pub_pipe_name, '\0', 256);
    memset(box_name, '\0', 32);
    memset(message, '\0', 1025);
    
    if (pthread_mutex_lock(&request_buffer_lock) == -1){
      return -1;
    }

    memcpy(pub_pipe_name, buffer, 256);
    memcpy(box_name, buffer + 256, 32);

    if (pthread_mutex_unlock(&request_buffer_lock) == -1){
      return -1;
    }

    pub_pipe_name[255] = '\0';
    box_name[31] = '\0';
    int pub_fd, box_fd;
    ssize_t written, nread;

    // check to see if the box exists and has no publisher
    if (doesnt_exist_or_has_publisher(box_name)){
      // abro o pipe do publisher
      pub_fd = open(pub_pipe_name, O_RDONLY);
      if(pub_fd == -1){
        unlink(pub_pipe_name);
        return -1;
      }
      close(pub_fd);
      //unlink?
      fprintf(stderr, "[ERR]: BOX NOT AVAILABLE");
      return 0;
    }
    // Open FIFO for Read only
    add_publisher(box_name);
    pub_fd = open(pub_pipe_name, O_RDONLY);
    if(pub_fd == -1){
      remove_publisher(box_name);
      unlink(pub_pipe_name);
      return -1;
    }

    box_fd = open_box(box_name, TFS_O_APPEND);
    if(box_fd == -1){
      remove_publisher(box_name);
      close(pub_fd);
      unlink(pub_pipe_name);
      return -1;
    }

    while(1){
      // Read from FIFO
      nread = read(pub_fd, message, 1025);
      if(nread == -1){
        remove_publisher(box_name);
        close(pub_fd);
        unlink(pub_pipe_name);
        return -1;
      }
      // check if publisher still exists
      else if(nread == 0){
        remove_publisher(box_name);
        close(pub_fd);
        unlink(pub_pipe_name);
        break;
      }
      if(message[0] != 9){
        remove_publisher(box_name);
        close(pub_fd);
        unlink(pub_pipe_name);
      }

      // write to the box
      written = write_to_box(box_name, box_fd, message+1, (size_t)1024);
      if(written == -1){
        close(pub_fd);
        unlink(pub_pipe_name);
        return -1;
      }

    }
    tfs_close(box_fd);
    return 0;
}

int handle_subscriber(char* buffer){
    signal(SIGPIPE, sigpipe_handler);
    char sub_pipe_name[256], box_name[32], message[1025];
    memset(sub_pipe_name, '\0', 256);
    memset(box_name, '\0', 32);
    memset(message, '\0', 1025);
    message[0] = (uint8_t)10;
    int sub_fd, box_fd;
    ssize_t bytes_read, bytes_written;
    if (pthread_mutex_lock(&request_buffer_lock) == -1){
      return -1;
    }
    memcpy(sub_pipe_name, buffer, 256);
    memcpy(box_name, buffer + 256, 32);

    if (pthread_mutex_unlock(&request_buffer_lock) == -1){
      return -1;
    }
    sub_pipe_name[255] = '\0';
    box_name[31] = '\0';

    // check to see if the box exists
    if(find_box(box_name) == -1){
      sub_fd = open(sub_pipe_name, O_WRONLY);
      if(sub_fd == -1){
          unlink(sub_pipe_name);
          return -1;
      }
      if(close(sub_fd) == -1){
        return -1;
      }
      fprintf(stderr, "[SERVER]: subscriber could not connect to box");
      return 0;
    }
    // Open FIFO for Read only
    add_subscriber(box_name);

    sub_fd = open(sub_pipe_name, O_WRONLY);
    if(sub_fd == -1){
        remove_subscriber(box_name);
        unlink(sub_pipe_name);
        return -1;
    }

    box_fd = open_box(box_name, 0);
    if(box_fd == -1){
        remove_subscriber(box_name);
        close(sub_fd);
        unlink(sub_pipe_name);
        return -1;
    }

    while(1){
      bytes_read = read_from_box(box_name, box_fd, message+1, sizeof(message)-1);
      // write the message to the subscriber
      if (bytes_read == -1){
        close(sub_fd);
        unlink(sub_pipe_name);
        remove_subscriber(box_name);
        tfs_close(box_fd);        
        return -1;
      }
      bytes_written = write(sub_fd, message, 1025);
      if(bytes_written <= 0){
        close(sub_fd);
        unlink(sub_pipe_name);
        remove_subscriber(box_name);
        tfs_close(box_fd);
        return -1;
      }
    }
    close(sub_fd);
    unlink(sub_pipe_name);
    remove_subscriber(box_name);
    tfs_close(box_fd);
    return 0;
}

int handle_box_creation(char* buffer){
  char man_pipe_name[256], box_name[32], response[1029];
  int man_fd;
  ssize_t code = 4;
  int32_t ret_code;
  if (pthread_mutex_lock(&request_buffer_lock) == -1){
    return -1;
  }

  memcpy(man_pipe_name, buffer, 256);
  memcpy(box_name, buffer + 256, 32);
  
  if (pthread_mutex_unlock(&request_buffer_lock) == -1){
    return -1;
      }
  man_pipe_name[255] = '\0';
  box_name[31] = '\0';

  // initialize the response
  memset(response,'\0',1029);
  memcpy(response, &code, 1);
  // check to see if the box exists
  if(create_box(box_name) != -1){
    ret_code = 0;
    memcpy(response + 1, &ret_code, 4);
  }

  else{
    ret_code = -1;
    memcpy(response + 1, &ret_code, 4);
    memcpy(response + 5, BOX_CREATION_FAILED, sizeof(BOX_CREATION_FAILED));
  }

  man_fd = open(man_pipe_name, O_WRONLY);
  if(man_fd == -1){
    unlink(man_pipe_name);
    return -1;
  }
  if(write(man_fd, response, 1029) == -1){
      return -1;
  }
  close(man_fd);
  return 0;
}

int handle_box_remotion(char* buffer){
  char man_pipe_name[256], box_name[32], response[1029];
  int man_fd;
  ssize_t code = 6;
  int32_t ret_code;
  if (pthread_mutex_lock(&request_buffer_lock) == -1){
    return -1;
  }

  memcpy(man_pipe_name, buffer, 256);
  memcpy(box_name, buffer + 256, 32);
  
  if (pthread_mutex_unlock(&request_buffer_lock) == -1){
    return -1;
  }
  man_pipe_name[255] = '\0';
  box_name[31] = '\0';

  // initialize the response
  memset(response,'\0',1029);
  memcpy(response, &code, 1);
  // create the box
  if(remove_box(box_name) != -1){
    ret_code = 0;
    memcpy(response + 1, &ret_code, 4);
  }

  else{
    ret_code = -1;
    memcpy(response + 1, &ret_code, 4);
    memcpy(response + 5, BOX_REMOVAL_FAILED, sizeof(BOX_REMOVAL_FAILED));
  }

  man_fd = open(man_pipe_name, O_WRONLY);
  if(man_fd == -1){
    unlink(man_pipe_name);
    return -1;
  }
  if(write(man_fd, response, 1029) == -1){
      return -1;
  }
  close(man_fd);
  return 0;
}

int handle_box_listing(char* buffer){
  char man_pipe_name[256], response[58];
  if (pthread_mutex_lock(&request_buffer_lock) == -1){
    return -1;
  }
      
  memcpy(man_pipe_name, buffer, 256);

  if (pthread_mutex_unlock(&request_buffer_lock) == -1){
    return -1;
  }
  man_pipe_name[255] = '\0';

  ssize_t code = 8;
  int um = 1, zero = 0, fd;

  memset(response, '\0', 58);
  memcpy(response, &code, 1);
  fd = open(man_pipe_name, O_WRONLY);
  if (fd == -1){
      return -1;
  }

  // check to see if the box exists
  if (boxes_count == 0) {
    // set last
    memcpy(response + 1, &um, 1);
    close(fd);
    return 0;
  }

  else {
    
    for (int i = 0; i < boxes_count; i++) {
      if (i == boxes_count - 1) {
        memcpy(response + 1, &um, 1);
      }
      else {
        memcpy(response + 1, &zero, 1);
      }
      if (pthread_mutex_lock(&boxes[i].box_lock) == -1){
        return -1;
      }
      memcpy(response + 2, &boxes[i].name, 32);
      memcpy(response + 34, &boxes[i].size, 8);
      memcpy(response + 42, &boxes[i].has_publisher, 8);
      memcpy(response + 50, &boxes[i].subscriber_count, 8);
      if (pthread_mutex_unlock(&boxes[i].box_lock) == -1){
        return -1;
      }

      // Write the input arr on FIFO
      if(write(fd, response, 58) == -1){
          return -1;
      }

      // reset response
      memset(response, '\0', 58);
      memcpy(response, &code, 1);
    }
  }
  return 0;
}

int get_request(uint8_t code, int fd){
    if(pthread_mutex_lock(&request_buffer_lock)==-1){
        return -1;
    }
    char buffer[BUFFER_SIZE];
    memset(buffer, '\0', BUFFER_SIZE);
    buffer[0] = (char)code;
    if(pthread_mutex_unlock(&request_buffer_lock)==-1){
        return -1;
    }
    if (code == 7){
      if(read(fd, buffer+1, 257) == -1){
        return -1;
      }
    }
    else if( code == 1 || code == 2 || code == 3 || code == 5){
      if(read(fd, buffer+1, 288) == -1){
        return -1;
      }
    }
    else{
      return -1;
    }
    pcq_enqueue(&requests, buffer);
    return 0;
}


void* thread_func(){
    while(1){
        char* request = (char*)pcq_dequeue(&requests);
        if(pthread_mutex_lock(&request_buffer_lock)==-1){
            exit(1);
        }
        uint8_t code = (uint8_t)request[0];
        if(pthread_mutex_unlock(&request_buffer_lock)==-1){
            exit(1);
        }
        switch (code) {
            case 1:
                // handle the request
                if(handle_publisher(request+1) == -1){
                    fprintf(stderr, PUB_REGISTRATION_FAILED);
                }
                printf(PUB_REGISTRATION_SUCCEEDED);
                break;
            case 2:
                // handle the request
                if(handle_subscriber(request+1) == -1){
                    fprintf(stderr, SUB_REGISTRATION_FAILED);
                }
                printf(SUB_REGISTRATION_SUCCEEDED);
                break;
            case 3:
                // handle the request
                if(handle_box_creation(request+1) == -1){
                    fprintf(stderr, BOX_CREATION_FAILED);
                }
                printf(BOX_CREATION_SUCCEEDED);
                break;

            case 5:
                // handle the request
                if(handle_box_remotion(request+1) == -1){
                    fprintf(stderr, BOX_REMOVAL_FAILED);
                }
                printf(BOX_REMOVAL_SUCCEEDED);
                break;
            
            case 7:
                // handle the request
                if(handle_box_listing(request+1) == -1){
                    fprintf(stderr, BOX_LISTING_FAILED);
                }
                printf(BOX_LISTING_SUCCEEDED);
                break;

            default:
                break;
        }
    }

}

int main(int argc, char **argv) {
    if(argc != 3){
        fprintf(stderr, "[MBROKER]: invalid number of arguments");
    }
    char register_pipe[256];
    tfs_params params = tfs_default_params();
    params.block_size = BLOCK_SIZE;
    tfs_init(&params);
    boxes_init(INITIAL_MAX_BOXES);
    pthread_mutex_init(&request_buffer_lock, NULL);

    size_t n_threads = (size_t)atoi(argv[2]);
    pcq_create(&requests, n_threads);
    pthread_t thread_pool[n_threads];
    for (int i = 0; i < n_threads; i++) {
        pthread_create(&thread_pool[i], NULL, &thread_func, NULL);
    }

    int fd;
    uint8_t code;
    memcpy(register_pipe, argv[1], 256);
    register_pipe[255] = '\0';
    unlink(register_pipe);
    if(mkfifo(register_pipe, S_IRWXU) == -1){
      pthread_mutex_destroy(&boxes_lock);
      pthread_mutex_destroy(&request_buffer_lock);
      free(boxes);
      unlink(register_pipe);
      tfs_destroy();
      pcq_destroy(&requests);
      return -1;
    }

    // main thread function - keeps reading from register fifo to check for new requests
    while(1){
      // Open FIFO for Read only
      fd = open(register_pipe, O_RDONLY);
      if(fd == -1){
        pthread_mutex_destroy(&boxes_lock);
        pthread_mutex_destroy(&request_buffer_lock);
        free(boxes);
        close(fd);
        unlink(register_pipe);
        tfs_destroy();
        pcq_destroy(&requests);
        return -1;
      }

      // Read code from FIFO
      if(read(fd, &code, 1) == -1){
        pthread_mutex_destroy(&boxes_lock);
        pthread_mutex_destroy(&request_buffer_lock);
        free(boxes);
        close(fd);
        unlink(register_pipe);
        tfs_destroy();
        pcq_destroy(&requests);
        return -1;
      }

      if(get_request(code, fd) == -1){
        pthread_mutex_destroy(&boxes_lock);
        pthread_mutex_destroy(&request_buffer_lock);
        free(boxes);
        close(fd);
        unlink(register_pipe);
        tfs_destroy();
        pcq_destroy(&requests);
        return -1;
      }

    }
    if(pthread_mutex_destroy(&boxes_lock)==-1){
      pthread_mutex_destroy(&boxes_lock);
      pthread_mutex_destroy(&request_buffer_lock);
      free(boxes);
      close(fd);
      unlink(register_pipe);
      tfs_destroy();
      pcq_destroy(&requests);
      return -1;
    }
    if(pthread_mutex_destroy(&request_buffer_lock)==-1){
      pthread_mutex_destroy(&boxes_lock);
      pthread_mutex_destroy(&request_buffer_lock);
      free(boxes);
      close(fd);
      unlink(register_pipe);
      tfs_destroy();
      pcq_destroy(&requests);
      return -1;
    }
    pthread_mutex_destroy(&boxes_lock);
    pthread_mutex_destroy(&request_buffer_lock);
    free(boxes);
    close(fd);
    unlink(register_pipe);
    tfs_destroy();
    pcq_destroy(&requests);
    return 0;
}
