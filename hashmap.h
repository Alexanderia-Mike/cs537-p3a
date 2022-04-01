#ifndef __hashmap_h__
#define __hashmap_h__

/* added for project p3a begin */

#include <pthread.h>

typedef struct {
    int num_waiting_reader;
    int num_reader;
    int num_waiting_writer;
    int num_writer;
    pthread_mutex_t lock;
    pthread_cond_t can_read;
    pthread_cond_t can_write;
} read_write_lock_t;

void rw_lock_init(read_write_lock_t *rw_lock);
void begin_read(read_write_lock_t *rw_lock);
void end_read(read_write_lock_t *rw_lock);
void begin_write(read_write_lock_t *rw_lock);
void end_write(read_write_lock_t *rw_lock);

/* added for project p3a end */

#define MAP_INIT_CAPACITY 11

typedef struct {
    char* key;
    void* value;
} MapPair;

typedef struct {
    MapPair** contents;
    size_t capacity;
    size_t size;
    /* added for project p3a begin */
    read_write_lock_t rw_lock;
    /* added for project p3a end */
} HashMap;


// External Functions
HashMap* MapInit(void);
void MapPut(HashMap* map, char* key, void* value, int value_size);
char* MapGet(HashMap* map, char* key);
size_t MapSize(HashMap* map);

// Internal Functions
int resize_map(HashMap* map);
size_t Hash(char* key, size_t capacity);


#endif // __hashmap_h__
