#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include "hashmap.h"

#define FNV_OFFSET 14695981039346656037UL
#define FNV_PRIME 1099511628211UL

/* added for project p3a begin */

void rw_lock_init(read_write_lock_t *rw_lock) {
    rw_lock->num_waiting_reader = 0;
    rw_lock->num_reader = 0;
    rw_lock->num_waiting_writer = 0;
    rw_lock->num_writer = 0;
    rw_lock->lock = (pthread_mutex_t) PTHREAD_MUTEX_INITIALIZER;
    rw_lock->can_read = (pthread_cond_t) PTHREAD_COND_INITIALIZER;
    rw_lock->can_write = (pthread_cond_t) PTHREAD_COND_INITIALIZER;
}

void begin_read(read_write_lock_t *rw_lock) {
    pthread_mutex_lock(&rw_lock->lock);
    while (rw_lock->num_writer + rw_lock->num_waiting_writer > 0) {
        rw_lock->num_waiting_reader ++;
        pthread_cond_wait(&rw_lock->can_read, &rw_lock->lock);
        rw_lock->num_waiting_reader --;
    }
    rw_lock->num_reader ++;
    pthread_mutex_unlock(&rw_lock->lock);
}

void end_read(read_write_lock_t *rw_lock) {
    pthread_mutex_lock(&rw_lock->lock);
    rw_lock->num_reader --;
    if (rw_lock->num_reader == 0 && rw_lock->num_waiting_writer > 0)
        pthread_cond_signal(&rw_lock->can_write);
    pthread_mutex_unlock(&rw_lock->lock);
}

void begin_write(read_write_lock_t *rw_lock) {
    pthread_mutex_lock(&rw_lock->lock);
    while (rw_lock->num_reader + rw_lock->num_writer > 0) {
        rw_lock->num_waiting_writer ++;
        pthread_cond_wait(&rw_lock->can_write, &rw_lock->lock);
        rw_lock->num_waiting_writer --;
    }
    rw_lock->num_writer = 1;
    pthread_mutex_unlock(&rw_lock->lock);
}

void end_write(read_write_lock_t *rw_lock) {
    pthread_mutex_lock(&rw_lock->lock);
    rw_lock->num_writer = 0;
    if (rw_lock->num_waiting_writer > 0)
        pthread_cond_signal(&rw_lock->can_write);
    else if (rw_lock->num_waiting_reader > 0)
        pthread_cond_broadcast(&rw_lock->can_read);
    pthread_mutex_unlock(&rw_lock->lock);
}

/* added for project p3a end */

HashMap* MapInit(void)
{
    HashMap* hashmap = (HashMap*) malloc(sizeof(HashMap));
    hashmap->contents = (MapPair**) calloc(MAP_INIT_CAPACITY, sizeof(MapPair*));
    hashmap->capacity = MAP_INIT_CAPACITY;
    hashmap->size = 0;
    /* added for project p3a begin */
    rw_lock_init(&hashmap->rw_lock);
    /* added for project p3a end */
    return hashmap;
}


/* added for project p3a begin */
void MapPut(HashMap* hashmap, char* key, void* value, int value_size)
{
    MapPair* newpair = (MapPair*) malloc(sizeof(MapPair));
    int h;
    newpair->key = strdup(key);
    newpair->value = (void *)malloc(value_size);
    memcpy(newpair->value, value, value_size);

    begin_write(&hashmap->rw_lock);
    if (hashmap->size > (hashmap->capacity / 2)) {
        if (resize_map(hashmap) < 0) {
            end_write(&hashmap->rw_lock);
            exit(0);
        }
    }

    size_t capacity = hashmap->capacity;
    h = Hash(key, capacity);

    while (hashmap->contents[h] != NULL) {  // open-addressing
        // if keys are equal, update
        if (!strcmp(key, hashmap->contents[h]->key)) {
            free(hashmap->contents[h]);
            hashmap->contents[h] = newpair;
            end_write(&hashmap->rw_lock);
            return;
        }
        h++;
        if (h == hashmap->capacity)
            h = 0;
    }

    // key not found in hashmap, h is an empty slot
    hashmap->contents[h] = newpair;
    hashmap->size += 1;
    end_write(&hashmap->rw_lock);
}
/* added for project p3a end */


/* added for project p3a begin */
char* MapGet(HashMap* hashmap, char* key)
{
    begin_read(&hashmap->rw_lock);
    int h = Hash(key, hashmap->capacity);
    while (hashmap->contents[h] != NULL) {
        if (!strcmp(key, hashmap->contents[h]->key)) {
            char *ret_val = hashmap->contents[h]->value;
            end_read(&hashmap->rw_lock);
            return ret_val;
        }
        h++;
        if (h == hashmap->capacity) {
            h = 0;
        }
    }
    end_read(&hashmap->rw_lock);
    return NULL;
}
/* added for project p3a end */


size_t MapSize(HashMap* map)
{
    begin_read(&map->rw_lock);
    size_t ret_val = map->size;
    end_read(&map->rw_lock);
    return ret_val;
}


/* when this function is called, write lock is already held */
int resize_map(HashMap* map)
{
    MapPair** temp;
    size_t newcapacity = map->capacity * 2; // double the capacity

    // allocate a new hashmap table
    temp = (MapPair**) calloc(newcapacity, sizeof(MapPair*));
    if (temp == NULL) {
        printf("Malloc error! %s\n", strerror(errno));
        return -1;
    }

    size_t i;
    int h;
    MapPair* entry;
    // rehash all the old entries to fit the new table
    for (i = 0; i < map->capacity; i++) {
        if (map->contents[i] != NULL)
            entry = map->contents[i];
        else 
            continue;
        h = Hash(entry->key, newcapacity);
        while (temp[h] != NULL) {
            h++;
            if (h == newcapacity)
            h = 0;
        }
        temp[h] = entry;
    }

    // free the old table
    free(map->contents);
    // update contents with the new table, increase hashmap capacity
    map->contents = temp;
    map->capacity = newcapacity;
    return 0;
}

// FNV-1a hashing algorithm
// https://en.wikipedia.org/wiki/Fowler-Noll-Vo_hash_function#FNV-1a_hash
size_t Hash(char* key, size_t capacity) {
    size_t hash = FNV_OFFSET;
    for (const char *p = key; *p; p++) {
	hash ^= (size_t)(unsigned char)(*p);
	hash *= FNV_PRIME;
	hash ^= (size_t)(*p);
    }
    return (hash % capacity);
}
