/************************************************************************
 * AUTHOR:		CHENFEI LOU

 * DATE:		APRIL 1

 * DESCRIPTION:	THIS FILE IMPLEMENTS THE MAP REDUCE ALGORITHM THAT SUPPORTS
 * 				CONCURRENCY AMONG MULTIPLE THREADS INSIDE ONE PROCESS
 * 
 * RELATED:		"mapreduce.h"
 ************************************************************************/

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <pthread.h>
#include <string.h>
#include <errno.h>

#include "mapreduce.h"
#include "hashmap.h"

////////////////////////////////////////////////////////////////////////////////
///                                DATA TYPES                                ///
////////////////////////////////////////////////////////////////////////////////

/* key-valye list */
typedef struct kv {
    char* key;
    char* value;
} kv_t;

/* partitioned kv list */
typedef struct kv_list {
    kv_t ***elements;
	size_t num_lists;
	size_t *num_elements_list;
    size_t *capacity_list;
	pthread_mutex_t lock;
	size_t *kvl_counter_list;
} kv_list_t;

/* the global job queue for mappers */
typedef struct job_list {
	int pointer;
	char **arg_list;
	int length;
	pthread_mutex_t lock;
} job_list_t;

typedef struct reducer_str {
	Reducer reducer;
	int partition_num;
} reducer_str_t;

typedef struct global_mapreduce {
	Partitioner partition;
	int num_reducers;
} global_mapreduce_t;


////////////////////////////////////////////////////////////////////////////////
///                             GLOBAL VARIABLES                             ///
////////////////////////////////////////////////////////////////////////////////

job_list_t job_list;

kv_list_t kvl;

global_mapreduce_t global_mapreduce;


////////////////////////////////////////////////////////////////////////////////
///                             DATATYPE HELPERS                             ///
////////////////////////////////////////////////////////////////////////////////

void job_list_init(int argc, char **argv) {
	job_list.length = argc - 1;
	job_list.arg_list = (char **) malloc(job_list.length * sizeof(char *));

	for (int i = 0; i < job_list.length; ++i) {
		job_list.arg_list[i] = strdup(argv[i + 1]);
	}

	job_list.pointer = 0;
	job_list.lock = (pthread_mutex_t) PTHREAD_MUTEX_INITIALIZER;
}

void job_list_free() {
	for (int i = 0; i < job_list.length; ++i)
		free(job_list.arg_list[i]);
	free(job_list.arg_list);
}

void kvl_init(int num_partition, int init_capacity) {
	/* initialize kv list */
	kvl.num_lists = num_partition;
	kvl.num_elements_list = (size_t *) malloc(num_partition * sizeof(size_t));
	kvl.capacity_list = (size_t *) malloc(num_partition * sizeof(size_t));
	kvl.kvl_counter_list = (size_t *) malloc(num_partition * sizeof(size_t));
	for (int i = 0; i < num_partition; ++i) {
		kvl.num_elements_list[i] = 0;
		kvl.capacity_list[i] = init_capacity;
		kvl.kvl_counter_list[i] = 0;
	}
	kvl.lock = (pthread_mutex_t) PTHREAD_MUTEX_INITIALIZER;
	kvl.elements = (kv_t ***) malloc(num_partition * sizeof(kv_t **));
	for (int i = 0; i < num_partition; ++i) {
		kvl.elements[i] = (kv_t **) malloc(init_capacity * sizeof(kv_t *));
	}	
}

void kvl_free() {
	for (int i = 0; i < kvl.num_lists; ++i) {
		for (size_t j = 0; j < kvl.num_elements_list[i]; ++j) {
			free(kvl.elements[i][j]->key);
			free(kvl.elements[i][j]->value);
			free(kvl.elements[i][j]);
		}
		free(kvl.elements[i]);
	}
	free(kvl.elements);
	free(kvl.num_elements_list);
	free(kvl.capacity_list);
	free(kvl.kvl_counter_list);
}




////////////////////////////////////////////////////////////////////////////////
///                                FUNCTIONS                                 ///
////////////////////////////////////////////////////////////////////////////////

char* get_func(char* key, int partition_number) {
	size_t pointer = kvl.kvl_counter_list[partition_number];
	if (pointer == kvl.num_elements_list[partition_number]) {
		return NULL;
	}
	kv_t *curr_elt = kvl.elements[partition_number][pointer];
	if (strcmp(curr_elt->key, key) == 0) {
		kvl.kvl_counter_list[partition_number] ++;
		return curr_elt->value;
	}
	return NULL;
}

void add_to_list(struct kv* elt) {
    int partition_num = global_mapreduce.partition(elt->key, global_mapreduce.num_reducers);
	pthread_mutex_lock(&kvl.lock);
	if (kvl.num_elements_list[partition_num] == kvl.capacity_list[partition_num]) {
		kvl.capacity_list[partition_num] *= 2;
		kvl.elements[partition_num] = realloc(kvl.elements[partition_num], 
											  kvl.capacity_list[partition_num] * sizeof(kv_t *));
	}
	kvl.elements[partition_num][kvl.num_elements_list[partition_num] ++] = elt;
	pthread_mutex_unlock(&kvl.lock);
}

int cmp(const void* a, const void* b) {
    char* str1 = (*(struct kv **)a)->key;
    char* str2 = (*(struct kv **)b)->key;
    return strcmp(str1, str2);
}

void MR_Emit(char* key, char* value) {
    kv_t *elt = (kv_t *) malloc(sizeof(kv_t));
    if (elt == NULL) {
        printf("Malloc error! %s\n", strerror(errno));
        exit(1);
    }
	elt->key = strdup(key);
    elt->value = strdup(value);
	add_to_list(elt);
	return;
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

/*
 * this function asks for job to do in an infinite loop until all jobs are done
 * when fetching for a job, increment <job_list.pointer> in a thread-safe way
 */
void *mapper_func(void *mapper) {
	while (1) {
		/* check whether all jobs are done, and if not, fetch a job */
		pthread_mutex_lock(&job_list.lock);
		if (job_list.pointer >= job_list.length) {	// all jobs done
			pthread_mutex_unlock(&job_list.lock);
			return NULL;
		}
		char *arg = job_list.arg_list[job_list.pointer];
		job_list.pointer ++;						// fetch a job
		pthread_mutex_unlock(&job_list.lock);

		Mapper map = (Mapper) mapper;
		map(arg);
	}
}

void *reducer_func(void *reducer_str) {
	while (1) {
		int partition_num = ((reducer_str_t *) reducer_str)->partition_num;

		/* check whether all jobs are done, and if not, fetch a job */
		pthread_mutex_lock(&kvl.lock);
		size_t pointer = kvl.kvl_counter_list[partition_num];
		if (pointer >= kvl.num_elements_list[partition_num]) {
			pthread_mutex_unlock(&kvl.lock);
			return NULL;
		}
		char *key = kvl.elements[partition_num][pointer]->key;
		pthread_mutex_unlock(&kvl.lock);
		
		Reducer reducer = ((reducer_str_t *) reducer_str)->reducer;
		reducer(key, get_func, partition_num);
	}
}

void MR_Run(int argc, char *argv[], Mapper map, int num_mappers,
	    	Reducer reduce, int num_reducers, Partitioner partition) {
	/* setup */
    kvl_init(num_reducers, 10);
	global_mapreduce.partition = partition;
	global_mapreduce.num_reducers = num_reducers;
	pthread_t mappers[num_mappers];
	pthread_t reducers[num_reducers];
	reducer_str_t reducer_str[num_reducers];
	job_list_init(argc, argv);
	for (int i = 0; i < num_reducers; ++i) {
		reducer_str[i].reducer = reduce;
		reducer_str[i].partition_num = i;
	}

	/* create mapper threads and join them. 
	 * The threads will automatically fetch new jobs from job_list 
	   until all jobs done */
    for (int i = 0; i < num_mappers; i++)
		if (pthread_create(mappers + i, NULL, mapper_func, map)) {
			printf("error when creating mapper threads!\n");
			exit(-1);
		}
	for (int i = 0; i < num_mappers; i++)
		if (pthread_join(mappers[i], NULL)) {
			printf("error when joining mapper threads!\n");
			exit(-1);
		}
	job_list_free();

	for (int i = 0; i < num_reducers; ++i) {
		qsort(kvl.elements[i], 
			  kvl.num_elements_list[i],
			  sizeof(kv_t *),
			  cmp);
	}

    /* create reducer threads and join them.
	 * The threads will automatically ask for new jobs from kvl 
	   until their partition is empty */
	for (int i = 0; i < num_reducers; ++i)
		if (pthread_create(reducers + i, NULL, reducer_func, reducer_str + i)) {
			printf("error when creating reducer threads!\n");
			exit(-1);
		}
	for (int i = 0; i < num_reducers; ++i)
		if (pthread_join(reducers[i], NULL)) {
			printf("error when joining reducer threads!\n");
			exit(-1);
		}
	kvl_free();
}
