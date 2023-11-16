
#include "../../config.h"
#include "../../globals.h"
#include "sax/ts.h"
#include "sax/sax.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "isax_index.h"
#include "isax_query_engine.h"
#include "parallel_query_engine.h"
#include "isax_node.h"
#include "pqueue.h"
#include "isax_first_buffer_layer.h"
#include "ads/isax_node_split.h"
#include "inmemory_index_engine.h"
#include "ads/array.h"

#ifndef al_parallel_inmemory_query_engine_h
#define al_parallel_inmemory_query_engine_h

//Added Geopat for random failure on query answering
int random_choice;


typedef struct MESSI_workerdata_ekosmas_lf
{
	ts_type *paa,*ts;
	pqueue_t **allpq;
	query_result ***allpq_data;	
	array_list_t *array_lists;
	sorted_array_t **sorted_arrays;
	volatile unsigned char *queue_finished;
	volatile unsigned char *helper_queue_exist;		
	volatile int *fai_queue_counters;
	volatile float **queue_bsf_distance;
	isax_index *index;
	float minimum_distance;
	query_result * volatile *bsf_result_p;
	volatile int *node_counter;
	volatile unsigned long *sorted_array_counter;					
	volatile unsigned long *sorted_array_FAI_counter;					
	isax_node **nodelist;
	int amountnode;
	int workernumber; 		
	char parallelism_in_subtree;
	volatile unsigned long *next_queue_data_pos;
	unsigned long query_id;
	pthread_barrier_t *wait_tree_pruning_phase_to_finish;
	pthread_barrier_t *wait_process_queue;
	volatile unsigned long *subtree_fai;
	volatile unsigned long *subtree;
	volatile unsigned long *subtree_prune_helpers_exist;
	int thread_fail; //Added Geopat Failure simulation
	int already_fail;
	int fail_once;
	int *current_helpers;
}MESSI_workerdata_ekosmas_lf;


extern void randomThreadsToFail(int start,int end,int N, int *failedThreads);


query_result exact_search_ParISnew_inmemory_hybrid_ekosmas_lf(ts_type *ts, ts_type *paa, isax_index *index, node_list *nodelist,
                           float minimum_distance, const char parallelism_in_subtree, const int third_phase, const int query_id,int already_failed);

void* exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_mixed_sorted_array_full_helping(void *rfdata);


void add_to_array_data_lf(float *paa, isax_node *node, isax_index *index, float bsf, array_list_t *array_lists, int *tnumber, volatile unsigned long *next_queue_data_pos, const int query_id);

float * rawfile;

#endif
