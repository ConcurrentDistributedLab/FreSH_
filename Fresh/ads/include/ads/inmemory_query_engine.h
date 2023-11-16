
#ifndef al_inmemory_query_engine_h
#define al_inmemory_query_engine_h
#include "../../config.h"
#include "../../globals.h"
#include "sax/ts.h"
#include "sax/sax.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "isax_index.h"
#include "isax_query_engine.h"
#include "isax_node.h"
#include "pqueue.h"
#include "isax_first_buffer_layer.h"
#include "ads/isax_node_split.h"


static __thread double my_time_for_tree_prunning = 0;
static __thread unsigned long my_num_subtrees_pruned = 0;
static __thread struct timeval my_time_start_val;
static __thread struct timeval my_current_time_val;
static __thread double my_tS;
static __thread double my_tE;
static __thread unsigned long subrees_pruned_helped_cnt = 0;
static __thread unsigned long subrees_pruned_helping_avoided_cnt = 0;


float calculate_node_distance_inmemory_ekosmas_lf (isax_index *index, isax_node *node, ts_type *query, float bsf, const char parallelism_in_subtree);
float calculate_node_distance2_inmemory_ekosmas_lf (isax_index *index, query_result *n, ts_type *query, ts_type *paa, float bsf, const char parallelism_in_subtree);
query_result  approximate_search_inmemory_pRecBuf_ekosmas_lf (ts_type *ts, ts_type *paa, isax_index *index, const char parallelism_in_subtree);

// volatile unsigned long all_subtrees_pruned = 0; // EKOSMAS: COMPILER ERROR: "multiple definition of `all_subtrees_pruned';"

#define COUNT_MY_TIME_START                 gettimeofday(&my_time_start_val, NULL);
#define COUNT_MY_TIME_FOR_TREE_PRUNING_END  gettimeofday(&my_current_time_val, NULL); \
                                                my_tS = my_time_start_val.tv_sec*1000000 + (my_time_start_val.tv_usec); \
                                                my_tE = my_current_time_val.tv_sec*1000000 + (my_current_time_val.tv_usec); \
                                                my_time_for_tree_prunning += (my_tE - my_tS);
#define BACKOFF_TREE_PRUNING_DELAY_VALUE    (my_time_for_tree_prunning/my_num_subtrees_pruned)

float * rawfile;
#endif
