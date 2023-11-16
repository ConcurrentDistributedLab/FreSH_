
#define _GNU_SOURCE


#ifdef VALUES
#include <values.h>
#endif
#include <float.h>
#include "../../config.h"
#include "../../globals.h"
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <pthread.h>
#include <stdbool.h>
#include <sys/wait.h>
#include <sched.h>
#include <malloc.h>


#include "omp.h"  
#include "ads/isax_query_engine.h"
#include "ads/inmemory_index_engine.h"
#include "ads/inmemory_query_engine.h"
#include "ads/parallel_inmemory_query_engine.h"
#include "ads/parallel_index_engine.h"
#include "ads/isax_first_buffer_layer.h"
#include "ads/pqueue.h"
#include "ads/array.h"
#include "ads/sax/sax.h"
#include "ads/isax_node_split.h"

#define left(i)   ((i) << 1)
#define right(i)  (((i) << 1) + 1)
#define parent(i) ((i) >> 1)

#define ULONG_CACHE_PADDING (CACHE_LINE_SIZE/sizeof(unsigned long))
#define EPSILON 0.00001

int count_duplicates = 0;

int NUM_PRIORITY_QUEUES;

volatile unsigned long all_subtrees_pruned = 0;

inline void threadPin(int pid, int max_threads) {
    int cpu_id;

    cpu_id = pid % max_threads;
    pthread_setconcurrency(max_threads);

    cpu_set_t mask;  
    unsigned int len = sizeof(mask);

    CPU_ZERO(&mask);

    CPU_SET(cpu_id, &mask);                              // SOCKETS PINNING - Nefeli

    // if (cpu_id % 2 == 0)                                             // OLD PINNING 2
    //    CPU_SET(cpu_id % max_threads, &mask);
    // else 
    //    CPU_SET((cpu_id + max_threads/2)% max_threads, &mask);

    // if (cpu_id % 2 == 0)                                             // FULL HT
    //    CPU_SET(cpu_id/2, &mask);
    // else 
    //    CPU_SET((cpu_id/2) + (max_threads/2), &mask);

    //CPU_SET((cpu_id%4)*10 + (cpu_id%40)/4 + (cpu_id/40)*40, &mask);     // SOCKETS PINNING - Vader

    int ret = sched_setaffinity(0, len, &mask);
    if (ret == -1)
        perror("sched_setaffinity");
}


// ekosmas-lf version
query_result exact_search_ParISnew_inmemory_hybrid_ekosmas_lf(ts_type *ts, ts_type *paa, isax_index *index, node_list *nodelist,
                           float minimum_distance, const char parallelism_in_subtree, const int third_phase, const int query_id,int already_failed) 
{   
    query_result bsf_result = approximate_search_inmemory_pRecBuf_ekosmas_lf(ts, paa, index, parallelism_in_subtree);
    query_result * volatile bsf_result_p = &bsf_result;
    SET_APPROXIMATE(bsf_result.distance);
    
    //-----------------------------------------Failing Simulation Initialization-------------------------------------------------------

    //Step 1 Choose Random Threads to fail. 
    int failThreads[maxquerythread];
    for(int i = 0; i < maxquerythread ; i++){
        failThreads[i] = 0 ;
    }
    random_choice = rand() %5; //Choosing random where to fail

    randomThreadsToFail(0,maxquerythread,index->settings->number_of_fail_threads,failThreads);
    
  //-------------------------------Ending of Failing Simulation Initialization-------------------------------------------------------


    // Early termination...
    if (bsf_result.distance == 0) {
        return bsf_result;
    }

    if (maxquerythread == 1) {
        NUM_PRIORITY_QUEUES = 1;
    }
    else {
        NUM_PRIORITY_QUEUES = maxquerythread/2;
    }    
    pqueue_t **allpq = calloc(NUM_PRIORITY_QUEUES, sizeof(pqueue_t*));
    // query_result ***allpq_data = malloc(sizeof(query_result **) * NUM_PRIORITY_QUEUES);
    array_list_t *array_lists = malloc(sizeof(array_list_t) * NUM_PRIORITY_QUEUES);
    for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
    {
        // allpq_data[i] = calloc (index->settings->root_nodes_size, sizeof(query_result *));
        initArrayList(&array_lists[i], index->settings->root_nodes_size);
    }
    sorted_array_t **sorted_arrays = calloc(NUM_PRIORITY_QUEUES, sizeof(sorted_array_t *));

    volatile unsigned long *next_queue_data_pos = calloc(NUM_PRIORITY_QUEUES, sizeof(unsigned long));       // EKOSMAS: playing with CACHE_LINE alignment/padding here could improve performance
    volatile unsigned char *queue_finished = calloc(NUM_PRIORITY_QUEUES, sizeof(unsigned char));
    volatile unsigned char *helper_queue_exist = calloc(NUM_PRIORITY_QUEUES, sizeof(unsigned char));
    volatile int *fai_queue_counters = calloc(NUM_PRIORITY_QUEUES, sizeof(int));
    volatile float **queue_bsf_distance = calloc(NUM_PRIORITY_QUEUES, sizeof(unsigned long));
    // volatile unsigned long *subtree_fai = calloc(nodelist->node_amount*ULONG_CACHE_PADDING, sizeof(unsigned long));
    // volatile unsigned long *subtree = calloc(nodelist->node_amount*ULONG_CACHE_PADDING, sizeof(unsigned long));
    // volatile unsigned long *subtree_prune_helpers_exist = calloc(nodelist->node_amount, sizeof(unsigned long));


    volatile int node_counter = 0;                                                                  // EKOSMAS, 29 AUGUST 2020: added volatile
    volatile unsigned long *sorted_array_counter = calloc(NUM_PRIORITY_QUEUES*ULONG_CACHE_PADDING, sizeof(unsigned long));        // EKOSMAS, 02 OCTOBER 2020: added, 14 OCTOBER 2020: changed
    volatile unsigned long *sorted_array_FAI_counter = calloc(NUM_PRIORITY_QUEUES*ULONG_CACHE_PADDING, sizeof(unsigned long));

    pthread_barrier_t wait_tree_pruning_phase_to_finish;                                // used only for the no_help version of Fresh; it is required to ensure that query answering threads will process elements from the priority queue only after the tree pruning phase has completed.
    pthread_barrier_init(&wait_tree_pruning_phase_to_finish, NULL, maxquerythread);
    pthread_barrier_t wait_process_queue;                                // used only for the no_help version of Fresh; it is required to ensure that query answering threads will process elements from the priority queue only after the tree pruning phase has completed.
    pthread_barrier_init(&wait_process_queue, NULL, maxquerythread);

    // EKOSMAS: change this so that only i is passed as an argument and all the others are only once in some global variable.
    MESSI_workerdata_ekosmas_lf workerdata[maxquerythread];
    for (int i = 0; i < maxquerythread; i++)
    {
        workerdata[i].ts = ts;                                                  // query ts
        workerdata[i].paa = paa;                                                // query paa
        workerdata[i].allpq = allpq;
        // workerdata[i].allpq_data = allpq_data;
        workerdata[i].array_lists = array_lists;
        workerdata[i].sorted_arrays = sorted_arrays;
        workerdata[i].queue_finished = queue_finished;
        workerdata[i].fai_queue_counters = fai_queue_counters;
        workerdata[i].queue_bsf_distance = queue_bsf_distance;
        workerdata[i].helper_queue_exist = helper_queue_exist;
        workerdata[i].nodelist = nodelist->nlist;                               // the list of subtrees
        workerdata[i].amountnode = nodelist->node_amount;                       // the total number of (non empty) subtrees
        workerdata[i].index = index;
        workerdata[i].minimum_distance = minimum_distance;                      // its value is FTL_MAX
        workerdata[i].node_counter = &node_counter;                             // a counter to perform FAI and acquire subtrees
        workerdata[i].sorted_array_counter = sorted_array_counter;
        workerdata[i].sorted_array_FAI_counter = sorted_array_FAI_counter;
        workerdata[i].bsf_result_p = &bsf_result_p;                             // the BSF
        workerdata[i].workernumber = i;
        workerdata[i].parallelism_in_subtree = parallelism_in_subtree;
        workerdata[i].next_queue_data_pos = next_queue_data_pos;
        workerdata[i].query_id = query_id;
        workerdata[i].wait_tree_pruning_phase_to_finish = &wait_tree_pruning_phase_to_finish;
         workerdata[i].wait_process_queue = &wait_process_queue;
        // workerdata[i].subtree_fai = subtree_fai;
        // workerdata[i].subtree = subtree;
        // workerdata[i].subtree_prune_helpers_exist = subtree_prune_helpers_exist;
        workerdata[i].fail_once = index->settings->fail_once; //Can be 0,1,2
        if(index->settings->fail_once == 1){  //Note, We want to fail once.
            workerdata[i].already_fail = already_failed; //Already_failed is equal to 0 for 1st query and 1 for the rest.
        }
        else if(index->settings->fail_once == 2) { // We want to fail once per query.
             workerdata[i].already_fail = 0;
        }
        if(failThreads[i] == 1){
            workerdata[i].thread_fail = 1;
        }
        else{
            workerdata[i].thread_fail = 0;
        }
    }

    pthread_t threadid[maxquerythread];
    for (int i = 0; i < maxquerythread; i++) {
        pthread_create(&(threadid[i]), NULL, exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_mixed_sorted_array_full_helping, (void*)&(workerdata[i])); 
    }
    for (int i = 0; i < maxquerythread; i++) {
        pthread_join(threadid[i], NULL);
    }



    // EKOSMAS: ADDED 06/01/2023
    COUNT_OUTPUT_TIME_START
    COUNT_QUERY_ANSWERING_TIME_START

    // Free the priority queue.
    for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
    {
        if (!allpq[i]) {
            continue;
        }

        pqueue_free(allpq[i]);
    }
    free(allpq);
    free((void *)queue_finished);
    free((void *)helper_queue_exist);

    // EKOSMAS: ADDED 06/01/2023
    COUNT_QUERY_ANSWERING_TIME_END
    COUNT_OUTPUT_TIME_END

    return *bsf_result_p;

    // +++ Free the nodes that where not popped.
}



int compare_sorted_array_items(const void *a, const void *b) {
    float dist_a = ((array_element_t*)a)->distance;
    float dist_b = ((array_element_t*)b)->distance;

    if (dist_a < dist_b){
        return -1;
    } 
    else if (dist_a > dist_b){
        return 1;
    }
    else{

            return 0;
    }

}


static inline void create_sorted_array_from_data_queue(int pq_id, array_list_t *array_lists, volatile unsigned long *next_queue_data_pos, sorted_array_t **sorted_arrays) 
{
    // pqueue_t *local_pq = pqueue_init(next_queue_data_pos[pq_id], cmp_pri, get_pri, set_pri, get_pos, set_pos);
    sorted_array_t *local_sa = malloc(sizeof(sorted_array_t));
    unsigned long array_elements = next_queue_data_pos[pq_id];
    local_sa->data = malloc(array_elements * sizeof(array_element_t));
    // sorted_array_t *local_sa = (void *)memalign(CACHE_LINE_SIZE, sizeof(sorted_array_t));
    // local_sa->data = (void *)memalign(CACHE_LINE_SIZE, array_elements * sizeof(array_element_t));

    size_t j = 0; 

    // NEW IMPLEMENTATION
    int array_buckets_num = array_lists[pq_id].Top->num_node+1;
    unsigned long array_elements_traversed = 0;
    array_list_node_t *bucket = array_lists[pq_id].Top;
    for (int i = 0; i<array_buckets_num; i++, bucket = bucket->next) {
        for (int k = 0; k<array_lists[pq_id].element_size && array_elements_traversed<array_elements && !sorted_arrays[pq_id]; k++, array_elements_traversed++) {
            if (bucket->data[k].node && bucket->data[k].distance > 0) {
                local_sa->data[j].node = bucket->data[k].node;
                local_sa->data[j].distance = bucket->data[k].distance;
                j++;
            }
        }
    }
    // // OLD IMPLEMENTATION
    // array_element_t *tmp_array_element;
    // // for (int i = 0; i<next_queue_data_pos[pq_id] && !sorted_arrays[pq_id]; i++) {
    // for (int i = 0; i<array_elements && !sorted_arrays[pq_id]; i++) {
    //     if (!(tmp_array_element = get_element_at(i, &array_lists[pq_id]))->node) {
    //         continue;
    //     }
    //     local_sa->data[j].node = tmp_array_element->node;
    //     local_sa->data[j].distance = tmp_array_element->distance;
    //     j++;
    // }

    local_sa->num_elements = j;
    int duplicates = 0; 
    if (!sorted_arrays[pq_id]) {            // EKOSMAS: 2022/08/22 - Performance Enhancement
        /*qsort(local_sa->data, j, sizeof(array_element_t), compare_sorted_array_items);   
            int i = 0;
            for (int j = 1; j < local_sa->num_elements; j++) 
            { 
                i =j -1;
                while (i >=0 && local_sa->data[j].distance == local_sa->data[i].distance ){ //while i >= 0  && local_sa->data[j].node == local_sa->data[i].node 
                   if(local_sa->data[j].node == local_sa->data[i].node ){                   
                         local_sa->data[j].distance = -2 ;                             /
                   }
                    i--;    
                }
            } 
        */
       /*
        qsort(local_sa->data, j, sizeof(array_element_t), compare_sorted_array_items_pointers); //try sort them by pointers
        for (int j = 0; j < local_sa->num_elements; j++) 
        { 
            int i = j+1;
            while(i <local_sa->num_elements && local_sa->data[j].node == local_sa->data[i].node){
                 local_sa->data[i].distance = -2;
                 i++;
            }
             
        }*/
        qsort(local_sa->data, j, sizeof(array_element_t), compare_sorted_array_items);   //sort them by distance now

    }
   /* Brutoforce checking
    int count = 0 ;
    int del_count = 0;
    for (int j = 0; j < local_sa->num_elements; j++){
        if(local_sa->data[j].distance == -2){ //If i am deleted continue
            continue;
        }
        else{
            for(int i=0; i<local_sa->num_elements;i++){ //For each element 
                if(j != i && local_sa->data[j].node == local_sa->data[i].node && local_sa->data[i].distance != -2){
                    count++;
                }
                if(j != i && local_sa->data[j].node == local_sa->data[i].node && local_sa->data[i].distance == -2){
                    del_count++;
                }
            }
        }
        
    }
    */ 


    if (sorted_arrays[pq_id] || !CASPTR(&sorted_arrays[pq_id], NULL, local_sa)) {
        free(local_sa->data);
        free(local_sa);
    }
    /*else{
        if(count > 0 ){
            printf("PQ_ID = %d -> count = %d\n",pq_id,count);
        }
        if(del_count > 0){
            printf("PQ_ID = %d -> DEL count = %d\n",pq_id,del_count);
        }
    }*/
   
}
static inline int process_sorted_array_element(array_element_t *n, MESSI_workerdata_ekosmas_lf *input_data, int *checks, const char parallelism_in_subtree)
{
    // process it!
    volatile query_result *bsf_result = *(input_data->bsf_result_p);
    float bsfdistance = bsf_result->distance;
    if (n->distance > bsfdistance || n->distance > input_data->minimum_distance) {         // The best node has a worse mindist, so search is finished!
        return 0;
    }

    // If it is a leaf, check its real distance.
    if (n->node->is_leaf) {                                         // EKOSMAS 15 SEPTEMBER 2020: This will always be a leaf!!!
        if (n->distance >= 0) {                                     // EKOSMAS 14 OCTOBER 2020: ADDED - I am not sure if distance should be used in case of sorted arrays! It may give no better results, and worse it may give worse results...
            (*checks)++;                                                                    // EKOSMAS: This is just for debugging. It can be removed!

            // EKOSMAS: fast fix: (query_result *) n <--- CAUTION NEEDED: It works since the first two fields are in same potition!!
            float distance = calculate_node_distance2_inmemory_ekosmas_lf(input_data->index, (query_result *) n, input_data->ts, input_data->paa, bsfdistance, parallelism_in_subtree);


            // update bsf, if needed
            while (distance < bsfdistance) {
                query_result *bsf_result_new = malloc(sizeof(query_result));
                bsf_result_new->distance = distance;
                bsf_result_new->node = n->node;

                if (!CASPTR(input_data->bsf_result_p, bsf_result, bsf_result_new)) {
                    free(bsf_result_new);
                }

                bsf_result = *(input_data->bsf_result_p);
                bsfdistance = bsf_result->distance;
            }

            if (n->distance >= 0) {                     
                n->distance = -1;                       //  in case of slow path, inform that this sorted array element has been processed
            }     
        }
    }
    else {
        printf ("This queue node is not a leaf. Why???\n"); fflush(stdout);
    }
    return 1;
}
static inline void help_sorted_array(MESSI_workerdata_ekosmas_lf *input_data, sorted_array_t *sa, int pq_id, int *checks, const char parallelism_in_subtree)
{
    size_t pq_size = sa->num_elements;

    if (pq_size != 0) {

        // initialize sorted_array_FAI_counter[pq_id] counter
        if (!input_data->sorted_array_FAI_counter[pq_id*ULONG_CACHE_PADDING] && input_data->sorted_array_counter[pq_id*ULONG_CACHE_PADDING]) {
            CASULONG(&input_data->sorted_array_FAI_counter[pq_id*ULONG_CACHE_PADDING], 0, input_data->sorted_array_FAI_counter[pq_id*ULONG_CACHE_PADDING]);
        }

        // repeatedly take an element with FAI
        int element_id; 
        while ((element_id = __sync_fetch_and_add(&(input_data->sorted_array_FAI_counter[pq_id*ULONG_CACHE_PADDING]), 1)) < pq_size) {
            array_element_t *n = &sa->data[element_id];
            if (!process_sorted_array_element(n, input_data, checks, parallelism_in_subtree))  
                break;
        }

        // search all previous array elements and help any unprocessed
        for (int i=0; i<pq_size && !input_data->queue_finished[pq_id]; i++) {
            array_element_t *n = &sa->data[i];
            if (n->distance >= 0 && !process_sorted_array_element(n, input_data, checks, parallelism_in_subtree))
                break;
        }
    }

    // mark this sorted array as processed/finished
    if (!input_data->queue_finished[pq_id]) {
        input_data->queue_finished[pq_id] = 1;
    }
}

inline void backoff_delay_ulong(unsigned long backoff, volatile unsigned long *stop, int query_id) {
    if (!backoff){
        return;
    }

    volatile unsigned long i;

    for (i = 0; i < backoff && (*stop) != query_id; i++)
        ;
}


// ekosmas-lf version - MIXED - Version with 80 sorted arrays and helping during sorted array processing from the beggining.
void* exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_mixed_sorted_array_full_helping(void *rfdata)
{   
    threadPin(((MESSI_workerdata_ekosmas_lf*)rfdata)->workernumber, maxquerythread);



    isax_node *current_root_node;

    MESSI_workerdata_ekosmas_lf *input_data = (MESSI_workerdata_ekosmas_lf*)rfdata;


        // EKOSMAS: ADD-FAIL
    /*
    if(random_choice == 0 && input_data->index->settings->fail_query_answering == 1){
        if(input_data->thread_fail == 1 && (input_data->already_fail == 0 || input_data->fail_once == 0 )){
            if(rand() % 100 < input_data->index->settings->percentage_of_fail_thread){
                usleep(input_data->index->settings->duration_of_fail_thread * 1000);
                input_data->already_fail = 1;
            }
        }
    }
*/
    // printf("Thread [%d] - search - START\n", input_data->workernumber); fflush(stdout);

    isax_index *index = input_data->index;
    ts_type *paa = input_data->paa;
    
    volatile query_result *bsf_result = *(input_data->bsf_result_p);
    float bsfdisntance = bsf_result->distance;
    const int query_id = input_data->query_id;

    // printf("Thread [%d] - search - Populate arrays - START\n", input_data->workernumber); fflush(stdout);
  
    // A. Populate Queues - Lock-Free Version with Load Balancing (threads are inserting into data queues in a round robin manner!)
    if (input_data->workernumber == 0) {
        //printf("Starting filling querue\n");
        COUNT_QUEUE_FILL_TIME_START
    }
    
    int tnumber = rand() % NUM_PRIORITY_QUEUES;
    unique_leaves_in_arrays_cnt = 0;
    leaves_in_arrays_cnt = 0;
    my_time_for_tree_prunning = 0;
    my_num_subtrees_pruned = 0;
    while (1) {
            int current_root_node_number = __sync_fetch_and_add(input_data->node_counter, 1);
            if(input_data->thread_fail == 1){
                return;
            }
            if(current_root_node_number >= input_data->amountnode) {
                break;
            }

            // EKOSMAS: ADD-FAIL : THIS IS THE SAME WITH THE FOLLOWING
            // if(random_choice == 1 && input_data->index->settings->fail_query_answering == 1){
            //     if(input_data->thread_fail == 1 && (input_data->already_fail == 0 || input_data->fail_once == 0 )){
            //         if(rand() % 100 < input_data->index->settings->percentage_of_fail_thread){
            //             usleep(input_data->index->settings->duration_of_fail_thread * 1000);
            //             input_data->already_fail = 1;
            //         }
            //     }
            // }

            // OLD IMPLEMENTATION
            current_root_node = input_data->nodelist[current_root_node_number];
            COUNT_MY_TIME_START

            add_to_array_data_lf(paa, current_root_node, index, bsfdisntance, input_data->array_lists, &tnumber, input_data->next_queue_data_pos, query_id);
            
            COUNT_MY_TIME_FOR_TREE_PRUNING_END
            my_num_subtrees_pruned++;
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_START
    }

    unsigned long backoff_time = backoff_multiplier_tree_pruning;

    if (my_num_subtrees_pruned) {
        backoff_time *= (unsigned long) BACKOFF_TREE_PRUNING_DELAY_VALUE;
    }
    else {
        backoff_time = 0;
    }

    if (DO_NOT_HELP) {                               // EKOSMAS: ADDED 15/08/2022
        pthread_barrier_wait(input_data->wait_tree_pruning_phase_to_finish);          
    }
    else {
        // A.1. Help unprocessed subtrees
        for (int current_root_node_number = 0; current_root_node_number < input_data->amountnode && all_subtrees_pruned < query_id; current_root_node_number++) {

            // OLD IMPLEMENTATION
            current_root_node = input_data->nodelist[current_root_node_number];

            if (current_root_node->processed == query_id) {
                continue;
            }
            backoff_delay_ulong(backoff_time, &current_root_node->processed, query_id);
            if (current_root_node->processed == query_id) {
                subrees_pruned_helping_avoided_cnt++;
                continue;
            }

            subrees_pruned_helped_cnt++;

            // EKOSMAS: ADD-FAIL : THIS IS THE SAME WITH THE PREVIOUS
            // EKOSMAS: IS HELPER
            // if(random_choice == 1 && input_data->index->settings->fail_query_answering == 1 && input_data->index->settings->fail_helper == 1){
            //     if(input_data->thread_fail == 1 && (input_data->already_fail == 0 || input_data->fail_once == 0 )){
            //         if(rand() % 100 < input_data->index->settings->percentage_of_fail_thread){
            //             usleep(input_data->index->settings->duration_of_fail_thread * 1000);
            //             input_data->already_fail = 1;
            //         }
            //     }
            // }

            add_to_array_data_lf(paa, current_root_node, index, bsfdisntance, input_data->array_lists, &tnumber, input_data->next_queue_data_pos, query_id);

            // // OPT?? IMPLEMENTATION
            // unsigned long cur_index = 0, next_index = 0; 
            // input_data->subtree_prune_helpers_exist[current_root_node_number] = 1;
            // char prunning_helpers_exist = 1;
            // input_data->subtree_prune_helpers_exist[current_root_node_number] = query_id;
            // current_root_node = input_data->nodelist[current_root_node_number];
            // add_to_array_data_lf_opt(paa, current_root_node, index, bsfdisntance, input_data->array_lists, &tnumber, input_data->next_queue_data_pos, query_id, &input_data->subtree[current_root_node_number*ULONG_CACHE_PADDING], &input_data->subtree_fai[current_root_node_number*ULONG_CACHE_PADDING], &cur_index, &next_index, &input_data->subtree_prune_helpers_exist[current_root_node_number], &prunning_helpers_exist, 0, &current_root_node->processed, 0);
            // cur_index = 0;
            // next_index = 0;
            // add_to_array_data_lf_opt(paa, current_root_node, index, bsfdisntance, input_data->array_lists, &tnumber, input_data->next_queue_data_pos, query_id, &input_data->subtree[current_root_node_number*ULONG_CACHE_PADDING], &input_data->subtree_fai[current_root_node_number*ULONG_CACHE_PADDING], &cur_index, &next_index, &input_data->subtree_prune_helpers_exist[current_root_node_number], &prunning_helpers_exist, 1, &current_root_node->processed, 0);
        }
    }

    if (all_subtrees_pruned < query_id) {
        all_subtrees_pruned = query_id;
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_END
    }    

    // ++ count the number of leafs inserted multiple times ++

    // printf ("data_queue %d has size [%d]\n", input_data->workernumber, input_data->next_queue_data_pos[input_data->workernumber]); fflush(stdout);

    // EKOSMAS: ADD-FAIL
    // if(random_choice == 2 && input_data->index->settings->fail_query_answering == 1){
    //     if(input_data->thread_fail == 1 && (input_data->already_fail == 0 || input_data->fail_once == 0 )){
    //         if(rand() % 100 < input_data->index->settings->percentage_of_fail_thread){
    //             usleep(input_data->index->settings->duration_of_fail_thread * 1000);
    //             input_data->already_fail = 1;
    //         }
    //     }
    // }

    // A.2. Create my sorted array
    if (!input_data->sorted_arrays[input_data->workernumber % NUM_PRIORITY_QUEUES] && input_data->next_queue_data_pos[input_data->workernumber % NUM_PRIORITY_QUEUES]) {
        create_sorted_array_from_data_queue(input_data->workernumber % NUM_PRIORITY_QUEUES, input_data->array_lists, input_data->next_queue_data_pos, input_data->sorted_arrays);
    }

    // B. Processing my sorted array
    int checks = 0;                                                                     // EKOSMAS: This is just for debugging. It can be removed!

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_END
    ///printf(" filling querue ended\n");
        COUNT_QUEUE_PROCESS_TIME_START
    ///printf("Starting processing\n");
    }


    // B.1. execute slow path: while a candidate queue node with smaller distance exists, compute actual distances
    sorted_array_t *my_array = input_data->sorted_arrays[input_data->workernumber % NUM_PRIORITY_QUEUES];
    if (my_array && !input_data->queue_finished[input_data->workernumber]) {
        // EKOSMAS: ADD-FAIL : THIS IS THE SAME WITH THE FOLLOWING
        if(random_choice == 3 && input_data->index->settings->fail_query_answering == 1){
            if(input_data->thread_fail == 1 && (input_data->already_fail == 0 || input_data->fail_once == 0 )){
                if(rand() % 100 < input_data->index->settings->percentage_of_fail_thread){
                    usleep(input_data->index->settings->duration_of_fail_thread * 1000);
                    input_data->already_fail = 1;
                }
            }
        }
        help_sorted_array(input_data, my_array, input_data->workernumber % NUM_PRIORITY_QUEUES, &checks, input_data->parallelism_in_subtree);
    }

    if (DO_NOT_HELP) {                               // EKOSMAS: ADDED 15/08/2022
        pthread_barrier_wait(input_data->wait_process_queue);          
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_PROCESS_TIME_END
        //printf(" processing end\n");
        COUNT_QUEUE_FILL_TIME_START
        COUNT_QUEUE_FILL_HELP_TIME_START
    }

    // A.3. Help other sorted arrays to be created
    if (!DO_NOT_HELP) {
        for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
        {
            if (!input_data->sorted_arrays[i] && input_data->next_queue_data_pos[i]) {
                create_sorted_array_from_data_queue(i, input_data->array_lists, input_data->next_queue_data_pos, input_data->sorted_arrays);
            }
        }
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_END
        COUNT_QUEUE_FILL_TIME_END
        COUNT_QUEUE_PROCESS_TIME_START
        COUNT_QUEUE_PROCESS_HELP_TIME_START
    }

    // B.3. Help non-finished sorted arrays
    if (!DO_NOT_HELP) {
        for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
        {
            if (input_data->queue_finished[i] || !input_data->sorted_arrays[i]) {                                           // if queue is completed, skip helping it 
                continue;
            }

            if (!input_data->helper_queue_exist[i]) {                                                               // inform that helpers exist 
                input_data->helper_queue_exist[i] = 1;
            }

            // EKOSMAS: ADD-FAIL : THIS IS THE SAME WITH THE PREVIOUS
            // EKOSMAS: IS HELPER
            if(random_choice == 3 && input_data->index->settings->fail_query_answering == 1 && input_data->index->settings->fail_helper == 1){
                if(input_data->thread_fail == 1 && (input_data->already_fail == 0 || input_data->fail_once == 0 )){
                    if(rand() % 100 < input_data->index->settings->percentage_of_fail_thread){
                        usleep(input_data->index->settings->duration_of_fail_thread * 1000);
                        input_data->already_fail = 1;
                    }
                }
            }

            help_sorted_array(input_data, input_data->sorted_arrays[i], i, &checks, input_data->parallelism_in_subtree);           // help it, by executing slow path
        }
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_PROCESS_HELP_TIME_END
        COUNT_QUEUE_PROCESS_TIME_END
    }

    if (leaves_in_arrays_cnt > 0) {
        COUNT_LEAVES_ARRAY(leaves_in_arrays_cnt);
    }

    if (unique_leaves_in_arrays_cnt > 0) {
        COUNT_UNIQUE_LEAVES_ARRAY(unique_leaves_in_arrays_cnt);
    }


    // EKOSMAS: UNSAFE!
    // // C. Free any element left in my queue
    // // printf("exact_search_worker_inmemory_hybridpqueue_ekosmas_lf: FREE REST QUEUE ELEMENTS\n");fflush(stdout);
    // query_result *n;
    // while(n = pqueue_pop(my_pq))
    // {
    //     free(n);
    // } 

    // EKOSMAS: ADD-FAIL
    if(random_choice == 4 && input_data->index->settings->fail_query_answering == 1){
        if(input_data->thread_fail == 1 && (input_data->already_fail == 0 || input_data->fail_once == 0 )){
            if(rand() % 100 < input_data->index->settings->percentage_of_fail_thread){
                usleep(input_data->index->settings->duration_of_fail_thread * 1000);
                input_data->already_fail = 1;
            }
        }
    }

    // EKOSMAS: ADDED 06/01/2023
    if (CASULONG(&query_answering_time_flag_end, 0, 1)) {
        COUNT_QUERY_ANSWERING_TIME_END
        COUNT_OUTPUT_TIME_END
    }

    if (subrees_pruned_helping_avoided_cnt) {
        COUNT_SUBTREES_PRUNED_HELP_AVOIDED(subrees_pruned_helping_avoided_cnt)
    }

    if (subrees_pruned_helped_cnt) {
        COUNT_SUBTREES_PRUNED_HELPED(subrees_pruned_helped_cnt)
    }

    
}

void add_to_array_data_lf(float *paa, isax_node *node, isax_index *index, float bsf, array_list_t *array_lists, int *tnumber, volatile unsigned long *next_queue_data_pos, const int query_id)
{   

    if (node->processed == query_id) {         // it can only be: node->processed <= query_id
        return;
    }


    //COUNT_CAL_TIME_START
    // ??? EKOSMAS: Why not using SIMD version of the following function?
    // Botao will send a correct SIMD version of the following function
    // calculate lower bound distance
    float distance =  minidist_paa_to_isax(paa, node->isax_values,
                                            node->isax_cardinalities,
                                            index->settings->sax_bit_cardinality,
                                            index->settings->sax_alphabet_cardinality,
                                            index->settings->paa_segments,
                                            MINVAL, MAXVAL,
                                            index->settings->mindist_sqrt);
    //COUNT_CAL_TIME_END
    if(distance < bsf && node->processed < query_id)
    {
        if (node->is_leaf) 
        {   
            unsigned long queue_data_pos = __sync_fetch_and_add(&(next_queue_data_pos[*tnumber]),1);
            array_element_t *array_elem = get_element_at(queue_data_pos, &array_lists[*tnumber]);
            array_elem->distance = distance;
            array_elem->node = node;
    
            leaves_in_arrays_cnt++;

            *tnumber=(*tnumber+1)%NUM_PRIORITY_QUEUES;
        }
        else
        {   
            if (node->left_child->isax_cardinalities != NULL && node->left_child->processed < query_id)                        // ??? EKOSMAS: Can this be NULL, while node is not leaf???
            {
                add_to_array_data_lf(paa, node->left_child, index, bsf, array_lists, tnumber, next_queue_data_pos, query_id);
            }
            if (node->right_child->isax_cardinalities != NULL && node->right_child->processed < query_id)                      // ??? EKOSMAS: Can this be NULL, while node is not leaf???
            {
                add_to_array_data_lf(paa, node->right_child, index, bsf, array_lists, tnumber, next_queue_data_pos, query_id);
            }
        }
    }

    // mark node as processed
    if (node->processed < query_id) {
        node->processed = query_id;
        if (node->is_leaf && distance < bsf) {
            unique_leaves_in_arrays_cnt++;
        }
    }
}

