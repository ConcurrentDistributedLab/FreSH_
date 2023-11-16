//
//  Updated by Eleftherios Kosmas on May 2020.
//



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
#include "ads/isax_query_engine.h"
#include "ads/inmemory_index_engine.h"
#include "ads/inmemory_query_engine.h"
#include "ads/parallel_index_engine.h"
#include "ads/isax_first_buffer_layer.h"
#include "ads/pqueue.h"
#include "ads/sax/sax.h"
#include "ads/isax_node_split.h"
#include <sched.h>
#include <malloc.h>


inline void backoff_delay_char(unsigned long backoff, volatile unsigned char *stop) {
    if (!backoff){
        return;
    }

    volatile unsigned long i;

    for (i = 0; i < backoff && !(*stop); i++)
        ;
}

inline void backoff_delay_lockfree_subtree_copy(unsigned long backoff, isax_node * volatile *stop) {
    if (!backoff){
        return;
    }

    volatile unsigned long i;

    for (i = 0; i < backoff && !(*stop); i++)
        ;
}

inline void backoff_delay_lockfree_subtree_parallel(unsigned long backoff, volatile unsigned char *stop) {
    if (!backoff){
        return;
    }

    volatile unsigned long i;

    for (i = 0; i < backoff && !(*stop); i++)
        ;
}

inline void threadPin(int pid, int max_threads) {
    int cpu_id;

    cpu_id = pid % 24;
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

long int count_ts_in_nodes (isax_node *root_node, const char parallelism_in_subtree, const char recBuf_helpers_exist)
{
    long int my_subtree_nodes = 0;

    if (!root_node -> is_leaf) {
        my_subtree_nodes = count_ts_in_nodes(root_node->left_child, parallelism_in_subtree, recBuf_helpers_exist);       
        my_subtree_nodes += count_ts_in_nodes(root_node->right_child, parallelism_in_subtree, recBuf_helpers_exist);
        return my_subtree_nodes;
    }
    else {
        if (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE ||
            (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP && recBuf_helpers_exist) ||
            (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP_PER_LEAF && root_node->recBuf_leaf_helpers_exist)) {
            if (root_node->fai_leaf_size == 0) {
                return root_node->leaf_size;
            }
            else if (root_node->fai_leaf_size < root_node->leaf_size) {
                printf ("root_node->fai_leaf_size < root_node->leaf_size  !!!!\n");fflush(stdout);
            }
            return root_node->fai_leaf_size;
        }
        else if (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_COW) {
            return root_node->buffer->partial_buffer_size;
        }
        else {
            return root_node->leaf_size;
        }
    }
}


void check_validity_ekosmas_lf(isax_index *index, long int ts_num, const char parallelism_in_subtree) {

    // print the number of threads helped each block (not required for validity)
    // unsigned long total_blocks = ts_num/read_block_length;
    // for (int i=0; i < total_blocks; i++) {
    //     if (block_helpers_num[i]) {
    //         printf("Block [%d] was helped by [%d] threads\n", i, block_helpers_num[i]);
    //     }
    // }

    // count the total number of time series stored into receive buffers
    ts_in_RecBufs_cnt = 0;
    for (int i=0; i < index->fbl->number_of_buffers; i++) {


        parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node = &((parallel_first_buffer_layer_ekosmas_lf*)(index->fbl))->soft_buffers[i];
        if (!current_fbl_node->initialized) {
            continue;
        }


        // int tmp_count = 0;
        for (int k = 0; k < maxquerythread; k++)
        {
            // tmp_count += current_fbl_node->buffer_size[k];
            ts_in_RecBufs_cnt += current_fbl_node->buffer_size[k];
         }

        // printf("RecBuf[%d] contains [%d] iSAX summarries\n", i, tmp_count);
    }

    // print difference with actual total time series in raw file
    // printf ("Total series in RecBufs = [%d] which are [%d] more than total series in raw file\n", ts_in_RecBufs_cnt, ts_in_RecBufs_cnt - ts_num);

    // count the total number of time series stored into tree index
    ts_in_tree_cnt = 0;
    non_empty_subtrees_cnt = 0;
    min_ts_in_subtrees = ts_num;
    max_ts_in_subtrees = 0;
    int cnt_1_10 = 0;
    int cnt_10_100 = 0;
    int cnt_100_1000 = 0;
    int cnt_1000_10000 = 0;
    int cnt_10000_100000 = 0;
    int cnt_100000_1000000 = 0;
    int cnt_1000000_10000000 = 0;
    for (int i=0; i < index->fbl->number_of_buffers; i++) {

        parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node = &((parallel_first_buffer_layer_ekosmas_lf*)(index->fbl))->soft_buffers[i];
        if (!current_fbl_node->initialized) {
            continue;
        }

        non_empty_subtrees_cnt++;
        long int tmp_num = count_ts_in_nodes((isax_node *)current_fbl_node->node, parallelism_in_subtree, current_fbl_node->recBuf_helpers_exist); 
        ts_in_tree_cnt += tmp_num;
        if (tmp_num < min_ts_in_subtrees) {
            min_ts_in_subtrees = tmp_num;
        }
        else if (tmp_num > max_ts_in_subtrees) {
            max_ts_in_subtrees = tmp_num;
        }

        // printf("Subtree[%d] contains [%d] nodes\n", i, tmp_num);

        if (tmp_num < 10) {
            cnt_1_10 ++;
        }
        else if (tmp_num >= 10 && tmp_num < 100) {
            cnt_10_100 ++;
        }
        else if (tmp_num >= 100 && tmp_num < 1000) {
            cnt_100_1000 ++;
        }
        else if (tmp_num >= 1000 && tmp_num < 10000) {
            cnt_1000_10000 ++;
        }
        else if (tmp_num >= 10000 && tmp_num < 100000) {
            cnt_10000_100000 ++;
        }
        else if (tmp_num >= 100000 && tmp_num < 1000000) {
            cnt_100000_1000000 ++;
        }
        else if (tmp_num >= 1000000 && tmp_num < 10000000) {
            cnt_1000000_10000000 ++;
        }

        // if (recBuf_helpers_num[i]) {
        //     printf("RecBuf [%d] was helped by [%d] threads and contains [%d] nodes \n", i, recBuf_helpers_num[i], tmp_num);
        // }
    }

    // printf("\nThere exist [%d] subtrees with 1-9 nodes\n", cnt_1_10);
    // printf("There exist [%d] subtrees with 10-99 nodes\n", cnt_10_100);
    // printf("There exist [%d] subtrees with 100-999 nodes\n", cnt_100_1000);
    // printf("There exist [%d] subtrees with 1000-9999 nodes\n", cnt_1000_10000);
    // printf("There exist [%d] subtrees with 10000-99999 nodes\n", cnt_10000_100000);
    // printf("There exist [%d] subtrees with 100000-999999 nodes\n", cnt_100000_1000000);
    // printf("There exist [%d] subtrees with 1000000-9999999 nodes\n", cnt_1000000_10000000);

    // compare numbers of time series stored into receive buffers and tree index. They have to be the same!!!
    // printf ("Total series in Tree = [%d] which are [%d] more than total series in RecBufs\n", ts_in_tree_cnt, ts_in_tree_cnt - ts_in_RecBufs_cnt);

    // check that all data series have been processed!
    for (int ts_id=0; ts_id < ts_num; ts_id++) {
        if (!ts_processed[ts_id]) {
            printf("--- ERROR : Time series with id [%d] has not been processed!!!! ---\n", ts_id); fflush(stdout);
        }
    }

    if (parallelism_in_subtree == NO_PARALLELISM_IN_SUBTREE) {
        return;
    }

    // check that all iSAX summaries have been iserted into index tree
    unsigned long num_iSAX_processed_from_RecBufs = 0;
    for (int i=0; i < index->fbl->number_of_buffers; i++) {
        parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node = &((parallel_first_buffer_layer_ekosmas_lf*)(index->fbl))->soft_buffers[i];

        if (!current_fbl_node->initialized) {
            continue;
        }

        for (int j=0; j < maxquerythread; j++) {
            for (int k=0; k < current_fbl_node->buffer_size[j]; k++) {
                if(!current_fbl_node->iSAX_processed[j][k]) {
                    printf("--- ERROR : iSAX summary of [%d] recBuf in position ([%d],[%d]) has not been processed!!!! ---", i, j, k); fflush(stdout);
                }
            }
            num_iSAX_processed_from_RecBufs += current_fbl_node->buffer_size[j];
        }

    }

    // printf ("Processed [%d] iSAX summaries from RecBufs\n", num_iSAX_processed_from_RecBufs);
}
    

//Returns an array of N random numbers between start and end. Start and end reprsenting the Smallest and highest Code id of same Numa Node
//On Titan  Machine 2 nodes 0-23 and 24-47
void randomThreadsToFail(int start,int end,int N, int *failedThreads){
    
    if(N == 0 ){
        return;
    }

  int lower = start;  // Lower bound for random numbers
  int upper = end;  // Upper bound for random numbers

  // Seed the random number generator with the current time
  srand(time(0));

  // Create an array to store the generated numbers
  int *numbers = malloc(sizeof(int) * N);

  for (int i = 0; i < N; i++) {
    // Generate a new random number
    int random_number = (rand() % (upper - lower + 1)) + lower;

    // Check if the number has already been generated
    bool found = false;
    for (int j = 0; j < i; j++) {
      if (numbers[j] == random_number) {
        found = true;
        break;
      }
    }

    // If the number has not been generated, add it to the array
    if (!found) {
      numbers[i] = random_number;
    }
    // If the number has already been generated, decrement i so that
    // the loop will run one extra time to generate a new number
    else {
      i--;
    }
  }

    for (int i = 0; i < N; i++) {
            //printf("Number = %d \t", numbers[i]);
            failedThreads[numbers[i]] = 1;
    }
    //printf("\n");

    /*For Debugging
    //Printing Random Numbers
    printf("Random numbers:");
    for (int i = 0; i < N; i++) {
        printf("%d ", numbers[i]);
        
    }
    printf("\n");
    */
}

int *randomPositionsToFail(int chunkSize,float percentage){
    int total_iterations = chunkSize * percentage / 100;
    int *numbers = malloc(sizeof(int) * chunkSize);
    int *randomNumbers = malloc(sizeof(int) * total_iterations);

    srand(time(0));

    for(int i = 0 ; i < chunkSize;i++){
        numbers[i] = 0;
    }

    for(int i = 0 ; i <total_iterations;i++){
        int random = 0 ;
        random = rand() % chunkSize;
        for(int j = 0 ; j < i; j++){
            if(random == randomNumbers[j]){
                i--;
                break;
            }
            randomNumbers[i] = random;
        }

    }

    for(int j = 0 ; j < total_iterations ; j++){
        int tmp = randomNumbers[j];
        numbers[tmp] = 1;
    }
    /* For Debugging
    printf("Random numbers:");
    for (int i = 0; i < total_iterations; i++) {
        printf("%d ", randomNumbers[i]);
    }
    printf("\n");
    */
    return numbers;
}


void index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_func(const char *ifilename, long int ts_num, isax_index *index, const char parallelism_in_subtree)
{
    // A. open input file and check its validity
    // ------------------------------------------------------------------
    FILE * ifile;
    ifile = fopen (ifilename,"rb");
    if (ifile == NULL) {
        fprintf(stderr, "File %s not found!\n", ifilename);
        exit(-1);
    }

    fseek(ifile, 0L, SEEK_END);                                                             
    file_position_type sz = (file_position_type) ftell(ifile);                              // sz = size in bytes
    file_position_type total_records = sz/index->settings->ts_byte_size;                    // total bytes / size (in bytes) of one data series
    fseek(ifile, 0L, SEEK_SET);

    if (total_records < ts_num) {                                                           // check if u have the entire file
        fprintf(stderr, "File %s has only %llu records!\n", ifilename, total_records);
        exit(-1);
    }
        //-----------------------------------------Failing Simulation Initialization-------------------------------------------------------
    //Step 1 Choose Random Threads to fail. 
    int failThreads[maxquerythread];
    for(int i = 0; i < maxquerythread ; i++){
        failThreads[i] = 0 ;
    }
    randomThreadsToFail(0,maxquerythread,index->settings->number_of_fail_threads,failThreads);
    //-------------------------------------Ending of Failing Simulation Initialization-------------------------------------------------------

    // ------------------------------------------------------------------

    // B. Read file in memory (into the "rawfile" array)
    // ------------------------------------------------------------------
    index->settings->raw_filename = malloc(256);                                    
    strcpy(index->settings->raw_filename, ifilename);
    rawfile = malloc(index->settings->ts_byte_size*ts_num);                                    // CHANGED BY EKOSMAS - 06/05/2020
    COUNT_INPUT_TIME_START
    int read_number = fread(rawfile, sizeof(ts_type), index->settings->timeseries_size*ts_num, ifile);
    COUNT_INPUT_TIME_END

    COUNT_OUTPUT_TIME_START
    COUNT_FILL_REC_BUF_TIME_START
    COUNT_FBL_INITIALIZE_START
    index->fbl = (first_buffer_layer *) initialize_pRecBuf_ekosmas_lf(
                                index->settings->initial_fbl_buffer_size,
                                pow(2, index->settings->paa_segments),
                                index->settings->max_total_buffer_size+DISK_BUFFER_SIZE*(PROGRESS_CALCULATE_THREAD_NUMBER-1), index);
    COUNT_FBL_INITIALIZE_TIME_END
    // C. Initialize variables and parallelize the receive buffers' fill in and index constuction
    // ------------------------------------------------------------------
    int nodeid[index->fbl->number_of_buffers];                                                      // not used!
    pthread_t threadid[maxquerythread];                                                             // thread's id array
    buffer_data_inmemory_ekosmas_lf *input_data = malloc(sizeof(buffer_data_inmemory_ekosmas_lf)*(maxquerythread));       // array of structs with informations we need for the workers - param for the threads - num of structs == num
    unsigned long next_block_to_process = 0;
    int node_counter = 0;                                                       // required for tree construction using fai

    pthread_barrier_t wait_summaries_to_compute;                                // required to ensure that workers will fill in buffers only after all summaries have been computed
    pthread_barrier_init(&wait_summaries_to_compute, NULL, maxquerythread);

    unsigned long total_blocks = ts_num/read_block_length;
    if (read_block_length*total_blocks < ts_num) {
        total_blocks++;
    }

    unsigned long total_groups = read_block_length/ts_group_length;
    if (ts_group_length*total_groups < read_block_length) {
        total_groups++;
    }


    block_processed = malloc(sizeof(unsigned char)*total_blocks);
    group_processed = malloc(sizeof(unsigned char *)*total_blocks);
    // next_ts_group_read_in_block = malloc(sizeof(unsigned long)*total_blocks);
    next_ts_group_read_in_block = malloc(sizeof(next_ts_group)*total_blocks);
    next_ts_read_in_group = malloc(sizeof(next_ts_in_group *)*total_blocks);
    next_ts_read_in_group_fai = malloc(sizeof(next_ts_in_group *)*total_blocks);
    block_helper_exist = malloc(sizeof(unsigned char)*total_blocks);
    group_helpers_exist = malloc(sizeof(unsigned char *)*total_blocks);
    // block_helpers_num = malloc(sizeof(unsigned char)*total_blocks);
    for (int i=0; i< total_blocks; i++) {
        block_processed[i] = 0;
        group_processed[i] = calloc(total_groups, sizeof(unsigned char));
        next_ts_group_read_in_block[i].num = 0;
        next_ts_read_in_group[i] = calloc(total_groups, sizeof(next_ts_in_group));
        next_ts_read_in_group_fai[i] = calloc(total_groups, sizeof(next_ts_in_group));
        block_helper_exist[i] = 0;
        group_helpers_exist[i] = calloc(total_groups, sizeof(unsigned char));
        // block_helpers_num[i] = 0;
    }

    recBuf_helpers_num = malloc(sizeof(unsigned char)*index->fbl->number_of_buffers);
    for (int i=0; i< index->fbl->number_of_buffers; i++) {
        recBuf_helpers_num[i] = 0;
    }      

    ts_processed = malloc(sizeof(unsigned char)*ts_num);
    for (int i=0; i< ts_num; i++) {
        ts_processed[i] = 0;
    }
    
    all_blocks_processed = 0;
    all_RecBufs_processed = 0;

    for (int i = 0; i < maxquerythread; i++)
    {
        input_data[i].index                     = index;      
        input_data[i].workernumber              = i;
        input_data[i].shared_start_number       = &next_block_to_process;
        input_data[i].ts_num                    = ts_num;                           
        input_data[i].wait_summaries_to_compute = &wait_summaries_to_compute;
        input_data[i].parallelism_in_subtree    = parallelism_in_subtree;
        input_data[i].node_counter              = &node_counter;                    // required for tree construction using fai
        input_data[i].sax                       = (void *)memalign(CACHE_LINE_SIZE, index->settings->sax_byte_size);
         if(failThreads[i] == 1){
            input_data[i].thread_fail = 1;
        }
        else{
            input_data[i].thread_fail = 0;
        }
        input_data[i].fail_once = index->settings->fail_once; //Can be 0,1,2
        input_data[i].already_fail = 0; //Note, here we dont have multiple cases as query answering the index creation is one (multiple queries).
        
    }

    // create worker threads to fill in receive buffers (with iSAX summaries)
    for (int i = 0; i < maxquerythread; i++)
    {
        pthread_create(&(threadid[i]),NULL,index_creation_pRecBuf_worker_new_ekosmas_lock_free_fai_only_after_help,(void*)&(input_data[i]));
    }

    // wait for worker threads to complete
    for (int i = 0; i < maxquerythread; i++)
    {
        pthread_join(threadid[i],NULL);
    }
    // ------------------------------------------------------------------

    free(input_data);
    fclose(ifile);
    // EKOSMAS : REMOVED 06/01/2023
    // COUNT_CREATE_TREE_INDEX_TIME_END
    // COUNT_OUTPUT_TIME_END


    //check_validity_ekosmas_lf(index, ts_num, parallelism_in_subtree);
}


void index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_with_lockfree_parallelism_in_subtree_announce_after_help_per_leaf(const char *ifilename, long int ts_num, isax_index *index)
{
    index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_func(ifilename, ts_num, index, LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP_PER_LEAF);
}





// EKOSMAS: FUNCTION NEED FOR DEBUG - TO BE DELETED
inline unsigned long count_nodes_in_RecBuf_2(parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node) 
{
    unsigned long subtree_nodes = 0;
    for (int k = 0; k < maxquerythread; k++) {
        subtree_nodes += current_fbl_node->buffer_size[k];
    }

    return subtree_nodes;
}
// EKOSMAS: FUNCTION NEED FOR DEBUG - TO BE DELETED
inline unsigned long print_nodes_in_RecBuf(parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node) 
{
    unsigned long subtree_nodes = 0;
    for (int k = 0; k < maxquerythread; k++) {
        subtree_nodes += current_fbl_node->buffer_size[k];
        printf ("Sub-Buffer of process [%d] contains [%d] iSAX summaries\n", k, current_fbl_node->buffer_size[k]);
    }

    return subtree_nodes;
}

inline void scan_RecBuf_for_unprocessed_iSAX_summaries(isax_index *index, parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node, isax_node_record *r, volatile unsigned char *stop, unsigned long my_id, const char is_helper, const char lockfree_parallelism_in_subtree) 
{
    for (int k = 0; k < maxquerythread; k++) {
        for (unsigned long i = 0; i < current_fbl_node->buffer_size[k] && !(*stop) ; i++)
        {
            if (!current_fbl_node->iSAX_processed[k][i]) {

                r->sax = (sax_type *) &(((current_fbl_node->sax_records[k]))[i*index->settings->paa_segments]);
                r->position = (file_position_type *) &((file_position_type *)(current_fbl_node->pos_records[k]))[i];
                r->insertion_mode = NO_TMP | PARTIAL;

                add_record_to_node_inmemory_parallel_lockfree_announce(index, current_fbl_node, r, my_id, maxquerythread, is_helper, lockfree_parallelism_in_subtree);
                
                if (!current_fbl_node->iSAX_processed[k][i]) {
                    current_fbl_node->iSAX_processed[k][i] = 1;
                }  
            }
        }
    }
}

unsigned long populate_tree_lock_free_announce(isax_index *index,buffer_data_inmemory_ekosmas_lf *input_data, parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node, isax_node_record *r, unsigned long my_id, const char is_helper, const char lockfree_parallelism_in_subtree) 
{

    // EKOSMAS: ADD-FAIL
    // EKOSMAS: CHECK IF HELPER
        //Note: PERFORM FAILURE:If this phase is failing AND (if you are not helper OR fail if you are helper and failing is enabled for helpers).
    // if(index->settings->fail_tree_construction == 1 && ((is_helper && index->settings->fail_helper) || (!is_helper))){
    //     if(input_data->thread_fail == 1 && (input_data->already_fail == 0 || input_data->fail_once == 0 )){
    //         if(rand() % 100 < input_data->index->settings->percentage_of_fail_thread){
    //             usleep(input_data->index->settings->duration_of_fail_thread * 1000);
    //             input_data->already_fail = 1;
    //         }
    //     }
    // }

    isax_node *root_node = NULL;

    // create and initialize a new fbl leaf node
    if (!current_fbl_node->node) {
        root_node = isax_root_node_init_lockfree_announce(current_fbl_node->mask, index->settings->initial_leaf_buffer_size, maxquerythread, lockfree_parallelism_in_subtree, current_fbl_node);
    }

    // try to establish root node
    if (root_node != NULL && (current_fbl_node->node || !CASPTR(&current_fbl_node->node, NULL, root_node))) {
        // free memory
        isax_tree_destroy_lockfree(root_node);
    }

    root_node = current_fbl_node->node;

    // populate tree
    unsigned long subtree_nodes = 0;
    unsigned long iSAX_group = 0;
    unsigned long prev_iSAX_group_id = 0;
    unsigned int process_recBuf_id = 0;
    unsigned int prev_recBuf_iSAX_num = 0;
    char helpers_exist = 0;

    unsigned long num_added = 0;

    while (!current_fbl_node->processed) {
        prev_iSAX_group_id = iSAX_group;

        if (lockfree_parallelism_in_subtree != LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE && !current_fbl_node->recBuf_helpers_exist) {
            iSAX_group = current_fbl_node->next_iSAX_group;
            current_fbl_node->next_iSAX_group = iSAX_group+1;               // EKOSMAS: ERROR: This is problematic, since the counter may return back
        }
        else {
            iSAX_group = __sync_fetch_and_add(&(current_fbl_node->next_iSAX_group),1);
        }
        
        #ifdef DEBUG
        if (iSAX_group && iSAX_group <= prev_iSAX_group_id) {                                                   // EKOSMAS: DEBUG
            printf ("\nCAUTION: populate_tree_lock_free_announce: Counter went back!!\n\n"); fflush(stdout);
            getchar();
        }
        #endif

        while (process_recBuf_id < maxquerythread && iSAX_group >= current_fbl_node->buffer_size[process_recBuf_id] + prev_recBuf_iSAX_num) {
            prev_recBuf_iSAX_num += current_fbl_node->buffer_size[process_recBuf_id];
            process_recBuf_id++;
        }

        if (process_recBuf_id == maxquerythread) {
            break;
        }

        if (!helpers_exist && iSAX_group > prev_iSAX_group_id + 1) {                                            // performance enhancement
            helpers_exist = 1;                                                      
        }

        int k = process_recBuf_id;
        if (k<0) {                                                                                              // EKOSMAS: DEBUG
            printf("ERRROR!!! - k equals [%d] but it can not be negative!\n", k); fflush(stdout);
            getchar();
        }

        int i = iSAX_group - prev_recBuf_iSAX_num;
        r->sax = (sax_type *) &(((current_fbl_node->sax_records[k]))[i*index->settings->paa_segments]);
        r->position = (file_position_type *) &((file_position_type *)(current_fbl_node->pos_records[k]))[i];
        r->insertion_mode = NO_TMP | PARTIAL;

        // Add record to index
        add_record_to_node_inmemory_parallel_lockfree_announce(index, current_fbl_node, r, my_id, maxquerythread, is_helper, lockfree_parallelism_in_subtree);

        if (!current_fbl_node->iSAX_processed[k][i]) {
            current_fbl_node->iSAX_processed[k][i] = 1;
        }

        subtree_nodes++;     
    }

    if ((is_helper || helpers_exist) && !current_fbl_node->processed) {                    // performance enhancement
        scan_RecBuf_for_unprocessed_iSAX_summaries (index, current_fbl_node, r, &current_fbl_node->processed, my_id, is_helper, lockfree_parallelism_in_subtree);
    }

    if (!current_fbl_node->processed) {
        current_fbl_node->processed = 1;
    }

    return subtree_nodes;
}


inline unsigned long count_nodes_in_RecBuf_for_subtree_copy(parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node, isax_node * volatile *stop) 
{
    unsigned long subtree_nodes = 0;
    for (int k = 0; k < maxquerythread && !(*stop); k++) {
        subtree_nodes += current_fbl_node->buffer_size[k];
    }

    return subtree_nodes;
}
inline unsigned long count_nodes_in_RecBuf_for_subtree_parallel(parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node, volatile unsigned char *stop) 
{
    unsigned long subtree_nodes = 0;
    for (int k = 0; k < maxquerythread && !(*stop); k++) {
        subtree_nodes += current_fbl_node->buffer_size[k];
    }

    return subtree_nodes;
}
static inline void scan_for_unprocessed_RecBufs(isax_index *index,buffer_data_inmemory_ekosmas_lf *input_data, isax_node_record *r, unsigned long my_id, const char parallelism_in_subtree) 
{
    unsigned long backoff_time = backoff_multiplier_tree_construction;

    if (my_num_subtree_construction) {
        backoff_time *= (unsigned long) BACKOFF_SUBTREE_DELAY_PER_NODE;
    }
    else {
        backoff_time = 0;
    }

    for (int i=0; i < index->fbl->number_of_buffers && !all_RecBufs_processed; i++) {

        parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node = &((parallel_first_buffer_layer_ekosmas_lf*)(index->fbl))->soft_buffers[i];

        if (!current_fbl_node->initialized || 
            (parallelism_in_subtree != NO_PARALLELISM_IN_SUBTREE && current_fbl_node->processed) || 
            (parallelism_in_subtree == NO_PARALLELISM_IN_SUBTREE && current_fbl_node->node)) {
            continue;
        }

        unsigned long num_nodes; 

        // UNCOMMENT THE FOLLOWING TO DISABLE BACKOFF
        // if (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP_PER_LEAF ||
        //     parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP ||
        //     parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE) {       //  do not backoff in this case
        //     ;   
        // }
        // else 
        if (parallelism_in_subtree != NO_PARALLELISM_IN_SUBTREE) {
            num_nodes = count_nodes_in_RecBuf_for_subtree_parallel(current_fbl_node, &current_fbl_node->processed);
            backoff_delay_lockfree_subtree_parallel(backoff_time*num_nodes, &current_fbl_node->processed);
        }
        // EKOSMAS: COMMENT OUT THE FOLLOWING TO DISABLE BACKOFF
        else {
            num_nodes = count_nodes_in_RecBuf_for_subtree_copy(current_fbl_node, &current_fbl_node->node);
            backoff_delay_lockfree_subtree_copy(backoff_time*num_nodes, &current_fbl_node->node);
        }

        if ((parallelism_in_subtree != NO_PARALLELISM_IN_SUBTREE && current_fbl_node->processed) || 
            (parallelism_in_subtree == NO_PARALLELISM_IN_SUBTREE && current_fbl_node->node)) {
            recBufs_helping_avoided_cnt++;
            continue;
        }

        recBufs_helped_cnt++;

        if (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP ||
                 parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP_PER_LEAF){
            // EKOSMAS: This should be like that, but current_fbl_node->recBuf_helpers_exist is also used to acquire
            // iSAX_groups by executing FAI when helpers arrive in the receive buffer. So, for LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP
            // this flag is also used to follow expeditive/standard mode during insertion in the subtree.
            // if (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP && !current_fbl_node->recBuf_helpers_exist) {
            if (!current_fbl_node->recBuf_helpers_exist) {
                current_fbl_node->recBuf_helpers_exist = 1;
            }

            populate_tree_lock_free_announce (index,input_data, current_fbl_node, r, my_id, 1, parallelism_in_subtree);
        }
    }

    if (!all_RecBufs_processed) {
        all_RecBufs_processed = 1;
    }

    if (recBufs_helping_avoided_cnt) {
        COUNT_SUBTREE_HELP_AVOIDED(recBufs_helping_avoided_cnt)
    }    
    if (recBufs_helped_cnt) {
        COUNT_SUBTREES_HELPED(recBufs_helped_cnt)
    }    
}
static inline void tree_index_creation_from_pRecBuf_fai_lock_free(void *transferdata, const char parallelism_in_subtree)
{
    buffer_data_inmemory_ekosmas_lf *input_data = (buffer_data_inmemory_ekosmas_lf*) transferdata;
    isax_index *index = input_data->index;
    int j;
    
    isax_node_record *r = malloc(sizeof(isax_node_record));

    while(!all_RecBufs_processed)
    {
        // acquire the next receive buffer
        j = __sync_fetch_and_add(input_data->node_counter,1);
        if( j >= index->fbl->number_of_buffers)
        {
            break;
        }
        parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node = &((parallel_first_buffer_layer_ekosmas_lf*)(index->fbl))->soft_buffers[j];
        
        // check if the acquired receive buffer contains elements
        if (!current_fbl_node->initialized) {
            continue;
        }

        COUNT_MY_TIME_START
        my_num_subtree_nodes += populate_tree_lock_free_announce (index,input_data, current_fbl_node, r, input_data->workernumber, 0, parallelism_in_subtree);
        COUNT_MY_TIME_FOR_SUBTREE_END
        my_num_subtree_construction++;
    }

    if (!DO_NOT_HELP) {     // there is no need for a barrier here, since query answering threads will be spawn only after all index worker threads finish
        scan_for_unprocessed_RecBufs(index,input_data, r, input_data->workernumber, parallelism_in_subtree);
    }
    
    // free(r);                                                                                     // EKOSMAS JULY 31, 2020: This is dangerous, since some helpers may still access r!
}
int random_choice = 0;


inline void store_isax_in_pRecBuf(buffer_data_inmemory_ekosmas_lf* input_data, isax_index *index, unsigned long ts_id) 
{
    file_position_type pos;
    int sax_byte_size = index->settings->sax_byte_size;
    int paa_segments = index->settings->paa_segments;
    
    sax_type *sax = malloc(sax_byte_size);          // EKOSMAS: 24-08-2022 - Remove this malloc!

    if(sax_from_ts( 
                (ts_type *)&rawfile[ts_id*index->settings->timeseries_size],
                input_data->sax, 
                // sax, 
                index->settings->ts_values_per_paa_segment,
                paa_segments, 
                index->settings->sax_alphabet_cardinality,
                index->settings->sax_bit_cardinality) 
        == SUCCESS)
    {
        pos = (file_position_type)(ts_id * index->settings->timeseries_size);

        // isax_pRecBuf_index_insert_inmemory(
        //             index, 
        ////             input_data->sax, 
        //             sax, 
        //             &pos, 
        //             input_data->lock_firstnode,
        //             input_data->workernumber,
        //             maxquerythread);


        // Create mask for the first bit of the sax representation
        root_mask_type first_bit_mask = 0x00;
        CREATE_MASK(first_bit_mask, index, input_data->sax);
        // CREATE_MASK(first_bit_mask, index, sax);

        insert_to_pRecBuf_lock_free(
                            (parallel_first_buffer_layer_ekosmas_lf*)(index->fbl), 
                            input_data->sax, 
                            // sax, 
                            &pos,
                            first_bit_mask, 
                            index,
                            input_data->workernumber,
                            maxquerythread,
                            input_data->parallelism_in_subtree);


    }
    else
    {
        fprintf(stderr, "error: cannot insert record in index, since sax representation failed to be created");
    }
}

static inline void scan_for_unprocessed_ts(buffer_data_inmemory_ekosmas_lf* input_data, isax_index *index, unsigned long ts_start, unsigned long ts_end, volatile unsigned char *stop) 
{
    for (unsigned long ts_id = ts_start; ts_id < ts_end && !(*stop) ; ts_id++)
    {
        if (!ts_processed[ts_id]) {
            store_isax_in_pRecBuf(input_data, index, ts_id);
            if (!ts_processed[ts_id]) {
                ts_processed[ts_id] = 1;
            }  
        }
    }
}

static void process_group(unsigned long ts_group, unsigned long block_num, unsigned long total_groups_in_block, unsigned long ts_group_start, unsigned long ts_group_end, buffer_data_inmemory_ekosmas_lf* input_data, isax_index *index, char is_helper, char fai_only_after_help) {
    unsigned long ts_id, tmp;
    while (!group_processed[block_num][ts_group]) {
        if (fai_only_after_help && !group_helpers_exist[block_num][ts_group]) {
            tmp = next_ts_read_in_group[block_num][ts_group].num;
            next_ts_read_in_group[block_num][ts_group].num = tmp+1;
        }
        else {
            if (!next_ts_read_in_group_fai[block_num][ts_group].num && next_ts_read_in_group[block_num][ts_group].num) {
                CASULONG(&next_ts_read_in_group_fai[block_num][ts_group].num, 0, next_ts_read_in_group[block_num][ts_group].num);
            }
            tmp = __sync_fetch_and_add(&next_ts_read_in_group_fai[block_num][ts_group].num, 1);
        }
        ts_id = ts_group_start + tmp;

        if (ts_id >= ts_group_end)
            break;

        if (!ts_processed[ts_id]) {
            store_isax_in_pRecBuf(input_data, index, ts_id);
            if (!ts_processed[ts_id]) {
                ts_processed[ts_id] = 1;
            }
        }
    }

    if ((is_helper || group_helpers_exist[block_num][ts_group]) && !group_processed[block_num][ts_group]) {                    // performance enhancement
        scan_for_unprocessed_ts(input_data, index, ts_group_start, ts_group_end, &group_processed[block_num][ts_group]);
    }
    else {
        COUNT_MY_TIME_FOR_BLOCKS_END
        my_num_blocks_processed++;        
    }

    if (!group_processed[block_num][ts_group]) {
        group_processed[block_num][ts_group] = 1;
    }

}

static inline void scan_for_unprocessed_groups(unsigned long block_num, buffer_data_inmemory_ekosmas_lf* input_data, isax_index *index, unsigned long total_groups_in_block, unsigned long my_ts_start, unsigned long my_ts_end, volatile unsigned char *stop, char fai_only_after_help)
{
    for (unsigned long ts_group = 0; ts_group < total_groups_in_block && !*stop; ts_group++) {
        unsigned long ts_group_start = my_ts_start + ts_group*ts_group_length;
        unsigned long ts_group_end;

        if (ts_group == total_groups_in_block-1)  {
            ts_group_end = my_ts_end; 
        }
        else {
            ts_group_end = ts_group_start + ts_group_length;
        }

        if (!group_helpers_exist[block_num][ts_group]) {
            group_helpers_exist[block_num][ts_group] = 1;
        }

        process_group(ts_group, block_num, total_groups_in_block, ts_group_start, ts_group_end, input_data, index, 1, fai_only_after_help);
    }

    if (!block_processed[block_num]) {
        block_processed[block_num] = 1;
    }

}


static void process_block(unsigned long block_num, unsigned long total_blocks, unsigned long total_ts_num, buffer_data_inmemory_ekosmas_lf* input_data, isax_index *index, char is_helper, char fai_only_after_help)
{

    // EKOSMAS: ADD-FAIL
    // EKOSMAS: check if it is helper against a script-defined variable
    //Note: PERFORM FAILURE:If this phase is failing AND (if you are not helper OR fail if you are helper and failing is enabled for helpers).
    // if(input_data->index->settings->fail_summarization == 1 && ((is_helper && input_data->index->settings->fail_helper) || (!is_helper))){
    //     if(input_data->thread_fail == 1 && (input_data->already_fail == 0 || input_data->fail_once == 0 )){
    //         if(rand() % 100 < input_data->index->settings->percentage_of_fail_thread){
    //             usleep(input_data->index->settings->duration_of_fail_thread * 1000);
    //             input_data->already_fail = 1;
    //         }
    //     }
    // }

    unsigned long my_ts_start, my_ts_end, ts_id;

    my_ts_start = block_num*read_block_length;
    if (block_num == total_blocks-1)  {                                                   // there may still remain some more data series (i.e. less than #read_block_length)
        my_ts_end = total_ts_num; 
    }
    else {
        my_ts_end = my_ts_start + read_block_length;
    }

    unsigned long total_groups_in_block = (my_ts_end-my_ts_start) / ts_group_length;
    if (total_groups_in_block*ts_group_length < my_ts_end-my_ts_start) {
        total_groups_in_block++;
    }

    unsigned long prev_group_id = 0;
    char helpers_exist = 0;

    if (!is_helper) {
        COUNT_MY_TIME_START
    }

    while (!block_processed[block_num]) {
        unsigned long ts_group;
        
        if (fai_only_after_help && !block_helper_exist[block_num]) {
            ts_group = next_ts_group_read_in_block[block_num].num;
            // next_ts_group_read_in_block[block_num].num++;                       // EKOSMAS: ERROR: This is problematic, since a ts_group may be lost
            next_ts_group_read_in_block[block_num].num = ts_group+1;               // EKOSMAS: ERROR: This is again problematic, since the counter may return back
        }
        else {
            ts_group = __sync_fetch_and_add(&next_ts_group_read_in_block[block_num].num, 1);
        }

        if (ts_group > prev_group_id + 1) {                                         // performance enhancement
            helpers_exist = 1;                                                      // EKOSMAS: helpers_exist can be replaced with block_helper_exist[block_num], after changing the corresponding line during scan_for_unprocessed_blocks in order to set this bit all the time and not only when fai_only_after_help!=0
        }

        unsigned long ts_group_start = my_ts_start + ts_group*ts_group_length;
        unsigned long ts_group_end;
        
        if (ts_group >= total_groups_in_block) {
            break;
        }
        
        if (ts_group == total_groups_in_block-1)  {
            ts_group_end = my_ts_end; 
        }
        else {
            ts_group_end = ts_group_start + ts_group_length;
        }

        #ifdef DEBUG
        if (ts_group && ts_group <= prev_group_id ) {
            printf ("\nCAUTION: process_block: Counter went back!!\n\n"); fflush(stdout);
            getchar();
        }
        #endif

        if (is_helper && !group_helpers_exist[block_num][ts_group]) {
            group_helpers_exist[block_num][ts_group] = 1;
        }

        process_group(ts_group, block_num, total_groups_in_block, ts_group_start, ts_group_end, input_data, index, is_helper, fai_only_after_help);

        prev_group_id = ts_group;                                                         // performance enhancement
    }

    if ((is_helper || helpers_exist) && !block_processed[block_num]) {                    // performance enhancement
        scan_for_unprocessed_groups(block_num, input_data, index, total_groups_in_block, my_ts_start, my_ts_end, &block_processed[block_num], fai_only_after_help);
    }
    else {
        COUNT_MY_TIME_FOR_BLOCKS_END
        my_num_blocks_processed++;        
    }

    if (!block_processed[block_num]) {
        block_processed[block_num] = 1;
    }
}

static inline void scan_for_unprocessed_blocks(buffer_data_inmemory_ekosmas_lf* input_data, isax_index *index, unsigned long total_blocks, char fai_only_after_help) 
{
    unsigned long total_ts_num = input_data->ts_num;
    unsigned long my_id = input_data->workernumber;

    unsigned long start_block_num = (my_id + 1) % total_blocks;
    // unsigned long start_block_num = my_id * (total_blocks/num_of_threads);       // EKOSMAS: 25/08/2022: performance enhancement
    unsigned long block_num = start_block_num;

    unsigned long backoff_time = backoff_multiplier_summarization;

    if (my_num_blocks_processed) {
        backoff_time *= (unsigned long) BACKOFF_BLOCK_DELAY_VALUE;
    }
    else {
        backoff_time = 0;
    }

    do
    {
        if (all_blocks_processed) {
            break;
        }
        else if (block_processed[block_num]) {
            block_num = (block_num+1)%total_blocks;
            continue;
        }

        backoff_delay_char(backoff_time, &block_processed[block_num]);

        if (block_processed[block_num]) {
            blocks_helping_avoided_cnt++;
            block_num = (block_num+1)%total_blocks;
            continue;
        }

        blocks_helped_cnt++;
        // __sync_fetch_and_add(&block_helpers_num[block_num], 1);

        if (fai_only_after_help && !block_helper_exist[block_num]) {
            block_helper_exist[block_num] = 1;
        }

        process_block(block_num, total_blocks, total_ts_num, input_data, index, 1, fai_only_after_help);

        block_num = (block_num+1)%total_blocks;
    } while (block_num != start_block_num);

    if (!all_blocks_processed)
        all_blocks_processed = 1;

    if (blocks_helping_avoided_cnt) {
        COUNT_BLOCK_HELP_AVOIDED(blocks_helping_avoided_cnt)
    }

    if (blocks_helped_cnt) {
        COUNT_BLOCKS_HELPED(blocks_helped_cnt)
    }
}



// EKOSMAS: Why not using only one of index_creation_pRecBuf_worker_new_ekosmas_lock_free_full_fai and
// index_creation_pRecBuf_worker_new_ekosmas_lock_free_fai_only_after_help functions with the corresponding parameters?

// Lock-Free FAI (per ts of a block) only after a helper exists (9993, 99939, 9995, 9959, 9997, 99979, 9999, 99999)
void* index_creation_pRecBuf_worker_new_ekosmas_lock_free_fai_only_after_help(void *transferdata)
{
    buffer_data_inmemory_ekosmas_lf *input_data = (buffer_data_inmemory_ekosmas_lf*) transferdata;

    threadPin(input_data->workernumber, maxquerythread);

    unsigned long total_ts_num = input_data->ts_num;
    unsigned long total_blocks = total_ts_num/read_block_length;
    if (read_block_length*total_blocks < total_ts_num) {
        total_blocks++;
    }   

    // EKOSMAS: ADD-FAIL
    /*
    if(input_data->index->settings->fail_summarization == 1){
        if(input_data->thread_fail == 1 && (input_data->already_fail == 0 || input_data->fail_once == 0 )){
            if(rand() % 100 < input_data->index->settings->percentage_of_fail_thread){
                usleep(input_data->index->settings->duration_of_fail_thread * 1000);
                input_data->already_fail = 1;
            }
        }
    }
    */
    isax_index *index = input_data->index;
    unsigned long *next_block_to_process = input_data->shared_start_number;
    unsigned long block_num;
    while(!all_blocks_processed)
    {

        block_num = __sync_fetch_and_add(next_block_to_process, 1);
        if(input_data->thread_fail == 1){
            return;
        }
        if(block_num >= total_blocks)
        {
            break;
        }

        process_block(block_num, total_blocks, total_ts_num, input_data, index, 0, 1);
    }

    if (DO_NOT_HELP) {                                  // EKOSMAS: ADDED 01/07/2020
        pthread_barrier_wait(input_data->wait_summaries_to_compute);          
    }
    else {
        scan_for_unprocessed_blocks(input_data, index, total_blocks, 1);
    }

    // // EKOSMAS: ADD-FAIL
    // if(input_data->index->settings->fail_summarization == 1){
    //     if(input_data->thread_fail == 1 && (input_data->already_fail == 0 || input_data->fail_once == 0 )){
    //         if(rand() % 100 < input_data->index->settings->percentage_of_fail_thread){
    //             usleep(input_data->index->settings->duration_of_fail_thread * 1000);
    //             input_data->already_fail = 1;
    //         }
    //     }
    // }

    // if (input_data->workernumber == 0) {
    if (CASULONG(&fill_rec_bufs_time_flag_end, 0, 1)) {     // EKOSMAS: UPDATED 06/01/2023
        ///printf("Stage1 completed\n");
        COUNT_FILL_REC_BUF_TIME_END
        COUNT_CREATE_TREE_INDEX_TIME_START
        //printf("Stage2 started\n");

    }

    // EKOSMAS: ADD-FAIL
    // if(input_data->index->settings->fail_tree_construction == 1 ){
    //     if(input_data->thread_fail == 1 && (input_data->already_fail == 0 || input_data->fail_once == 0 )){
    //         if(rand() % 100 < input_data->index->settings->percentage_of_fail_thread){
    //             usleep(input_data->index->settings->duration_of_fail_thread * 1000);
    //             input_data->already_fail = 1;
    //         }
    //     }
    // }

    tree_index_creation_from_pRecBuf_fai_lock_free(transferdata, input_data->parallelism_in_subtree);     

    // EKOSMAS: ADD-FAIL
    // if(input_data->index->settings->fail_tree_construction == 1){
    //     if(input_data->thread_fail == 1 && (input_data->already_fail == 0 || input_data->fail_once == 0 )){
    //         if(rand() % 100 < input_data->index->settings->percentage_of_fail_thread){
    //             usleep(input_data->index->settings->duration_of_fail_thread * 1000);
    //             input_data->already_fail = 1;
    //         }
    //     }
    // }

    // EKOSMAS: ADDED 06/01/2023
    if (CASULONG(&create_tree_index_time_flag_end, 0, 1)) {
        COUNT_CREATE_TREE_INDEX_TIME_END
        COUNT_OUTPUT_TIME_END
        //printf("Stage2 completed\n");

    }    
}



