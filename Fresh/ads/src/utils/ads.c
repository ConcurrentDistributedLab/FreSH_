//
//  main.c
//  isaxlib
//
//  Created by Kostas Zoumpatianos on 3/12/12.
//  Copyright 2012 University of Trento. All rights reserved.
//
//  Updated by Eleftherios Kosmas on May 2020.
//
#define _GNU_SOURCE

#define PRODUCT "----------------------------------------------\
\nThis is the Adaptive Leaf iSAX index.\n\
Copyright (C) 2011-2014 University of Trento.\n\
----------------------------------------------\n\n"
#ifdef VALUES
#include <values.h>
#endif

#include "../../config.h"
#include "../../globals.h"
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include <math.h>
#include <getopt.h>
#include <time.h>
#include <float.h>
#include <sched.h>

#include "ads/sax/sax.h"
#include "ads/sax/ts.h"
#include "ads/isax_visualize_index.h"
#include "ads/isax_file_loaders.h"
#include "ads/isax_visualize_index.h"
#include "ads/isax_first_buffer_layer.h"
#include "ads/isax_query_engine.h"
#include "ads/inmemory_query_engine.h"
#include "ads/parallel_inmemory_query_engine.h"
#include "ads/inmemory_index_engine.h"
#include "ads/parallel_query_engine.h"
#include "ads/parallel_index_engine.h"
#include "ads/inmemory_topk_engine.h"
//#define PROGRESS_CALCULATE_THREAD_NUMBER 4
//#define PROGRESS_FLUSH_THREAD_NUMBER 4
//#define QUERIES_THREAD_NUMBER 4
//#define DISK_BUFFER_SIZE 32

int main (int argc, char **argv)
{
    static char * dataset = "/home/ekosmas/datasets/dataset10GB.bin";
    static char * queries = "/home/botao/document/";
    static char * dataset_output = NULL;
    static char * index_path = "/home/botao/document/myexperiment/";
    static char * labelset="/home/botao/document/myexperiment/";
    static long int dataset_size = 10485760;//testbench
    static int queries_size = 10;
    static int time_series_size = 256;
    static int paa_segments = 16;
    static int sax_cardinality = 8;
    static int leaf_size = 2000;
    static int min_leaf_size = 2000;
    static int initial_lbl_size = 2000;
    static int flush_limit = 1000000;
    static int initial_fbl_size = 100;
    static char use_index = 0;
    static int complete_type = 0;
    static int total_loaded_leaves = 1;
    static int tight_bound = 0;
    static int aggressive_check = 0;
    static float minimum_distance = FLT_MAX;
    static int serial_scan = 0;
    static char knnlabel = 0;
    static int min_checked_leaves = -1;
    static int cpu_control_type = 80;
    static char inmemory_flag=1;
    static int number_of_threads_fail = 0;
    static unsigned int duration_of_thread_fail = 0;
    static int percentage_of_thread_fail = 0;
    static int fail_once = 0;
    static int fail_summarization = 0;
    static int fail_tree_construction = 0;
    static int fail_query_answering = 0;
    static int fail_helper = 0;
    static int helpers = 0;
    int calculate_thread=8;
    int  function_type = 9994;
    maxreadthread=5;
    read_block_length=20000;
    ts_group_length = 1;
    // backoff_time = 1 << 10;
    backoff_multiplier_summarization = 1;
    int k_size=0;
    long int labelsize=1;
    int topk=0;
    while (1)
    {
        static struct option long_options[] =  {
            {"use-index", no_argument, 0, 'a'},
            {"initial-lbl-size", required_argument, 0, 'b'},
            {"complete-type", required_argument, 0, 'c'},
            {"dataset", required_argument, 0, 'd'},
            {"total-loaded-leaves", required_argument, 0, 'e'},
            {"flush-limit", required_argument, 0, 'f'},
            {"aggressive-check", no_argument, 0, 'g'},
            {"help", no_argument, 0, 'h'},
            {"initial-fbl-size", required_argument, 0, 'i'},
            {"serial", no_argument, 0, 'j'},
            {"queries-size", required_argument, 0, 'k'},
            {"leaf-size", required_argument, 0, 'l'},
            {"min-leaf-size", required_argument, 0, 'm'},
            {"tight-bound", no_argument, 0, 'n'},
            {"read-thread", required_argument, 0, 'o'},
            {"index-path", required_argument, 0, 'p'},
            {"queries", required_argument, 0, 'q'},
            {"read-block", required_argument, 0, 'r'},
            {"minimum-distance", required_argument, 0, 's'},
            {"timeseries-size", required_argument, 0, 't'},
            {"min-checked-leaves", required_argument, 0, 'u'},
            {"in-memory", no_argument, 0, 'v'},
            {"cpu-type", required_argument, 0, 'w'},
            {"sax-cardinality", required_argument, 0, 'x'},
            {"function-type", required_argument, 0, 'y'},
            {"dataset-size", required_argument, 0, 'z'},
            {"k-size", required_argument, 0, '0'},
            {"knn-label-set", required_argument, 0, '1'},
            {"knn-label-size", required_argument, 0, '2'},
            {"knn", no_argument, 0, '3'},
            {"topk", no_argument, 0, '4'},
            {"ts-group-length", required_argument, 0, '5'},
            {"backoff-power-summarization", required_argument, 0, '6'},
            {"dataset-output",required_argument,0,'7'},
            {"backoff-power-tree-construction", required_argument, 0, '8'},
            {"backoff-power-tree-pruning", required_argument, 0, '9'},
            {"number-of-threads-fail",required_argument,0,'$'},
            {"duration-of-thread-fail",required_argument,0,'#'},
            {"percentage-of-fail",required_argument,0,'!'},
            {"fail-once",required_argument,0,'@'},
            {"fail-summarization",required_argument,0,'%'},
            {"fail-tree-construction",required_argument,0,'^'},
            {"fail-query-answering",required_argument,0,'&'},
            {"fail-helper",required_argument,0,'*'},
            {"helpers",required_argument,0,'('},
            {NULL, 0, NULL, 0}
        };

        /* getopt_long stores the option index here. */
        int option_index = 0;
        int c = getopt_long (argc, argv, "",
                             long_options, &option_index);
        if (c == -1)
            break;
        switch (c)
        {
        	case 'j':
        		serial_scan = 1;
        		break;
            case 'g':
                aggressive_check = 1;
                break;

            case 's':
                minimum_distance = atof(optarg);
                break;

            case 'n':
                tight_bound = 1;
                break;

            case 'e':
                total_loaded_leaves = atoi(optarg);
                break;

            case 'c':
                complete_type = atoi(optarg);
                break;

            case 'q':
                queries = optarg;
                break;

            case 'k':
                queries_size = atoi(optarg);
                break;

            case 'd':
                dataset = optarg;
                break;

            case 'p':
                index_path = optarg;
                break;

            case 'z':
                dataset_size = atoi(optarg);
                break;

            case 't':
                time_series_size = atoi(optarg);
                break;

            case 'x':
                sax_cardinality = atoi(optarg);
                break;

            case 'l':
                leaf_size = atoi(optarg);
                break;

            case 'm':
                min_leaf_size = atoi(optarg);
                break;

            case 'b':
                initial_lbl_size = atoi(optarg);
                break;

            case 'f':
                flush_limit = atoi(optarg);
                break;

            case 'u':
            	min_checked_leaves = atoi(optarg);
            	break;
            case 'w':
                cpu_control_type = atoi(optarg);
                break;

            case 'y':
                function_type = atoi(optarg);
                break;
            case 'i':
                initial_fbl_size = atoi(optarg);
                break;
            case 'o':
                maxreadthread = atoi(optarg);
                break;
            case 'r':
                read_block_length = atoi(optarg);
                break;
            case '0':
                k_size = atoi(optarg); 
                
                break;
            case '1':
                labelset = optarg;
                break;
            case '2':
                labelsize =  atoi(optarg);
            case '3':
               knnlabel=1;
            case '4':
               topk=1;
                break;
            case '5':
                ts_group_length = atoi(optarg);
                break;
            case '6':
                if (atoi(optarg) == -1){
                    backoff_multiplier_summarization = 0;
                }
                else {
                    backoff_multiplier_summarization = atoi(optarg);
                }     
                break;
            case '7':
                dataset_output = optarg;
                break;
            case '8':
                if (atoi(optarg) == -1){
                    backoff_multiplier_tree_construction = 0;
                }
                else {
                    backoff_multiplier_tree_construction = atoi(optarg);
                }
                break;
            case '9':
                if (atoi(optarg) == -1){
                    backoff_multiplier_tree_pruning = 0;
                }
                else {
                    backoff_multiplier_tree_pruning = atoi(optarg);
                }
                break;
            case '$':            
                number_of_threads_fail = atoi(optarg);
                break;
            case '#':
                duration_of_thread_fail = atoi(optarg);
                break;
            case '!':
                percentage_of_thread_fail = atoi(optarg);
                break;
            case '@':
                fail_once = atoi(optarg); //If fail_once == 1 then fail only once, if fail_once == 2 then fail once for each query
                break;
            case '%':
                fail_summarization = atoi(optarg); 
                break;    
            case '^':
                fail_tree_construction = atoi(optarg); 
                break;
            case '&':
                fail_query_answering = atoi(optarg); 
                break;
            case '*':
                fail_helper = atoi(optarg); 
                break;
            case '(':
                helpers = atoi(optarg); 
                break;                                                                                                       
            case 'h':
                printf("Usage:\n\
                \t--dataset XX \t\t\tThe path to the dataset file\n\
                \t--queries XX \t\t\tThe path to the queries file\n\
                \t--dataset-size XX \t\tThe number of time series to load\n\
                \t--queries-size XX \t\tThe number of queries to do\n\
                \t--minimum-distance XX\t\tThe minimum distance we search (MAX if not set)\n\
                \t--use-index  \t\t\tSpecifies that an input index will be used\n\
                \t--index-path XX \t\tThe path of the output folder\n\
                \t--timeseries-size XX\t\tThe size of each time series\n\
                \t--sax-cardinality XX\t\tThe maximum sax cardinality in number of bits (power of two).\n\
                \t--leaf-size XX\t\t\tThe maximum size of each leaf\n\
                \t--min-leaf-size XX\t\tThe minimum size of each leaf\n\
                \t--initial-lbl-size XX\t\tThe initial lbl buffer size for each buffer.\n\
                \t--flush-limit XX\t\tThe limit of time series in memory at the same time\n\
                \t--initial-fbl-size XX\t\tThe initial fbl buffer size for each buffer.\n\
                \t--complete-type XX\t\t0 for no complete, 1 for serial, 2 for leaf\n\
                \t--total-loaded-leaves XX\tNumber of leaves to load at each fetch\n\
                \t--min-checked-leaves XX\t\tNumber of leaves to check at minimum\n\
                \t--tight-bound XX\tSet for tight bounds.\n\
                \t--aggressive-check XX\t\tSet for aggressive check\n\
                \t--serial\t\t\tSet for serial scan\n\
                \t--in-memory\t\t\tSet for in-memory search\n\
                \t--function-type\t\t\tSet for query answering type on disk\n\
                                \t\t\tADS+: 0\n\
                \t\t\tParIS+: 1\n\
                \t\t\tnb-ParIS+: 2\n\n\
                \t\t\tin memory  traditional exact search: 0\n\
                \t\t\tADS+: 1\n\
                \t\t\tParIS-TS: 2\n\
                \t\t\tParIS: 4\n\
                \t\t\tParIS+: 6\n\
                \t\t\t\\MESSI-Hq: 7\n\
                \t\t\t\\MESSI-Sq: 8\n\
                \t--cpu-type\t\t\tSet for how many cores you want to used and in 1 or 2 cpu\n\
                \t--help\n\n\
                \tCPU type code:\t\t\t21 : 2 core in 1 CPU\n\
                \t\t\t\t\t22 : 2 core in 2 CPUs\n\
                \t\t\t\t\t41 : 4 core in 1 CPU\n\
                \t\t\t\t\t42 : 4 core in 2 CPUs\n\
                \t\t\t\t\t61 : 6 core in 1 CPU\n\
                \t\t\t\t\t62 : 6 core in 2 CPUs\n\
                \t\t\t\t\t81 : 8 core in 1 CPU\n\
                \t\t\t\t\t82 : 8 core in 2 CPUs\n\
                \t\t\t\t\t101: 10 core in 1 CPU\n\
                \t\t\t\t\t102: 10 core in 2 CPUs\n\
                \t\t\t\t\t121: 12 core in 1 CPU\n\
                \t\t\t\t\t122: 12 core in 2 CPUs\n\
                \t\t\t\t\t181: 18 core in 1 CPU\n\
                \t\t\t\t\t182: 18 core in 2 CPUs\n\
                \t\t\t\t\t242: 24 core in 2 CPUs\n\
                \t\t\t\t\tOther: 1 core in 1 CPU\n\
                \t--topk\t\t\tSet for topk search\n\
                \t--knn\t\t\tSet for knn search\n");
                return 0;
                break;
            case 'a':
                use_index = 1;
                break;
            case 'v':
                inmemory_flag = 1;
                break;
            default:
                exit(-1);
                break;
        }
    }
    INIT_STATS();

    maxquerythread = cpu_control_type;          // EKOSMAS: ADDED 10 JULY, 2020

    char rm_command[256];

    // EKOSMAS: The following function attempts to create a directory with name "index_path". 
    //          I suspect that this creation is not successful!
	isax_index_settings * index_settings = isax_index_settings_init(    index_path,         // INDEX DIRECTORY
                                                                        time_series_size,   // TIME SERIES SIZE
	                                                                    paa_segments,       // PAA SEGMENTS
	                                                                    sax_cardinality,    // SAX CARDINALITY IN BITS
	                                                                    leaf_size,          // LEAF SIZE
	                                                                    min_leaf_size,      // MIN LEAF SIZE
	                                                                    initial_lbl_size,   // INITIAL LEAF BUFFER SIZE
	                                                                    flush_limit,        // FLUSH LIMIT
	                                                                    initial_fbl_size,   // INITIAL FBL BUFFER SIZE
	                                                                    total_loaded_leaves,// Leaves to load at each fetch
																		tight_bound,		// Tightness of leaf bounds
																		aggressive_check,	// aggressive check
																		1,                  // ???? EKOSMAS ????
                                                                        inmemory_flag,
                                                                        number_of_threads_fail, //Number of fail threads
                                                                        duration_of_thread_fail, //Duration of fail thread
                                                                        percentage_of_thread_fail,//Percentage of thread failure	// new index
                                                                        fail_once,fail_summarization,fail_tree_construction,
                                                                        fail_query_answering,fail_helper,helpers);	
	
    
    isax_index *idx;

    
    COUNT_TOTAL_TIME_START
    if (function_type == 0) {   // Botao's version of MESSI
    }
    else {  // All ekosmas versions
        idx = isax_index_init_inmemory_ekosmas(index_settings);

         // ekosmas's lock-free full fai version with lock-free parallelism in subtree and announce, only after helping and per leaf
        if (function_type == function_type == 999777013 || 999777913) {      
            DO_NOT_HELP = 0;
            if ((function_type%1000)/100 == 9) {
                DO_NOT_HELP = 1;
            }
            // PHASE 1: Fill in of Receive Bufers and PHASE 2: Index Creation
            index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_with_lockfree_parallelism_in_subtree_announce_after_help_per_leaf(dataset, dataset_size, idx);

            // PHASE 3: Query Answering
            isax_query_binary_file_traditional_ekosmas_lf(queries, dataset_output, queries_size, idx, minimum_distance, LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP_PER_LEAF, function_type%100, &exact_search_ParISnew_inmemory_hybrid_ekosmas_lf);
        }
         else {  // Something went wrong!
            printf("ERROR: Version [%d] of MESSI does not exist!\n", function_type);
        }
    }
    
    COUNT_TOTAL_TIME_END
    MY_PRINT_STATS(0.00f)
    
    char filename[100];
    sprintf(filename, "results/results_[%ld_RANDOM].txt", dataset_size);
    FILE * fp;
    fp = fopen (filename, "a");
    PRINT_STATS_TO_FILE(0.00f, fp)
    fclose(fp);

    return 0;
}