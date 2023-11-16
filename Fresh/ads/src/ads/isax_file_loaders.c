//
//  isax_file_loaders.c
//  isax
//
//  Created by Kostas Zoumpatianos on 4/7/12.
//  Copyright 2012 University of Trento. All rights reserved.
//


#include "../../config.h"
#include "../../globals.h"
#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <inttypes.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdbool.h>
#include <float.h>
#include <unistd.h>
#include <math.h>

#include "ads/isax_node.h"
#include "ads/isax_index.h"
#include "ads/isax_query_engine.h"
#include "ads/isax_node_record.h"
#include "ads/isax_file_loaders.h"
#include "ads/isax_first_buffer_layer.h"
#include "ads/inmemory_index_engine.h"

// ekosmas Lock Free version
void isax_query_binary_file_traditional_ekosmas_lf(const char *ifilename, const char *output_file, int q_num, isax_index *index,
                            float minimum_distance, const char parallelism_in_subtree, const int third_phase,
                            query_result (*search_function)(ts_type*, ts_type*, isax_index*,node_list*, float, const char, const int, const int query_id,int already_failed)) 
{
    COUNT_INPUT_TIME_START
    fprintf(stderr, ">>> Performing queries in file: %s\n", ifilename);
    FILE * ifile;
    ifile = fopen (ifilename,"rb");
    if (ifile == NULL) {
        fprintf(stderr, "File %s not found!\n", ifilename);
        exit(-1);
    }

    fseek(ifile, 0L, SEEK_END);
    file_position_type sz = (file_position_type) ftell(ifile);
    file_position_type total_records = sz/index->settings->ts_byte_size;
    fseek(ifile, 0L, SEEK_SET);
    if (total_records < q_num) {
        fprintf(stderr, "File %s has only %llu records!\n", ifilename, total_records);
        exit(-1);
    }

    unsigned long q_loaded = 1; 
    ts_type * ts = malloc(sizeof(ts_type) * index->settings->timeseries_size);
    COUNT_INPUT_TIME_END

    COUNT_OUTPUT_TIME_START
    COUNT_QUERY_ANSWERING_TIME_START
    ts_type * paa = malloc(sizeof(ts_type) * index->settings->paa_segments);

    node_list nodelist;
    nodelist.nlist = malloc(sizeof(isax_node *) * pow(2, index->settings->paa_segments));
    nodelist.node_amount = 0;

    // maintain root nodes into an array, so that it is possible to execute FAI
    for (int j = 0; j < index->fbl->number_of_buffers; j++ ) {

        parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node = &((parallel_first_buffer_layer_ekosmas_lf*)(index->fbl))->soft_buffers[j];
        if (!current_fbl_node->initialized) {
            continue;
        }

        nodelist.nlist[nodelist.node_amount] = current_fbl_node->node;
        nodelist.node_amount++;
                    
    }
    int already_failed = 0;
    while (q_loaded <= q_num)
    {
        // re-initialized index tree
        // if (q_loaded > 0) { 
        //     COUNT_INITIALIZE_INDEX_TREE_TIME_START
        //     reinitialize_index_tree(index);
        //     COUNT_INITIALIZE_INDEX_TREE_TIME_END
        // }

        COUNT_QUERY_ANSWERING_TIME_END
        COUNT_OUTPUT_TIME_END

        COUNT_INPUT_TIME_START
        fread(ts, sizeof(ts_type),index->settings->timeseries_size,ifile);
        COUNT_INPUT_TIME_END

        query_answering_time_flag_end = 0;

        COUNT_OUTPUT_TIME_START
        COUNT_QUERY_ANSWERING_TIME_START

        // Parse ts and make PAA representation
        paa_from_ts(ts, paa, index->settings->paa_segments,
                    index->settings->ts_values_per_paa_segment);
        unique_leaves_in_arrays = 0;
        leaves_in_arrays = 0;
        query_result result = search_function(ts, paa, index, &nodelist, minimum_distance, parallelism_in_subtree, third_phase, q_loaded,already_failed);
        q_loaded++;
        already_failed = 1;

        // EKOSMAS: REMOVED 06/01/2023
        // COUNT_QUERY_ANSWERING_TIME_END
        // COUNT_OUTPUT_TIME_END

        COUNT_UNIQUE_LEAVES_ARRAY_TOTAL(unique_leaves_in_arrays)
        COUNT_LEAVES_ARRAY_TOTAL(leaves_in_arrays)
        PRINT_QUERY_STATS(result.distance);

        COUNT_OUTPUT_TIME_START
        COUNT_QUERY_ANSWERING_TIME_START
    }
    
    free(nodelist.nlist);
    free(paa);

    COUNT_QUERY_ANSWERING_TIME_END
    COUNT_OUTPUT_TIME_END

    COUNT_INPUT_TIME_START
    free(ts);
    fclose(ifile);
    fprintf(stderr, ">>> Finished querying.\n");
    COUNT_INPUT_TIME_END
}