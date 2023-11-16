//
//  parallel_index_engine.c
//
//
//  Created by Botao PENG on 29/1/18.
//


#include "../../config.h"
#include "../../globals.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <inttypes.h>
#include <pthread.h>
#include <unistd.h>
//#include <semaphore.h>
#include <stdbool.h>

#include "ads/isax_node.h"
#include "ads/isax_index.h"
#include "ads/isax_query_engine.h"
#include "ads/isax_node_record.h"
#include "ads/isax_file_loaders.h"
#include "ads/isax_first_buffer_layer.h"
#include "ads/parallel_index_engine.h"
#include "ads/parallel_query_engine.h"


isax_node * insert_to_pRecBuf_lock_free(parallel_first_buffer_layer_ekosmas_lf *fbl, sax_type *sax,
                          file_position_type *pos,root_mask_type mask,
                          isax_index *index, int workernumber, int total_workernumber, const char parallelism_in_subtree)
{
    parallel_fbl_soft_buffer_ekosmas_lf *current_buffer = &fbl->soft_buffers[(int) mask];
    current_buffer->mask = mask;

    file_position_type *filepointer;
    sax_type *saxpointer;

    int current_buffer_number;
    char *cd_s,*cd_p;

    // Check if this buffer is initialized
    if (!current_buffer->initialized)
    {
        int *tmp_max_buffer_size = calloc(total_workernumber, sizeof(int));
        int *tmp_buffer_size = calloc(total_workernumber, sizeof(int));
        sax_type ** tmp_sax_records = calloc(total_workernumber, sizeof(sax_type *));
        file_position_type ** tmp_pos_records = calloc(total_workernumber, sizeof(file_position_type *));
        unsigned char ** tmp_iSAX_processed;
        // announce_rec *tmp_announce_array;
        if (parallelism_in_subtree != NO_PARALLELISM_IN_SUBTREE) {
            tmp_iSAX_processed = calloc(total_workernumber, sizeof(unsigned char *));

        }

        if (!current_buffer->max_buffer_size && !CASPTR(&current_buffer->max_buffer_size, NULL, tmp_max_buffer_size)) {
            free (tmp_max_buffer_size);
        }

        if (!current_buffer->buffer_size && !CASPTR(&current_buffer->buffer_size, NULL, tmp_buffer_size)) {
            free (tmp_buffer_size);
        }

        if (!current_buffer->sax_records && !CASPTR(&current_buffer->sax_records, NULL, tmp_sax_records)) {
            free (tmp_sax_records);
        }

        if (!current_buffer->pos_records && !CASPTR(&current_buffer->pos_records, NULL, tmp_pos_records)) {
            free (tmp_pos_records);
        }

        if (parallelism_in_subtree!=NO_PARALLELISM_IN_SUBTREE && !current_buffer->iSAX_processed && !CASPTR(&current_buffer->iSAX_processed, NULL, tmp_iSAX_processed)) {
            free (tmp_iSAX_processed);
        }

        // if (parallelism_in_subtree==LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE && !current_buffer->announce_array && !CASPTR(&current_buffer->announce_array, NULL, tmp_announce_array)) {
        //     free (tmp_announce_array);
        // }

        // current_buffer->node = NULL;                            // EKOSMAS: CHANGED JUNE 16 2020     // EKOSMAS: MOVED TO initialize_pRecBuf JUNE 16 2020
        current_buffer->mask = mask;
        
        if (!current_buffer->initialized) {
            current_buffer->initialized = 1;
        }
    }

    // Check if this buffer is not full!
    if (current_buffer->buffer_size[workernumber] >= current_buffer->max_buffer_size[workernumber]) {
        if(current_buffer->max_buffer_size[workernumber] == 0) {
            current_buffer->max_buffer_size[workernumber] = fbl->initial_buffer_size;
            current_buffer->sax_records[workernumber] = malloc(index->settings->sax_byte_size *
                                                 current_buffer->max_buffer_size[workernumber]);
            current_buffer->pos_records[workernumber] = malloc(index->settings->position_byte_size*
                                                 current_buffer->max_buffer_size[workernumber]);

            if (parallelism_in_subtree!=NO_PARALLELISM_IN_SUBTREE) {
                current_buffer->iSAX_processed[workernumber] = calloc(current_buffer->max_buffer_size[workernumber], sizeof(unsigned char));
            }
        }
        else {
            current_buffer->max_buffer_size[workernumber] *= BUFFER_REALLOCATION_RATE;
            current_buffer->sax_records[workernumber] = realloc(current_buffer->sax_records[workernumber],
                                           index->settings->sax_byte_size *
                                           current_buffer->max_buffer_size[workernumber]);
            current_buffer->pos_records[workernumber] = realloc(current_buffer->pos_records[workernumber],
                                           index->settings->position_byte_size *
                                           current_buffer->max_buffer_size[workernumber]);

            if (parallelism_in_subtree!=NO_PARALLELISM_IN_SUBTREE) {
                current_buffer->iSAX_processed[workernumber] = realloc((void *)current_buffer->iSAX_processed[workernumber],
                                           sizeof(unsigned char) * 
                                           current_buffer->max_buffer_size[workernumber]);
                for (int i = current_buffer->max_buffer_size[workernumber]/2; i < current_buffer->max_buffer_size[workernumber]; i++) {
                    current_buffer->iSAX_processed[workernumber][i] = 0;
                }
            }
        }
    }

    if (current_buffer->sax_records[workernumber] == NULL || current_buffer->pos_records[workernumber] == NULL) {
        fprintf(stderr, "error: Could not allocate memory in FBL.");
        return OUT_OF_MEMORY_FAILURE;
    }
    
    current_buffer_number = current_buffer->buffer_size[workernumber];
    filepointer = (file_position_type *)current_buffer->pos_records[workernumber];
    saxpointer = (sax_type *)current_buffer->sax_records[workernumber];
    //printf("the work number is %d sax is  %d \n",workernumber,saxpointer[current_buffer_number*index->settings->paa_segments]);
    // EKOSMAS: we could avoid these memcopies and dereferences of pointers by having: i) sax_type *sax_records and ii) file_position_type *pos_records, insted pf **
    memcpy((void *) (&saxpointer[current_buffer_number*index->settings->paa_segments]), (void *) sax, index->settings->sax_byte_size);
    memcpy((void *) (&filepointer[current_buffer_number]), (void *) pos, index->settings->position_byte_size);

    (current_buffer->buffer_size[workernumber])++;
    return (isax_node *) current_buffer->node;
}
