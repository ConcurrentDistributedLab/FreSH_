//
//  isax_node_buffer.c
//  aisax
//
//  Created by Kostas Zoumpatianos on 4/6/12.
//  Copyright 2012 University of Trento. All rights reserved.
//
#include "../../config.h"
#include "../../globals.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#include "ads/isax_node_buffer.h"
#include "ads/isax_node.h"
#include "ads/isax_node_record.h"

void destroy_node_buffer(isax_node_buffer *node_buffer) {
    if (node_buffer->full_position_buffer != NULL) {
        free(node_buffer->full_position_buffer);
        node_buffer->full_position_buffer = NULL;
    }
    if (node_buffer->full_sax_buffer != NULL) {
        free(node_buffer->full_sax_buffer);
        node_buffer->full_sax_buffer = NULL;
    }
    if (node_buffer->full_ts_buffer != NULL) {
        free(node_buffer->full_ts_buffer);
        node_buffer->full_ts_buffer = NULL;
    }
    if (node_buffer->partial_position_buffer != NULL) {
        // !!! DON'T FREE THAT IT REMOVES THE DATA!!!!
        free(node_buffer->partial_position_buffer);
        node_buffer->partial_position_buffer = NULL;
    }
    if (node_buffer->partial_sax_buffer != NULL) {
        free(node_buffer->partial_sax_buffer);
        node_buffer->partial_sax_buffer = NULL;
    }
    if (node_buffer->tmp_full_position_buffer != NULL) {
        free(node_buffer->tmp_full_position_buffer);
        node_buffer->tmp_full_position_buffer = NULL;
    }
    if (node_buffer->tmp_full_sax_buffer != NULL) {
        free(node_buffer->tmp_full_sax_buffer);
        node_buffer->tmp_full_sax_buffer = NULL;
    }
    if (node_buffer->tmp_full_ts_buffer != NULL) {
        free(node_buffer->tmp_full_ts_buffer);
        node_buffer->tmp_full_ts_buffer = NULL;
    }
    if (node_buffer->tmp_partial_position_buffer != NULL) {
        free(node_buffer->tmp_partial_position_buffer);
        node_buffer->tmp_partial_position_buffer = NULL;
    }
    if (node_buffer->tmp_partial_sax_buffer != NULL) {
        free(node_buffer->tmp_partial_sax_buffer);
        node_buffer->tmp_partial_sax_buffer = NULL;
    }
    free(node_buffer);
}


isax_node_buffer * init_node_buffer(int initial_buffer_size) {
    isax_node_buffer * node_buffer = malloc(sizeof(isax_node_buffer));
    node_buffer->initial_buffer_size = initial_buffer_size;
    
    node_buffer->max_full_buffer_size = 0;
    node_buffer->max_partial_buffer_size = 0;
    node_buffer->max_tmp_full_buffer_size = 0;
    node_buffer->max_tmp_partial_buffer_size = 0;
    node_buffer->full_buffer_size = 0;
    node_buffer->partial_buffer_size = 0;
    node_buffer->tmp_full_buffer_size = 0;
    node_buffer->tmp_partial_buffer_size = 0;
    
    (node_buffer->full_position_buffer) = NULL;
    (node_buffer->full_sax_buffer) = NULL;
    (node_buffer->full_ts_buffer) = NULL;
    (node_buffer->partial_position_buffer) = NULL;
    (node_buffer->partial_sax_buffer) = NULL;
    (node_buffer->tmp_full_position_buffer) = NULL;
    (node_buffer->tmp_full_sax_buffer) = NULL;
    (node_buffer->tmp_full_ts_buffer) = NULL;
    (node_buffer->tmp_partial_position_buffer) = NULL;
    (node_buffer->tmp_partial_sax_buffer = NULL);   
    
    return node_buffer;
}



// EKOSMAS: FUNCTION READ
enum response add_to_node_buffer_lockfree(isax_node_buffer *node_buffer, 
                                 isax_node_record *record, 
                                 unsigned long next_buf_pos)
{
    node_buffer->partial_position_buffer[next_buf_pos] = record->position;
    node_buffer->partial_sax_buffer[next_buf_pos] = record->sax;
    // node_buffer->partial_buffer_size++;                                                 // EKOSMAS AUGUST 01, 2020: This should become FAI!
    
    return SUCCESS;
}
