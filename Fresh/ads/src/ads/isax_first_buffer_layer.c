//
//  first_buffer_layer.c
//  isaxlib
//
//  Created by Kostas Zoumpatianos on 3/20/12.
//  Copyright 2012 University of Trento. All rights reserved.
//
#include "../../config.h"
#include "../../globals.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "ads/sax/sax.h"
#include "ads/isax_first_buffer_layer.h"

// ekosmas lock free version
struct parallel_first_buffer_layer_ekosmas_lf * initialize_pRecBuf_ekosmas_lf(int initial_buffer_size, int number_of_buffers,
                                           int max_total_buffers_size, isax_index *index)
{
    struct parallel_first_buffer_layer_ekosmas_lf *fbl = malloc(sizeof(parallel_first_buffer_layer_ekosmas_lf));

    fbl->max_total_size = max_total_buffers_size;
    fbl->initial_buffer_size = initial_buffer_size;
    fbl->number_of_buffers = number_of_buffers;

    // Allocate a set of soft buffers to hold pointers to the hard buffer
    fbl->soft_buffers = malloc(sizeof(parallel_fbl_soft_buffer_ekosmas_lf) * number_of_buffers);
    fbl->current_record_index = 0;
    int i;
    for (i=0; i<number_of_buffers; i++) {
        fbl->soft_buffers[i].initialized = 0;
        fbl->soft_buffers[i].processed = 0;
        fbl->soft_buffers[i].next_iSAX_group = 0;           // EKOSMAS: ADDED 07 JULY 2020
        fbl->soft_buffers[i].max_buffer_size = NULL;        // EKOSMAS: ADDED 30 JUNE 2020
        fbl->soft_buffers[i].buffer_size = NULL;            // EKOSMAS: ADDED 30 JUNE 2020
        fbl->soft_buffers[i].sax_records = NULL;            // EKOSMAS: ADDED 30 JUNE 2020
        fbl->soft_buffers[i].pos_records = NULL;            // EKOSMAS: ADDED 30 JUNE 2020
        fbl->soft_buffers[i].node = NULL;                   // EKOSMAS: ADDED 30 JUNE 2020
        fbl->soft_buffers[i].iSAX_processed = NULL;         // EKOSMAS: ADDED 20 JULY 2020
        // fbl->soft_buffers[i].announce_array = NULL;         // EKOSMAS: ADDED 29 JULY 2020
        fbl->soft_buffers[i].recBuf_helpers_exist = 0;      // EKOSMAS: ADDED 07 AUGUST 2020
    }
    return fbl;
}


