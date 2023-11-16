//
//  first_buffer_layer.h
//  isaxlib
//
//  Created by Kostas Zoumpatianos on 3/20/12.
//  Copyright 2012 University of Trento. All rights reserved.
//

#ifndef isaxlib_first_buffer_layer_h
#define isaxlib_first_buffer_layer_h
#include "../../config.h"
#include "../../globals.h"
#include "isax_node.h"
#include "isax_index.h"


typedef struct isax_index isax_index;

typedef struct fbl_soft_buffer {
    isax_node *node;
    sax_type ** sax_records;
    file_position_type ** pos_records;
    int initialized; 
    int max_buffer_size;
    int buffer_size;
} fbl_soft_buffer;

typedef struct first_buffer_layer {
    int number_of_buffers;
    int initial_buffer_size;
    int max_total_size;
    int current_record_index;
    fbl_soft_buffer *soft_buffers;
    char *current_record;
    char *hard_buffer;
} first_buffer_layer;


typedef struct parallel_fbl_soft_buffer_ekosmas_lf {
    isax_node* volatile node;                       // EKOSMAS: ADDED 'volatile' JUNE 16 2020 - CHANGED JUNE 24 2020
    sax_type **sax_records;
    file_position_type **pos_records;
    volatile unsigned char initialized;             // EKOSMAS: ADDED 'volatile'
    int *max_buffer_size;                           // EKOSMAS: Why is this a pointer?
    int *buffer_size;                               // EKOSMAS: Why is this a pointer?
    volatile unsigned char processed;               // EKOSMAS: CHANGED from finished to processed
    volatile unsigned long next_iSAX_group;         // EKOSMAS: ADDED JULY 07, 2020
    root_mask_type mask;                            // EKOSMAS: ADDED JUNE 16, 2020
    volatile unsigned char **iSAX_processed;        // EKOSMAS: ADDED JULY 20, 2020
    // volatile announce_rec *announce_array;          // EKOSMAS: ADDED JULY 29, 2020
    volatile unsigned char recBuf_helpers_exist;    // EKOSMAS: ADDED AUGUST 07, 2020
} parallel_fbl_soft_buffer_ekosmas_lf;



typedef struct parallel_first_buffer_layer_ekosmas_lf {
    int number_of_buffers;
    int initial_buffer_size;
    int max_total_size;
    int current_record_index;
    parallel_fbl_soft_buffer_ekosmas_lf *soft_buffers;
} parallel_first_buffer_layer_ekosmas_lf;


parallel_first_buffer_layer_ekosmas_lf * initialize_pRecBuf_ekosmas_lf(int initial_buffer_size, int max_fbl_size, 
                                    int max_total_size, isax_index *index);

#endif
