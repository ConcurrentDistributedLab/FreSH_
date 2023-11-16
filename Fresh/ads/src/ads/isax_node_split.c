//
//  isax_node_split.c
//  isaxlib
//
//  Created by Kostas Zoumpatianos on 3/12/12.
//  Copyright 2012 University of Trento. All rights reserved.
//
#include "../../config.h"
#include "../../globals.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <float.h>
#include <pthread.h>

#include "ads/sax/sax.h"
#include "ads/sax/sax_breakpoints.h"
#include "ads/isax_node.h"
#include "ads/isax_index.h"
#include "ads/isax_node_split.h"

int informed_split_decision (isax_node_split_data * split_data, 
                             isax_index_settings * settings,
                             isax_node_record * records_buffer,
                             int records_buffer_size) 
{
    double * segment_mean = malloc(sizeof(double) * settings->paa_segments);
    double * segment_stdev = malloc(sizeof(double) * settings->paa_segments);
    
    
    int i,j;
    for(i=0; i<settings->paa_segments; i++)
    {
        segment_mean[i] = 0;
        segment_stdev[i] = 0;
    }
    for (i=0; i<records_buffer_size; i++) 
    {
        for(j=0; j<settings->paa_segments; j++)
        {
            segment_mean[j] += (int)records_buffer[i].sax[j]; 
            
        }
    }
    for(i=0; i<settings->paa_segments; i++)
    {
        segment_mean[i] /= (records_buffer_size); 
        //printf("mean: %lf\n", segment_mean[i]);
    }
    for (i=0; i<records_buffer_size; i++) 
    {
        for(j=0; j<settings->paa_segments; j++)
        {
            segment_stdev[j] += pow(segment_mean[j] - (int)records_buffer[i].sax[j], 2); 
        }
    }
    for(i=0; i<settings->paa_segments; i++)
    {
        segment_stdev[i] = sqrt(segment_stdev[i]/(records_buffer_size)); 
        //printf("stdev: %lf\n", segment_stdev[i]);
    }
    
    
    
    // Decide split point based on the above calculations
    int segment_to_split = -1;
    int segment_to_split_b = -1;
    for(i=0; i<settings->paa_segments; i++)
    {
        if (split_data->split_mask[i] + 1 > settings->sax_bit_cardinality - 1 ) {
            continue;
        }
        else
        {
            // TODO: Optimize this.
            // Calculate break point for new cardinality, a bit complex.
            int new_bit_cardinality = split_data->split_mask[i] + 1;
            int break_point_id = records_buffer[0].sax[i];
            break_point_id = (break_point_id >> ((settings->sax_bit_cardinality) -
                                                (new_bit_cardinality))) << 1;
            int new_cardinality = pow(2, new_bit_cardinality+1);
            int right_offset = ((new_cardinality - 1) * (new_cardinality - 2)) / 2
                                + new_cardinality - 2;
            float b = sax_breakpoints[right_offset - break_point_id];
            
            if (segment_to_split == -1) {
                segment_to_split = i;
                segment_to_split_b = b;
                continue;
            }
            
            float left_range = segment_mean[i] - (3 * segment_stdev[i]);
            float right_range = segment_mean[i] + (3 * segment_stdev[i]);
            //printf("%d, %lf -- %lf \n", i, left_range, right_range);
            
            if(left_range <= b && b <= right_range) {
                if (abs(segment_mean[i] - b) <= abs(segment_mean[i] - segment_to_split_b)) {
                    segment_to_split = i;
                    segment_to_split_b = b;
                }
            }
        }
    }
    
    free(segment_mean);
    free(segment_stdev);
    return segment_to_split;
}


isax_node *split_node_inmemory_parallel_lockfree_announce(isax_index *index, isax_node *node, 
                                                 parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node, unsigned long total_workers_num,
                                                 unsigned long my_id,
                                                 const char lockfree_parallelism_in_subtree,
                                                 unsigned char lightweight_path,
                                                 const unsigned char local_flag)
{

    isax_node * volatile *node_to_change = NULL;
    isax_node *parent = node->parent;

    // CAS on parent....
    if (!parent) {
        node_to_change = &current_fbl_node->node;
    }
    else if (parent->left_child == node) {
        node_to_change = &parent->left_child;
    }
    else if (parent->right_child == node) {
        node_to_change = &parent->right_child;
    }
    else { // node has been already replaced by some other thread
        return parent;          // EKOSMAS AUGUST 01, 2020: Is this correct?
    }

    isax_node *new_node; 
    if (!parent) {
        new_node = isax_root_node_init_lockfree_announce_copy(node, node->mask, index->settings->initial_leaf_buffer_size, total_workers_num, lockfree_parallelism_in_subtree, current_fbl_node);
    }
    else {
        new_node = isax_leaf_node_init_lockfree_announce_copy(node, index->settings->initial_leaf_buffer_size, total_workers_num, lockfree_parallelism_in_subtree, current_fbl_node);
    }

    new_node->parent = node->parent;
    new_node->lightweight_path = lightweight_path;

    // ******************************************************* 
    // CREATE TWO NEW NODES AND SET OLD ONE AS AN INTERMEDIATE
    // ******************************************************* 
    int i;
    
    #ifdef DEBUG    
    printf("*** Splitting. ***\n\n");
    #endif
    
    #ifdef DEBUG
    if (!node->is_leaf) {
        fprintf(stderr,"sanity error: You are trying to split something weird...\
                ARE YOU TRYING TO KILL ME?\n");
    }
    #endif
     
    // Create split_data for this node.
    isax_node_split_data * split_data = malloc(sizeof(isax_node_split_data));
    if(split_data == NULL) {
        fprintf(stderr,"error: could not allocate memory for node split data.\n");
    }
    split_data->split_mask = malloc(sizeof(sax_type) * index->settings->paa_segments);
    if(split_data->split_mask == NULL) {
        fprintf(stderr,"error: could not allocate memory for node split mask.\n");
    }
   
    if (node->parent == NULL) {
        for (i=0; i<index->settings->paa_segments; i++) {
            split_data->split_mask[i] = 0;
        }
        split_data->splitpoint = 0;
    }
    else {
        for (i=0; i<index->settings->paa_segments; i++) {
            split_data->split_mask[i] = node->parent->split_data->split_mask[i];
        }
    }

    new_node->split_data = split_data;

    // index->memory_info.mem_tree_structure += 2;              // EKOSMAS 28 JULY, 2020: removed
    
   
    // ############ S P L I T   D A T A #############
    // Allocating total_workers_num more position to cover any announced insertion that
    // have not yet acquired a potision into node->buffer.
    // EKOSMAS 26/01/2022 -- ERROR? ---: Yes, but this may be problematic (!) since it may happen that
    // more than index->settings->max_leaf_size have to be stored in some leaf!! (e.g. in case splitting of
    // iSAX summaries results on all of them to be on one leaf child!). In case this is actually a problem
    // the solution could be to ignore those that have not yet acquired a position!
    isax_node_record *split_buffer = malloc(sizeof(isax_node_record) * 
                                            (index->settings->max_leaf_size + 2*total_workers_num));        // EKOSMAS: I added a 2x as an easy fix here
    
    int split_buffer_index = 0;
    
    // ********************************************************
    // SPLIT SAX BUFFERS CONTAINED IN *RAM* AND PUT IN CHILDREN
    // ******************************************************** 
    // Split both sax and ts data and move to the new leafs
     
    // if (lockfree_parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE || current_fbl_node->recBuf_helpers_exist) {
    if ((lockfree_parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP_PER_LEAF && node->recBuf_leaf_helpers_exist) ||
        (lockfree_parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP && current_fbl_node->recBuf_helpers_exist) ||
        lockfree_parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE) {
        if (lightweight_path == 1) {                                            // if lightweight path has been followed then return failure, since you have to follow heavy path
            isax_tree_destroy_lockfree(new_node);
            free(split_buffer);
            return NULL;
        }

        // If new_node has not been created locally and announce_array exists
        if (!local_flag && node->announce_array != NULL) {
            if (lockfree_parallelism_in_subtree != LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE) {      // in this case, allocate new_node's announce array
               new_node->announce_array = calloc(total_workers_num, sizeof(announce_rec *));
            }

            if (new_node->announce_array == NULL) {printf("ERROR: NULL new_node->announce_array -2- !!!!"); fflush(stdout);}

            // copy active operations from announce array to the corresponding announce array into new_node
            for (i = 0; i < total_workers_num; i++) {
                volatile announce_rec *tmp_announce_rec = node->announce_array[i];

                if (tmp_announce_rec != NULL) {                                              // active operation (insert)
                    if (tmp_announce_rec->buf_pos != ULONG_MAX) {                            // if it has selected position, then
                        new_node->announce_array[i] = tmp_announce_rec;
                        add_to_node_buffer_lockfree(node->buffer, (isax_node_record *) &tmp_announce_rec->record, tmp_announce_rec->buf_pos);     // (re-)write it to position in node->buffer, in order to guarrantee that it is there        
                    }
                    else {                                                                                      // otherwise,
                        split_buffer[split_buffer_index].sax = tmp_announce_rec->record.sax;        // add it into split_buffer
                        split_buffer[split_buffer_index].position = tmp_announce_rec->record.position;
                        split_buffer[split_buffer_index].ts = NULL;
                        split_buffer[split_buffer_index].insertion_mode = NO_TMP | PARTIAL;                     // EKOSMAS JULY 30, 2020: is this required???
                        split_buffer_index++;

                        announce_rec *new_announce_rec = create_new_announce_rec((isax_node_record *)&tmp_announce_rec->record);
                        new_announce_rec->buf_pos = 1;                                                // mark it as completed, i.e. any value ohte than ULONG_MAX (in case the CAS operation (below) on this new_node succeeds)
                        new_node->announce_array[i] = new_announce_rec;
                    }
                }
            }
        }
        // else {          // local copy of announce array to (left or right) child of node
        //     // !!!ERROR!!! THE FOLLOWING HAS TO BE PERFORMED LATER WHILE POPULATING BUFFERS OF CHILDREN
        //     // for (i = 0; i < total_workers_num; i++) {
        //     //     new_node->announce_array[i] = node->announce_array[i];
        //     // }
        // }
    }

    for (i=index->settings->max_leaf_size-1; i>=0; i--) {                                                                  
        if (node->buffer->partial_sax_buffer[i] != NULL) {                                              // EKOSMAS AUGUST 08, 2020: ADDED. If this is true, then the ith elements of both partial_sax_buffer and partial_position_buffer are not empty, dut to the order that these are initialized during add_to_node_buffer_lockfree()
            split_buffer[split_buffer_index].sax = node->buffer->partial_sax_buffer[i];
            split_buffer[split_buffer_index].ts = NULL;
            split_buffer[split_buffer_index].position = node->buffer->partial_position_buffer[i];
            split_buffer[split_buffer_index].insertion_mode = NO_TMP | PARTIAL;                         // EKOSMAS JULY 30, 2020: is this required???
            split_buffer_index++;
        }
    }
    
    // destroy_node_buffer(node->buffer);                       // EKOSMAS 28 JULY, 2020: removed, DANGEROUS!!!
    // node->buffer = NULL;                                     // EKOSMAS 28 JULY, 2020: removed, DANGEROUS!!!
    
    
    // *****************************************************
    // SPLIT BUFFERS CONTAINED ON *DISK* AND PUT IN CHILDREN
    // ***************************************************** 
  
    split_data->splitpoint = informed_split_decision(split_data, index->settings, split_buffer, split_buffer_index);
    
    if(split_data->splitpoint < 0)
    {
        fprintf(stderr,"error: cannot split in depth more than %d.\n",
                index->settings->sax_bit_cardinality);
        exit(-1);
    }
    
    if(++split_data->split_mask[split_data->splitpoint] > index->settings->sax_bit_cardinality - 1) {
        fprintf(stderr,"error: cannot split in depth more than %d.\n", 
                index->settings->sax_bit_cardinality);
        exit(-1);
    }
    
    // EKOSMAS - 26/01/2022 - COMMENT: It happens that 
    // index->settings->initial_leaf_buffer_size == index->settings->max_leaf_size 
    // as required. This is an invariant. Possibly use only one of these values???
    isax_node * left_child = isax_leaf_node_init_lockfree_announce(index->settings->initial_leaf_buffer_size, total_workers_num, lockfree_parallelism_in_subtree, current_fbl_node);
    isax_node * right_child = isax_leaf_node_init_lockfree_announce(index->settings->initial_leaf_buffer_size, total_workers_num, lockfree_parallelism_in_subtree, current_fbl_node);
    
    left_child->parent = new_node;                              
    right_child->parent = new_node;                            
    
    new_node->left_child = left_child;
    new_node->right_child = right_child;

    root_mask_type mask = index->settings->bit_masks[index->settings->sax_bit_cardinality - 
                                                     split_data->split_mask[split_data->splitpoint] - 1];
    
    while (split_buffer_index > 0) {
        split_buffer_index--;
        if(mask & split_buffer[split_buffer_index].sax[split_data->splitpoint]) {   
            right_child = add_record_to_node_inmemory_parallel_lockfree_announce_local(index, right_child, current_fbl_node, &split_buffer[split_buffer_index], total_workers_num, my_id, lockfree_parallelism_in_subtree, lightweight_path);            
            if (!right_child) {
                free(split_buffer);
                isax_tree_destroy_lockfree(new_node);       // it also destroys left and right child!
                return NULL;                                // lightweight path should change to heavy path
            }
        }
        else {
            left_child = add_record_to_node_inmemory_parallel_lockfree_announce_local(index, left_child, current_fbl_node, &split_buffer[split_buffer_index], total_workers_num, my_id, lockfree_parallelism_in_subtree, lightweight_path);
            if (!left_child) {
                free(split_buffer);
                isax_tree_destroy_lockfree(new_node);       // it also destroys left and right child!
                return NULL;                                // lightweight path should change to heavy path
            }
        }
    }

    free(split_buffer);

    // If new_node has been created locally
    if (local_flag) {
        *node_to_change = new_node;
        return new_node;
    }

    isax_node *ret_node;
    if (*node_to_change == node && CASPTR(node_to_change, node, new_node)) {
        ret_node = new_node;
    }
    else {
        isax_tree_destroy_lockfree(new_node);   //  it also destroys left and right child!
        if (lightweight_path == 1) {
            return NULL;                    // I followed lightweight path and my CAS failed due to other helpers, so I have to restart and follow the heavy path.
        }
        ret_node = *node_to_change;
    }

    if (lightweight_path == 0 && ret_node->lightweight_path == 1) {
        return NULL;                                                    // the initial process was still following lightweight path and succeeded on updating *node_to_change, so my current record has not been added, since announce arrays have been ignored. So, restart my operation in order to try insert my record again.
    }

    return ret_node;
}

