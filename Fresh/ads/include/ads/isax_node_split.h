//
//  isax_node_split.h
//  isaxlib
//
//  Created by Kostas Zoumpatianos on 3/12/12.
//  Copyright 2012 University of Trento. All rights reserved.
//

#ifndef isaxlib_isax_node_split_h
#define isaxlib_isax_node_split_h
#include "../../config.h"
#include "../../globals.h"
#include "isax_index.h"
#include "isax_node.h"



int informed_split_decision (isax_node_split_data * split_data, 
                             isax_index_settings * settings,
                             isax_node_record * records_buffer,
                             int records_buffer_size);

isax_node *split_node_inmemory_parallel_lockfree_announce(isax_index *index, isax_node *node, 
												 parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node, 
												 unsigned long total_workers_num,
												 unsigned long my_id,
												 const char lockfree_parallelism_in_subtree,
                                                 unsigned char lightweight_path,
                                                 const unsigned char local_flag);

#endif
