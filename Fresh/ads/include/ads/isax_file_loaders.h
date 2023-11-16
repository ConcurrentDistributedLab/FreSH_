//
//  isax_file_loaders.h
//  isax
//
//  Created by Kostas Zoumpatianos on 4/7/12.
//  Copyright 2012 University of Trento. All rights reserved.
//

#ifndef isax_isax_file_loaders_h
#define isax_isax_file_loaders_h
#include "../../config.h"
#include "../../globals.h"
#include "sax/ts.h"
#include "sax/sax.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "isax_index.h"
#include "isax_query_engine.h"
#include "inmemory_index_engine.h"



void isax_query_binary_file_traditional_ekosmas_lf(const char *ifilename, const char *output_file, int q_num, isax_index *index,
                            float minimum_distance, const char parallelism_in_subtree, const int third_phase,
                            query_result (*search_function)(ts_type*, ts_type*, isax_index*,node_list*, float, const char, const int, const int query_id,int already_failed));

#endif
