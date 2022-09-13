#ifndef OPTIMAL_RESTORE_H_INCLUDED
#define OPTIMAL_RESTORE_H_INCLUDED

#include <stdio.h>
#include <stdint.h>
#include <glib.h>
#include <inttypes.h>
#include <assert.h>

#include "rewrite_phase.h"
/*******调查******/
/* int64_t segment_utl_id;
FILE *fp_utilization;
int32_t sum_cap_level;
int32_t sum_segments;
int flag_adjust; */

//void init_optimal_capping();


/*****optimal capping******/
/*int64_t segment_num;
int64_t container_num;
int64_t top_container_num;
int64_t rewrite_container_num;
double thres; */
//int capping_type; //0 denotes fixcapping; 1 denotes optimalcapping
//double opt_minimal;
//int32_t opt_capping_thres;
//int opt_rewrite_containers;

typedef struct {
	int32_t container_num;
	double current_utility_threshold;
	//int min_index;
	/* [0,1/100), [1/100, 2/100), ... , [99/100, 1] */
	int32_t buckets[100];
} High_buckets;

typedef struct {
	int32_t container_num;
	double current_utility_threshold;
	//int min_index;
	/* [0,1/100), [1/100, 2/100), ... , [99/100, 1] */	
} Low_buckets;


void init_buckets(High_buckets&,Low_buckets&);
void add_container_to_high_buckets(double container_util,High_buckets&);
void low_buckets_update(double container_util, int32_t num,Low_buckets&);
void utility_buckets_update(double percentage);
//void redical_capping(GSequenceIter ** iter, int *temp_cap);

#endif // OPTIMAL_RESTORE_H_INCLUDED
