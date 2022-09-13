/*
 * rewrite_phase.h
 *
 *  Created on: Nov 27, 2013
 *      Author: fumin
 */

#ifndef REWRITE_PHASE_H_
#define REWRITE_PHASE_H_

#include "destor.h"
#include "containerstore.h"
#include "jcr.h"
#include "backup.h"
#include "kvstore.h"
#include "index.h"
#include <vector>
#include <string>
#include <queue>
#include <iostream>
#include <algorithm>
using namespace std;

struct containerRecord {
	containerid cid;
	int32_t size;
	int32_t out_of_order;
};

struct structrewritebuffer {
	GQueue *chunk_queue;
	GSequence *container_record_seq; //
	int num;
	int size;
};

void* cfl_rewrite(void* arg);
void* cbr_rewrite(void* arg);
void* cap_rewrite(void* arg);
void* smr_rewrite(void* arg);
void* ned_rewrite(void* arg);

/* har_rewrite.c */
void init_har();
void close_har();
void har_monitor_update(containerid id, int32_t size);
void har_check(struct chunk* c);

/* restore_aware.c */
void init_restore_aware();
void restore_aware_update(containerid id, int32_t chunklen);
int restore_aware_contains(containerid id);
double restore_aware_get_cfl();

/* For sorting container records. */
gint g_record_descmp_by_length(struct containerRecord* a,
		struct containerRecord* b, gpointer user_data);
gint g_record_cmp_by_id(struct containerRecord* a, struct containerRecord* b,
		gpointer user_data);

int rewrite_buffer_push(struct chunk* c);
struct chunk* rewrite_buffer_pop();
struct chunk* rewrite_buffer_top();
#define MAX_CONTAINER_COUNT 8000

#endif /* REWRITE_PHASE_H_ */
