#include <assert.h>
//#include "destor.h"
#include "optimal_fls.h"

//extern GHashTable *top;
/*
void log_segUtil2_file(GSequenceIter *iter)  //print the top containers
{
    fprintf(fp_utilization,"segId: %" PRId32 "\n", segment_utl_id);
    segment_utl_id++;
    double perce;
    struct containerRecord* record = g_sequence_get(iter);
    perce=(record->size)/(4*1024*1024);
    fprintf(fp_utilization,"%-12" PRId32 " %-12" PRId32 " %.4f\n", record->cid, record->size, perce);
}*/

/* init utility buckets */
void init_buckets(High_buckets &high_buckets, Low_buckets &low_buckets) {
	high_buckets.container_num = 0;
	
	high_buckets.current_utility_threshold = 1;
	
	bzero(&high_buckets.buckets, sizeof(high_buckets.buckets));

    low_buckets.container_num = 0;
    low_buckets.current_utility_threshold = 0;
}

void add_container_to_high_buckets(double container_util,High_buckets &high_buckets) {    //根据一个块的重写价值更新当前的阈值
	high_buckets.container_num++;
	//assert(container_num == utility_buckets2.container_num);
	printf(" add: high utility :%0.2f\n", container_util);
    high_buckets.current_utility_threshold = container_util < high_buckets.current_utility_threshold ? container_util : high_buckets.current_utility_threshold;
	int index = container_util >= 1 ? 99 : container_util * 100;
	high_buckets.buckets[index]++;	
}

void low_buckets_update(double container_util, int num, Low_buckets &low_buckets){
    low_buckets.container_num += num;
	printf(" add: low utility: %0.2f\n", container_util);
    low_buckets.current_utility_threshold = container_util > low_buckets.current_utility_threshold ? container_util : low_buckets.current_utility_threshold;	
}

void high_buckets_update(double percentage,High_buckets high_buckets) {    //根据一个块的重写价值更新当前的阈值
	if (high_buckets.container_num >= 10) {
		double best_num = high_buckets.container_num * (1-percentage);    //重写碎片块的限制，0.05
		int current_index = 0;
        int count = 0;
		for (; current_index <= 99; current_index++) {
			count += high_buckets.buckets[current_index];
			if (count >= best_num) {
				break;
			}
		}
		high_buckets.current_utility_threshold = (current_index + 1)/ 100.0;
	}
}

