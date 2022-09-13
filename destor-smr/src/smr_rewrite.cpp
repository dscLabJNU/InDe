#include "destor.h"
#include "jcr.h"
#include "rewrite_phase.h"
#include "backup.h"
#include "containerstore.h"
#include "kvstore.h"
#include "index.h"
#include "optimal_fls.h"
#include <vector>
#include <string>
#include <queue>
#include <iostream>
#include <algorithm>
using namespace std;

struct structindexlock{
	/* g_mutex_init() is unnecessary if in static storage. */
	pthread_mutex_t mutex;
	pthread_cond_t cond; // index buffer is not full
	// index buffer is full, waiting
	// if threshold < 0, it indicates no threshold.
	int wait_threshold;
};
extern struct structindexlock index_lock;
extern struct structdestor destor;
extern struct structjcr jcr;
extern SyncQueue* dedup_queue;
extern SyncQueue* rewrite_queue;;
static GHashTable *top;
//static GHashTable *top_read;

static int64_t chunk_num;
static int32_t num_rw_before;
static int32_t seg_num;
static int32_t read_cap_before;

extern vector <chunk *> rewrite_buffer_chunk_pt;
extern GHashTable *real_containerid_to_tmp;
extern int64_t tmp_to_real_containerid[MAX_CONTAINER_COUNT];

extern struct containerchunkcount{
	int64_t id;
	int64_t chunkcount;
	bool operator < (const containerchunkcount &a) const {
		return chunkcount > a.chunkcount;
	}
	containerchunkcount() {
		id = -1;
		chunkcount = 0;
	}
}Node[MAX_CONTAINER_COUNT];

extern vector <string> all_fp[MAX_CONTAINER_COUNT];

extern bool is_container_selected[MAX_CONTAINER_COUNT];
extern vector <int> container_selected;

void *smr_rewrite(void* arg) {
	High_buckets high_buckets;
	Low_buckets low_buckets;
    init_buckets(high_buckets,low_buckets);
	top = g_hash_table_new_full(g_int64_hash, g_int64_equal, NULL, free);    
	struct chunk *c;
	rewrite_buffer_chunk_pt.clear();
	for (int i = 0; i < MAX_CONTAINER_COUNT; i++) {
		all_fp[i].clear();
	}
    
	while (1) {
		struct chunk *c = (chunk *)sync_queue_pop(dedup_queue);

		if (c == NULL)
			break;

		TIMER_DECLARE(1);
		TIMER_BEGIN(1);
		if (!rewrite_buffer_push(c)) {
			rewrite_buffer_chunk_pt.push_back(c);
			//Grouping into segment.
			TIMER_END(1, jcr.rewrite_time);
			continue;
		}
		seg_num++;
		printf("\nseg_num:%d\n",seg_num);
		//Redundancy Identification
		//Getting the referenced container IDs by inquiring the fingerprint index table
		real_containerid_to_tmp = g_hash_table_new_full(g_int64_hash, g_int64_equal, NULL, free);
		memset(tmp_to_real_containerid, -1, sizeof(tmp_to_real_containerid));
		int cur_container_count = 0;

		for (int chunk_id = 0; chunk_id < rewrite_buffer_chunk_pt.size(); chunk_id++) {
			c = rewrite_buffer_chunk_pt[chunk_id];
			if(!CHECK_CHUNK(c, CHUNK_DUPLICATE)) continue;
			//only check all duplicate chunks
			fingerprint cur_fp;
			fingerprint chunk_count_fp;
			fingerprint new_fp;
			for (int i = 0; i < 20; i++) {
				cur_fp[i] = c->fp[i];
				chunk_count_fp[i] = c->fp[i];
				new_fp[i] = c->fp[i];
			}
			chunk_count_fp[0] = 254;
			fingerprint *new_fp_pt =  &new_fp;
			
			pthread_mutex_lock(&index_lock.mutex);
			int64_t *chunk_count_fp_pt = kvstore_lookup(chunk_count_fp);
			int64_t chunk_count = 0;
			if (chunk_count_fp_pt != NULL) chunk_count = *chunk_count_fp_pt;
			for (int i = 1; i <= chunk_count; i++) {
				new_fp[0] = (unsigned char)i;
				int64_t *new_fp_id_pt = kvstore_lookup(new_fp);
				containerid realid = *kvstore_lookup(new_fp);
				containerid *idpt = (containerid *)g_hash_table_lookup(real_containerid_to_tmp, &realid);
				if (!idpt &&((cur_container_count + 1) > MAX_CONTAINER_COUNT)) {
					continue;
				}
		    	containerid *realidpt = (containerid *)malloc(sizeof(containerid));
				containerid *tmpidpt = (containerid *)malloc(sizeof(containerid));
		    	*realidpt = realid;
		    	containerid tmpid;
		    	if(idpt == NULL) {
		    		tmpid = cur_container_count++;
		    		if (cur_container_count > MAX_CONTAINER_COUNT) {
		    			continue;
		    		}
		    		*tmpidpt = tmpid;
		    		g_hash_table_insert(real_containerid_to_tmp, realidpt, tmpidpt);
			    	tmp_to_real_containerid[tmpid] = realid;
		    	}
		    	else {
		    		tmpid = *idpt;
		    	}	
		    	assert(tmp_to_real_containerid[tmpid] == realid);
		    	char code[41];
				hash2code(c->fp, code);
				code[40] = 0;
		    	all_fp[tmpid].push_back(code);		
			}
			pthread_mutex_unlock(&index_lock.mutex);
		}
		//SMR: submodular maximization rewriting scheme
		//For each iteration, choose the container which maximize the submodular monotone score function, i.e, the number of distinct referenced chunks.
		//Note: If the increased number is not larger than 0, the iteration will stop_rw to save disk accesses.
		
        //read_phase
        int32_t length = cur_container_count;		
		int32_t num = length > destor.rewrite_smr_level ? destor.rewrite_smr_level : length;	

		memset(is_container_selected, 0, sizeof(is_container_selected));
		container_selected.clear();
        GHashTable *chunk_cover; 
		chunk_cover = g_hash_table_new(g_str_hash, g_str_equal);
        
		int32_t read_RC = 0; 
        int32_t num_rw_chunk = 0;
	    int32_t num_rw_this = 0;
	    if(destor.unique_chunk_num == 0){
		    num_rw_chunk = 0;		
	    }else{
		    num_rw_chunk = (destor.unique_chunk_num * DEDUP_REDUCTION_LIM) / ((1 - DEDUP_REDUCTION_LIM) * (destor.chunk_num/destor.rewrite_algorithm[1]));		
	    }
       	num_rw_this = num_rw_chunk * seg_num - num_rw_before;
				
		printf("fls_rewrite-------num_rw_chunk:%d\n",num_rw_chunk);	
		printf("------destor.unique_chunk_num: %d---------num_rw_before:%d----------num_rw_this:%d \n", destor.unique_chunk_num,num_rw_before,num_rw_this);
        printf("------curcontainer_num: %d \n", cur_container_count);

        int64_t pre_count_r = 0;
		if (num > 0)
		{
            int64_t last_pre = -1;
            int64_t delta = -1;
			for (int subround = 0; subround < num; subround++)
			{
				int64_t preDedup = -1;
				int64_t preCtrID = -1;                				
				for (int cur_ctr_id = 0; cur_ctr_id < cur_container_count; cur_ctr_id++)
				{
					if (is_container_selected[cur_ctr_id])
						continue;
					int64_t curDedup = 0;
					for (int chunk_id = 0; chunk_id < all_fp[cur_ctr_id].size(); chunk_id++)
					{
						string cur_chunk = all_fp[cur_ctr_id][chunk_id];
						char code[41];
						code[40] = 0;
						strcpy(code, cur_chunk.c_str());
						containerid *idpt = (containerid *)g_hash_table_lookup(chunk_cover, &code);
						if (!idpt)
						{
							curDedup++;
						}
					}
					if (curDedup > preDedup)
					{
						preCtrID = cur_ctr_id;
						preDedup = curDedup;
					}
				}               
                
				if (preDedup <= 0.1 * destor.rewrite_algorithm[1] || (preDedup <= 0.3*last_pre) || (((delta + last_pre - preDedup) >= preDedup) && preDedup <150 ))
				{
                    pre_count_r = preDedup;
					break;
				}                 
                if(delta == -1) delta = 0;
                else delta = last_pre - preDedup;
                last_pre = preDedup;
				read_RC ++;
                printf("----------preDedup_read:%d",preDedup);

				container_selected.push_back(preCtrID);
				is_container_selected[preCtrID] = 1;
				containerid tmpid = preCtrID;
				containerid realid = tmp_to_real_containerid[tmpid];
				assert(realid != -1);	
				struct containerRecord *r = (struct containerRecord *)malloc(
					sizeof(struct containerRecord));
				r->cid = realid;
				r->size = all_fp[tmpid].size();
				r->out_of_order = 0;					
				for (int j = 0; j < all_fp[tmpid].size(); j++)
				{
					g_hash_table_insert(chunk_cover, (gpointer)all_fp[tmpid][j].c_str(), &r->cid);
				}                 
			}
		}
        g_hash_table_remove_all(chunk_cover);
        printf("\n");

        //rw_num phase   
        memset(is_container_selected, 0, sizeof(is_container_selected));
		container_selected.clear();
		int32_t rw_RC = 0; 
        int32_t rw = 0; 
		int32_t rw_num = 0;

		int64_t pre_count_w = 0;
        if(num_rw_this > 0){
           
        for (int subround = 0; subround < cur_container_count; subround++) {
            int64_t preDedup = MAX_CONTAINER_COUNT;
            int64_t preCtrID = -1;
            for (int cur_ctr_id = 0; cur_ctr_id < cur_container_count; cur_ctr_id++) {
                if (is_container_selected[cur_ctr_id]) continue;
                int64_t curDedup = 0;
                for (int chunk_id = 0; chunk_id < all_fp[cur_ctr_id].size(); chunk_id++) {
                    string cur_chunk = all_fp[cur_ctr_id][chunk_id];
                    char code[41];
                    code[40] = 0;
                    strcpy(code, cur_chunk.c_str());
                    containerid *idpt = (containerid *)g_hash_table_lookup(chunk_cover, &code);
                    if (!idpt) {
                         curDedup++;
                    }
                }
                if (curDedup < preDedup) {
                    preCtrID = cur_ctr_id;
                    preDedup = curDedup;
                }
            }
            
            if (rw_num + preDedup > num_rw_this || preDedup == -1)
			{
				pre_count_w = preDedup;
				break; 
			}
			rw_RC ++;
			rw_num += preDedup;
            printf("------preDedup_rw:%d",preDedup);

            container_selected.push_back(preCtrID);
            is_container_selected[preCtrID] = 1;
            containerid tmpid = preCtrID;
            containerid realid = tmp_to_real_containerid[tmpid];
            assert(realid != -1);
            struct containerRecord* r = (struct containerRecord*) malloc(
                    sizeof(struct containerRecord));
            r->cid = realid;
            r->size = all_fp[tmpid].size();
            r->out_of_order = 0;
            for (int j = 0; j < all_fp[tmpid].size(); j++) {
                g_hash_table_insert(chunk_cover, (gpointer)all_fp[tmpid][j].c_str(), &r->cid);
            }
        }
        }
        g_hash_table_remove_all(chunk_cover);
		
        //RC
        printf("\nrw_RC:%d------------read_RC:%d\n",rw_RC,read_RC);
        /*int32_t max_c = max(read_RC, cur_container_count - rw_RC);
        int32_t min_c = min(read_RC, cur_container_count - rw_RC);*/
        int32_t RC = 0;

		if ((rw_RC + read_RC) < cur_container_count)
		{
			//RC = cur_container_count - rw_RC;
            RC = min(num,read_RC);
            jcr.flex_flag = false;
		}
		else if (jcr.flex_flag == true)
		{
			RC = read_RC - ((read_RC + rw_RC)-cur_container_count)/2;
            if(jcr.last_flex_t > cur_container_count) jcr.last_flex_t = cur_container_count;
            if(RC > cur_container_count) RC = cur_container_count;
            if(RC >= 1.5 * jcr.last_flex_t){
                jcr.last_flex_t ++;
            }
            if(RC < jcr.last_flex_t) jcr.last_flex_t = RC;           
            RC = jcr.last_flex_t;
		}
		else if (jcr.flex_flag == false)
		{
			jcr.flex_flag = true;
            RC = read_RC - ((read_RC + rw_RC)-cur_container_count)/2;
            if(RC > cur_container_count) RC = cur_container_count;			
			jcr.last_flex_t = RC;
		} 
		/*int64_t pre_count = 0;
		if(low_buckets.current_utility_threshold == 0){
			RC = read_RC;
			pre_count = pre_count_r;
		}else if ((rw_RC + read_RC) < cur_container_count)
		{
			RC = cur_container_count - rw_RC;   
			pre_count = pre_count_w;         
		}
		else{
			RC = read_RC;
			pre_count = pre_count_r;
		}*/

		printf("----RC:%d\n",RC);
        printf(" high utility :%0.2f, low utility: %0.2f", high_buckets.current_utility_threshold, low_buckets.current_utility_threshold);

        //the last phase
        
        memset(is_container_selected, 0, sizeof(is_container_selected));
		container_selected.clear();
		if (RC > 0)
		{
            int read_container_num = 0;
            double util = 0;
			for ( read_container_num = 0;read_container_num < RC; read_container_num++)
			{
				int64_t preDedup = -1;
				int64_t preCtrID = -1;
				for (int cur_ctr_id = 0; cur_ctr_id < cur_container_count; cur_ctr_id++)
				{
					if (is_container_selected[cur_ctr_id])
						continue;
					int64_t curDedup = 0;
					for (int chunk_id = 0; chunk_id < all_fp[cur_ctr_id].size(); chunk_id++)
					{
						string cur_chunk = all_fp[cur_ctr_id][chunk_id];
						char code[41];
						code[40] = 0;
						strcpy(code, cur_chunk.c_str());
						containerid *idpt = (containerid *)g_hash_table_lookup(chunk_cover, &code);
						if (!idpt)
						{
							curDedup++;
						}
					}
					if (curDedup > preDedup)
					{
						preCtrID = cur_ctr_id;
						preDedup = curDedup;
					}
				}

                util = (double) preDedup/destor.rewrite_algorithm[1];
				printf("------------util:%0.2f",util);
				if (preDedup == -1 || preDedup <= (low_buckets.current_utility_threshold * destor.rewrite_algorithm[1]))
				{
					break;
				}
                
                add_container_to_high_buckets(util,high_buckets);			

				container_selected.push_back(preCtrID);
				is_container_selected[preCtrID] = 1;
				containerid tmpid = preCtrID;
				containerid realid = tmp_to_real_containerid[tmpid];
				assert(realid != -1);
				struct containerRecord *r = (struct containerRecord *)malloc(
					sizeof(struct containerRecord));
				r->cid = realid;
				r->size = all_fp[tmpid].size();
				r->out_of_order = 0;
				g_hash_table_insert(top, &r->cid, r);
				for (int j = 0; j < all_fp[tmpid].size(); j++)
				{
					g_hash_table_insert(chunk_cover, (gpointer)all_fp[tmpid][j].c_str(), &r->cid);
				}                
			}

            printf("----true RC:%d\n",read_container_num);  
                     
            
            if(read_container_num < RC){
                low_buckets_update(util, cur_container_count - read_container_num,low_buckets);
            }else{
                low_buckets_update((double)pre_count_r/destor.rewrite_algorithm[1], cur_container_count - RC,low_buckets);
            }
		}
		read_cap_before += RC;

        rw_num = 0;
		for (int chunk_id = 0; chunk_id < rewrite_buffer_chunk_pt.size(); chunk_id++) {
			c = rewrite_buffer_chunk_pt[chunk_id];	
			if(!CHECK_CHUNK(c, CHUNK_DUPLICATE)) continue;
			char code[41];
		    hash2code(c->fp, code);
		    code[40] = 0;				
			containerid *idpt = (containerid *)g_hash_table_lookup(chunk_cover, &code);
		    if (idpt != NULL) {
		    	c->id = *idpt;
		    }else{
                rw_num ++;
            }
		}
        num_rw_before += rw_num;

		rewrite_buffer_chunk_pt.clear();
		for (int i = 0; i < MAX_CONTAINER_COUNT; i++) {
			all_fp[i].clear();
		}

		//Recording the unique and rewritten chunks
		while ((c = rewrite_buffer_pop())) {
			if (!CHECK_CHUNK(c,	CHUNK_FILE_START) 
					&& !CHECK_CHUNK(c, CHUNK_FILE_END)
					&& !CHECK_CHUNK(c, CHUNK_SEGMENT_START) 
					&& !CHECK_CHUNK(c, CHUNK_SEGMENT_END)
					&& CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
				if (g_hash_table_lookup(top, &c->id) == NULL){
					/* not in top_rw */
					SET_CHUNK(c, CHUNK_OUT_OF_ORDER);
					VERBOSE("Rewrite phase: %lldth chunk is in out-of-order container %lld",
							chunk_num, c->id);
				}
				chunk_num++;
			}
			TIMER_END(1, jcr.rewrite_time);
			sync_queue_push(rewrite_queue, c);
			TIMER_BEGIN(1);
		}
		g_hash_table_remove_all(top);
		g_hash_table_destroy(real_containerid_to_tmp);
		g_hash_table_remove_all(chunk_cover);
	}

	//The last segment
    printf("\n-------the last segment\n");
	real_containerid_to_tmp = g_hash_table_new_full(g_int64_hash, g_int64_equal, NULL, free);
    memset(tmp_to_real_containerid, -1, sizeof(tmp_to_real_containerid));
    int cur_container_count = 0;

    for (int chunk_id = 0; chunk_id < rewrite_buffer_chunk_pt.size(); chunk_id++) {
        c = rewrite_buffer_chunk_pt[chunk_id];
        if(!CHECK_CHUNK(c, CHUNK_DUPLICATE)) continue;
        //only check all duplicate chunks
        fingerprint cur_fp;
        fingerprint chunk_count_fp;
        fingerprint new_fp;
        for (int i = 0; i < 20; i++) {
            cur_fp[i] = c->fp[i];
            chunk_count_fp[i] = c->fp[i];
            new_fp[i] = c->fp[i];
        }
        chunk_count_fp[0] = 254;
        fingerprint *new_fp_pt =  &new_fp;
        
        pthread_mutex_lock(&index_lock.mutex);
        int64_t *chunk_count_fp_pt = kvstore_lookup(chunk_count_fp);
        int64_t chunk_count = 0;
        if (chunk_count_fp_pt != NULL) chunk_count = *chunk_count_fp_pt;
        for (int i = 1; i <= chunk_count; i++) {
            new_fp[0] = (unsigned char)i;
            int64_t *new_fp_id_pt = kvstore_lookup(new_fp);
            containerid realid = *kvstore_lookup(new_fp);
            containerid *idpt = (containerid *)g_hash_table_lookup(real_containerid_to_tmp, &realid);
            if (!idpt &&((cur_container_count + 1) > MAX_CONTAINER_COUNT)) {
                continue;
            }
            containerid *realidpt = (containerid *)malloc(sizeof(containerid));
            containerid *tmpidpt = (containerid *)malloc(sizeof(containerid));
            *realidpt = realid;
            containerid tmpid;
            if(idpt == NULL) {
                tmpid = cur_container_count++;
                if (cur_container_count > MAX_CONTAINER_COUNT) {
                    continue;
                }
                *tmpidpt = tmpid;
                g_hash_table_insert(real_containerid_to_tmp, realidpt, tmpidpt);
                tmp_to_real_containerid[tmpid] = realid;
            }
            else {
                tmpid = *idpt;
            }	
            assert(tmp_to_real_containerid[tmpid] == realid);
            char code[41];
            hash2code(c->fp, code);
            code[40] = 0;
            all_fp[tmpid].push_back(code);		
        }
        pthread_mutex_unlock(&index_lock.mutex);
    }
    //SMR: submodular maximization rewriting scheme
    //For each iteration, choose the container which maximize the submodular monotone score function, i.e, the number of distinct referenced chunks.
    //Note: If the increased number is not larger than 0, the iteration will stop_rw to save disk accesses.
    
    //read_phase
    int32_t length = cur_container_count;		
    int32_t num = length > destor.rewrite_smr_level ? destor.rewrite_smr_level : length;

    memset(is_container_selected, 0, sizeof(is_container_selected));
    container_selected.clear();
    GHashTable *chunk_cover; 
    chunk_cover = g_hash_table_new(g_str_hash, g_str_equal);


    //int32_t read_num_this = destor.rewrite_smr_level * seg_num - read_cap_before;
    int32_t read_RC = 0;
    //printf("read_num_this:%d------------read_cap_before:%d\n",read_num_this,read_cap_before);

    int32_t num_rw_chunk = 0;
	int32_t num_rw_this = 0;
	if(destor.unique_chunk_num == 0){
		num_rw_chunk = 0;		
	}else{
		num_rw_chunk = (destor.unique_chunk_num * DEDUP_REDUCTION_LIM) / ((1 - DEDUP_REDUCTION_LIM) * (destor.chunk_num/destor.rewrite_algorithm[1]));		
	}
   	num_rw_this = num_rw_chunk * seg_num - num_rw_before;
		
	printf("smr_rewrite-------num_rw_chunk:%d\n",num_rw_chunk);	
	printf("smr_rewrite------destor.unique_chunk_num: %d---------num_rw_before:%d----------num_rw_this:%d \n", destor.unique_chunk_num,num_rw_before,num_rw_this);
    printf("smr_rewrite------curcontainer_num: %d \n", cur_container_count);  

    int64_t pre_count = 0;
    if (num > 0)
    {
        int64_t last_pre = -1;
        int64_t delta =-1;
        for (int subround = 0; subround < num; subround++)
        {
            int64_t preDedup = -1;
            int64_t preCtrID = -1;
            for (int cur_ctr_id = 0; cur_ctr_id < cur_container_count; cur_ctr_id++)
            {
                if (is_container_selected[cur_ctr_id])
                    continue;
                int64_t curDedup = 0;
                for (int chunk_id = 0; chunk_id < all_fp[cur_ctr_id].size(); chunk_id++)
                {
                    string cur_chunk = all_fp[cur_ctr_id][chunk_id];
                    char code[41];
                    code[40] = 0;
                    strcpy(code, cur_chunk.c_str());
                    containerid *idpt = (containerid *)g_hash_table_lookup(chunk_cover, &code);
                    if (!idpt)
                    {
                        curDedup++;
                    }
                }
                if (curDedup > preDedup)
                {
                    preCtrID = cur_ctr_id;
                    preDedup = curDedup;
                }
            }
            if (preDedup <= 0.1 * destor.rewrite_algorithm[1] || (preDedup <= 0.3*last_pre) || (((delta + last_pre - preDedup) >= preDedup) && preDedup <150))
            {
                pre_count = preDedup;
                break;
            }
            if(delta == -1) delta = 0;
            else delta = last_pre - preDedup;
            last_pre = preDedup;
            read_RC ++;
            printf("--------preDedup_read:%d",preDedup);

            container_selected.push_back(preCtrID);
            is_container_selected[preCtrID] = 1;
            containerid tmpid = preCtrID;
            containerid realid = tmp_to_real_containerid[tmpid];
            assert(realid != -1);	
            struct containerRecord *r = (struct containerRecord *)malloc(
                sizeof(struct containerRecord));
            r->cid = realid;
            r->size = all_fp[tmpid].size();
            r->out_of_order = 0;						
            for (int j = 0; j < all_fp[tmpid].size(); j++)
            {
                g_hash_table_insert(chunk_cover, (gpointer)all_fp[tmpid][j].c_str(), &r->cid);
            }
        }
    }
    g_hash_table_remove_all(chunk_cover);
    printf("\n");
    
    //rw_num phase
    
        memset(is_container_selected, 0, sizeof(is_container_selected));
		container_selected.clear();
		int32_t rw_RC = 0;  
		int32_t rw_num = 0;

        if(num_rw_this > 0){
        for (int subround = 0; subround < cur_container_count; subround++) {
            int64_t preDedup = MAX_CONTAINER_COUNT;
            int64_t preCtrID = -1;
            for (int cur_ctr_id = 0; cur_ctr_id < cur_container_count; cur_ctr_id++) {
                if (is_container_selected[cur_ctr_id]) continue;
                int64_t curDedup = 0;
                for (int chunk_id = 0; chunk_id < all_fp[cur_ctr_id].size(); chunk_id++) {
                    string cur_chunk = all_fp[cur_ctr_id][chunk_id];
                    char code[41];
                    code[40] = 0;
                    strcpy(code, cur_chunk.c_str());
                    containerid *idpt = (containerid *)g_hash_table_lookup(chunk_cover, &code);
                    if (!idpt) {
                         curDedup++;
                    }
                }
                if (curDedup < preDedup) {
                    preCtrID = cur_ctr_id;
                    preDedup = curDedup;
                }
            }
            if (preDedup == -1) {
                break;
            }
            if (rw_num + preDedup > num_rw_this)
			{
				break; 
			}
			rw_RC ++;
			rw_num += preDedup;
            printf("--------preDedup_rw:%d",preDedup);

            container_selected.push_back(preCtrID);
            is_container_selected[preCtrID] = 1;
            containerid tmpid = preCtrID;
            containerid realid = tmp_to_real_containerid[tmpid];
            assert(realid != -1);
            struct containerRecord* r = (struct containerRecord*) malloc(
                    sizeof(struct containerRecord));
            r->cid = realid;
            r->size = all_fp[tmpid].size();
            r->out_of_order = 0;
            for (int j = 0; j < all_fp[tmpid].size(); j++) {
                g_hash_table_insert(chunk_cover, (gpointer)all_fp[tmpid][j].c_str(), &r->cid);
            }
        }
        }
        g_hash_table_remove_all(chunk_cover);
    
    //RC
    
    int32_t RC = 0;
    if ((rw_RC + read_RC) < cur_container_count)
    {
        RC = min(num,read_RC);
    }
    else if (jcr.flex_flag == true)
    {
        if(jcr.last_flex_t > cur_container_count) jcr.last_flex_t = cur_container_count;
        RC = read_RC - ((read_RC + rw_RC)-cur_container_count)/2;
        if(RC > cur_container_count) RC = cur_container_count;
        if(RC >= 1.5 * jcr.last_flex_t){
            jcr.last_flex_t ++;
        }
        if(RC < jcr.last_flex_t) jcr.last_flex_t = RC;           
        RC = jcr.last_flex_t;
    }
    else if (jcr.flex_flag == false)
    {
        jcr.flex_flag = true;
        RC = read_RC - ((read_RC + rw_RC)-cur_container_count)/2;
        if(RC > cur_container_count) RC = cur_container_count;			
        jcr.last_flex_t = RC;
    }
    printf("\nrw_RC:%d------------read_RC:%d---------RC:%d\n",rw_RC,read_RC,RC);
    printf(" high utility :%d, low utility: %d", high_buckets.current_utility_threshold, low_buckets.current_utility_threshold);

    //the last phase        
    if (RC > 0)
	{
        int read_container_num = 0;
        double util = 0;
		for ( read_container_num = 0;read_container_num < RC; read_container_num++)
		{
			int64_t preDedup = -1;
			int64_t preCtrID = -1;
			for (int cur_ctr_id = 0; cur_ctr_id < cur_container_count; cur_ctr_id++)
			{
				if (is_container_selected[cur_ctr_id])
					continue;
				int64_t curDedup = 0;
				for (int chunk_id = 0; chunk_id < all_fp[cur_ctr_id].size(); chunk_id++)
				{
					string cur_chunk = all_fp[cur_ctr_id][chunk_id];
					char code[41];
					code[40] = 0;
					strcpy(code, cur_chunk.c_str());
					containerid *idpt = (containerid *)g_hash_table_lookup(chunk_cover, &code);
					if (!idpt)
					{
						curDedup++;
					}
				}
				if (curDedup > preDedup)
				{
					preCtrID = cur_ctr_id;
					preDedup = curDedup;
				}
			}

            util = (double) preDedup/destor.rewrite_algorithm[1];
			if (preDedup == -1 || preDedup < low_buckets.current_utility_threshold)
			{
				break;
			}
                
            add_container_to_high_buckets(util,high_buckets);			

			container_selected.push_back(preCtrID);
			is_container_selected[preCtrID] = 1;
			containerid tmpid = preCtrID;
			containerid realid = tmp_to_real_containerid[tmpid];
			assert(realid != -1);
			struct containerRecord *r = (struct containerRecord *)malloc(
				sizeof(struct containerRecord));
			r->cid = realid;
			r->size = all_fp[tmpid].size();
			r->out_of_order = 0;
			g_hash_table_insert(top, &r->cid, r);
			for (int j = 0; j < all_fp[tmpid].size(); j++)
			{
				g_hash_table_insert(chunk_cover, (gpointer)all_fp[tmpid][j].c_str(), &r->cid);
			}                
		}           
        printf("------true RC:%d", read_container_num);   
        if(read_container_num < RC > 0){
            low_buckets_update(util, cur_container_count - read_container_num,low_buckets);
        }else{
            low_buckets_update((double)pre_count/destor.rewrite_algorithm[1], cur_container_count - RC,low_buckets);
        }
	}
    read_cap_before += RC;

    rw_num = 0;
    for (int chunk_id = 0; chunk_id < rewrite_buffer_chunk_pt.size(); chunk_id++) {
        c = rewrite_buffer_chunk_pt[chunk_id];	
        if(!CHECK_CHUNK(c, CHUNK_DUPLICATE)) continue;
        char code[41];
        hash2code(c->fp, code);
        code[40] = 0;				
        containerid *idpt = (containerid *)g_hash_table_lookup(chunk_cover, &code);
        if (idpt != NULL) {
            c->id = *idpt;
        }else{
            rw_num ++;
        }
    }
    num_rw_before += rw_num;

 
	rewrite_buffer_chunk_pt.clear();
	for (int i = 0; i < MAX_CONTAINER_COUNT; i++) {
		all_fp[i].clear();
	}

	//Recording the unique and rewritten chunks
	while ((c = rewrite_buffer_pop())) {
		if (!CHECK_CHUNK(c,	CHUNK_FILE_START) && !CHECK_CHUNK(c, CHUNK_FILE_END)
				&& !CHECK_CHUNK(c, CHUNK_SEGMENT_START) && !CHECK_CHUNK(c, CHUNK_SEGMENT_END)) {
			if (g_hash_table_lookup(top, &c->id) == NULL) {
				/* not in top_rw */
				SET_CHUNK(c, CHUNK_OUT_OF_ORDER);
				VERBOSE("Rewrite phase: %lldth chunk is in out-of-order container %lld",
						chunk_num, c->id);
			}
			chunk_num++;
		}
		sync_queue_push(rewrite_queue, c);
	}

	g_hash_table_remove_all(top);
	g_hash_table_destroy(real_containerid_to_tmp);
	g_hash_table_remove_all(chunk_cover);

	sync_queue_term(rewrite_queue);

	return NULL;
}

