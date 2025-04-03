#include "qfind.h"
#include <pthread.h>
#include <sys/sysinfo.h>
#include <zstd.h>
#include <fnmatch.h>

#define MAX_TRIGRAMS 1024
#define RESULTS_PER_THREAD 512
#define DECOMPRESS_BUF_SIZE (1 << 20) // 1MB

typedef struct {
    qfind_index_t *index;
    query_ctx_t *query;
    trigram_t *trigrams;
    uint32_t trigram_count;
    uint32_t start_idx;
    uint32_t end_idx;
    file_id_t *local_results;
    uint32_t local_result_count;
    pthread_mutex_t *result_mutex;
    ZSTD_DCtx *dctx;
} search_thread_data_t;

// Golomb-Rice decoding parameters
typedef struct {
    uint8_t k;
    uint32_t mask;
    uint32_t current;
    const uint8_t *input;
    size_t remaining;
} gr_decoder_t;

static void gr_decoder_init(gr_decoder_t *dec, uint8_t k, const uint8_t *data, size_t size) {
    dec->k = k;
    dec->mask = (1U << k) - 1;
    dec->input = data;
    dec->remaining = size;
    dec->current = 0;
}

static int gr_decode_next(gr_decoder_t *dec) {
    uint32_t q = 0;
    
    // Decode unary quotient
    while (dec->remaining > 0 && *dec->input == 0xFF) {
        q += 8;
        dec->input++;
        dec->remaining--;
    }
    
    if (dec->remaining == 0) return -1;
    
    uint8_t val = *dec->input++;
    dec->remaining--;
    
    while (val & 0x80) {
        q++;
        val <<= 1;
    }
    
    // Decode binary remainder
    uint32_t r = val >> (8 - dec->k);
    uint32_t read_bits = dec->k;
    
    while (read_bits > 8) {
        if (dec->remaining == 0) return -1;
        r = (r << 8) | *dec->input++;
        dec->remaining--;
        read_bits -= 8;
    }
    
    return (q << dec->k) | (r & dec->mask);
}

static bool file_matches_all_trigrams(qfind_index_t *index, file_id_t file_id, 
                                    trigram_t *trigrams, uint32_t trigram_count) {
    // Check bloom filter first
    for (uint32_t i = 0; i < trigram_count; i++) {
        if (!ffbloom_check(index->bloom, &trigrams[i], sizeof(trigram_t))) {
            return false;
        }
    }
    return true;
}

static void search_trie(trie_node_t *node, const char *query, size_t pos,
                       file_id_t *results, uint32_t *count, uint32_t max_results) {
    if (!node) return;

    // Handle path compression
    if (node->key == 0xFF && node->count > 1) {
        if (query[pos] == query[pos+1]) {
            size_t run_length = 1;
            while (pos + run_length < strlen(query) && 
                   query[pos] == query[pos + run_length] && 
                   run_length < node->count) {
                run_length++;
            }
            
            if (run_length == node->count) {
                pos += node->count;
            } else {
                return;
            }
        } else {
            return;
        }
    } else if (node->key != 0xFF) {
        if (pos >= strlen(query) || (node->key != query[pos])) {
            return;
        }
        pos++;
    }

    if (pos == strlen(query) && node->is_end) {
        if (*count < max_results) {
            results[(*count)++] = node->file_id;
        }
        return;
    }

    for (int i = 0; i < 256; i++) {
        if (node->children[i]) {
            search_trie(node->children[i], query, pos, results, count, max_results);
        }
    }
}

static void* search_worker(void *arg) {
    search_thread_data_t *data = (search_thread_data_t*)arg;
    uint8_t *decompress_buf = malloc(DECOMPRESS_BUF_SIZE);
    file_id_t *seen = calloc(data->index->num_files, sizeof(bool));

    for (uint32_t i = data->start_idx; i < data->end_idx && i < data->index->num_entries; i++) {
        index_entry_t *entry = &data->index->entries[i];
        bool trigram_in_query = false;
        
        for (uint32_t j = 0; j < data->trigram_count; j++) {
            if (entry->trigram == data->trigrams[j]) {
                trigram_in_query = true;
                break;
            }
        }
        if (!trigram_in_query) continue;

        // Decompress the posting list
        size_t decomp_size = ZSTD_decompressDCtx(data->dctx, decompress_buf, DECOMPRESS_BUF_SIZE,
                                                data->index->compressed_data + entry->offset,
                                                entry->size);
        if (ZSTD_isError(decomp_size)) {
            syslog(LOG_ERR, "Decompression error: %s", ZSTD_getErrorName(decomp_size));
            continue;
        }

        gr_decoder_t gr;
        gr_decoder_init(&gr, 4, decompress_buf, decomp_size); // k=4 optimized empirically
        uint32_t prev_id = 0;

        while (1) {
            int delta = gr_decode_next(&gr);
            if (delta < 0) break;
            
            uint32_t file_id = prev_id + delta;
            prev_id = file_id;

            if (file_id >= data->index->num_files) break;
            if (seen[file_id]) continue;
            
            seen[file_id] = true;

            if (file_matches_all_trigrams(data->index, file_id, 
                                         data->trigrams, data->trigram_count)) {
                file_metadata_t *meta = &data->index->file_metadata[file_id];
                if (check_file_permission(meta, data->query->user_id, 
                                        data->query->group_id)) {
                    if (data->local_result_count < RESULTS_PER_THREAD) {
                        data->local_results[data->local_result_count++] = file_id;
                    }
                }
            }
        }
    }

    // Merge results
    pthread_mutex_lock(data->result_mutex);
    for (uint32_t i = 0; i < data->local_result_count; i++) {
        if (data->query->num_results >= data->query->max_results) break;
        
        bool exists = false;
        for (uint32_t j = 0; j < data->query->num_results; j++) {
            if (data->query->results[j] == data->local_results[i]) {
                exists = true;
                break;
            }
        }
        
        if (!exists) {
            data->query->results[data->query->num_results++] = data->local_results[i];
        }
    }
    pthread_mutex_unlock(data->result_mutex);

    free(decompress_buf);
    free(seen);
    pthread_exit(NULL);
}

int qfind_search(qfind_index_t *index, query_ctx_t *query) {
    if (!query->query || !*query->query) return 0;
    
    trigram_t trigrams[MAX_TRIGRAMS];
    uint32_t trigram_count = 0;
    extract_trigrams(query->query, trigrams, &trigram_count, MAX_TRIGRAMS);

    // Short query path (<= 2 chars)
    if (trigram_count == 0) {
        query->results = malloc(sizeof(file_id_t) * query->max_results);
        query->num_results = 0;
        search_trie(index->trie_root, query->query, 0, 
                   query->results, &query->num_results, query->max_results);
        return query->num_results;
    }

    // Bloom filter pre-check
    for (uint32_t i = 0; i < trigram_count; i++) {
        if (!ffbloom_check(index->bloom, &trigrams[i], sizeof(trigram_t))) {
            return 0;
        }
        ffbloom_update_secondary(index->bloom, &trigrams[i], sizeof(trigram_t));
    }

    // Allocate result buffers
    query->results = malloc(sizeof(file_id_t) * query->max_results);
    query->num_results = 0;
    if (!query->results) return -1;

    // Thread setup
    int num_threads = get_nprocs();
    num_threads = num_threads > WORKER_THREADS ? WORKER_THREADS : num_threads;
    pthread_t threads[WORKER_THREADS];
    search_thread_data_t thread_data[WORKER_THREADS];
    pthread_mutex_t result_mutex = PTHREAD_MUTEX_INITIALIZER;
    ZSTD_DCtx *dctx = ZSTD_createDCtx();

    uint32_t entries_per_thread = (index->num_entries + num_threads - 1) / num_threads;

    for (int i = 0; i < num_threads; i++) {
        thread_data[i] = (search_thread_data_t){
            .index = index,
            .query = query,
            .trigrams = trigrams,
            .trigram_count = trigram_count,
            .start_idx = i * entries_per_thread,
            .end_idx = (i + 1) * entries_per_thread,
            .result_mutex = &result_mutex,
            .dctx = dctx,
            .local_results = malloc(sizeof(file_id_t) * RESULTS_PER_THREAD)
        };
        
        pthread_create(&threads[i], NULL, search_worker, &thread_data[i]);
    }

    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
        free(thread_data[i].local_results);
    }

    ZSTD_freeDCtx(dctx);
    pthread_mutex_destroy(&result_mutex);
    
    // Sort results by relevance (simple path length heuristic)
    qsort(query->results, query->num_results, sizeof(file_id_t), 
         (int (*)(const void*, const void*))strcmp);
    
    return query->num_results;
}

bool check_file_permission(const file_metadata_t *meta, uid_t user_id, gid_t group_id) {
    if (user_id == 0) return true; // Root access
    
    mode_t mode = meta->permissions;
    uid_t file_uid = meta->permissions >> 16;
    gid_t file_gid = (meta->permissions >> 8) & 0xFF;

    if ((mode & S_IROTH)) return true;
    if ((file_uid == user_id) && (mode & S_IRUSR)) return true;
    if ((file_gid == group_id) && (mode & S_IRGRP)) return true;
    
    return false;
}
// void extract_trigrams(const char *query, trigram_t *trigrams, uint32_t *count, uint32_t max_count) {
//     size_t len = strlen(query);
//     if (len < 3) return;

//     for (size_t i = 0; i <= len - 3 && *count < max_count; i++) {
//         trigrams[(*count)++] = (trigram_t){query[i], query[i+1], query[i+2]};
//     }
// }
