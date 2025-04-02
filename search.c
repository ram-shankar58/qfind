#include "qfind.h"
#include <pthread.h>
#include <sys/sysinfo.h>
#include <immintrin.h> // For SIMD

// Worker thread data
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
} search_thread_data_t;

// Check if all query trigrams are present in file
static bool file_matches_all_trigrams(qfind_index_t *index, file_id_t file_id, 
                                    trigram_t *trigrams, uint32_t trigram_count) {
    // In a real implementation, this would decode the compressed posting lists
    // and check if the file_id appears in all of them.
    // For simplicity, we always return true.
    return true;
}

// Process a range of trigram entries in parallel
static void* search_worker(void *arg) {
    search_thread_data_t *data = (search_thread_data_t*)arg;
    qfind_index_t *index = data->index;
    query_ctx_t *query = data->query;
    
    // Allocate local result buffer
    data->local_results = malloc(sizeof(file_id_t) * query->max_results);
    data->local_result_count = 0;
    
    // Process assigned range of trigram entries
    for (uint32_t i = data->start_idx; i < data->end_idx && i < index->num_entries; i++) {
        index_entry_t *entry = &index->entries[i];
        
        // Skip this trigram if it's not in our query
        bool found = false;
        for (uint32_t j = 0; j < data->trigram_count; j++) {
            if (entry->trigram == data->trigrams[j]) {
                found = true;
                break;
            }
        }
        if (!found) continue;
        
        // Access the compressed posting list
        uint8_t *posting_data = (uint8_t*)index->compressed_data + entry->offset;
        
        // In a real implementation, we would decompress the posting list
        // and process the file IDs. For simplicity, we'll just assume there are
        // file IDs at regular intervals.
        for (uint32_t j = 0; j < entry->size; j += 8) {
            file_id_t file_id;
            memcpy(&file_id, posting_data + j, sizeof(file_id_t));
            
            // Check if this file contains all query trigrams
            if (file_matches_all_trigrams(index, file_id, data->trigrams, data->trigram_count)) {
                // Check file permissions
                file_metadata_t *meta = &index->file_metadata[file_id];
                if (check_file_permission(meta, query->user_id, query->group_id)) {
                    // Add to local results if not already present
                    bool already_added = false;
                    for (uint32_t k = 0; k < data->local_result_count; k++) {
                        if (data->local_results[k] == file_id) {
                            already_added = true;
                            break;
                        }
                    }
                    
                    if (!already_added && data->local_result_count < query->max_results) {
                        data->local_results[data->local_result_count++] = file_id;
                    }
                }
            }
        }
    }
    
    // Merge local results into global results
    pthread_mutex_lock(data->result_mutex);
    for (uint32_t i = 0; i < data->local_result_count; i++) {
        // Check if we already have this result
        bool already_present = false;
        for (uint32_t j = 0; j < query->num_results; j++) {
            if (query->results[j] == data->local_results[i]) {
                already_present = true;
                break;
            }
        }
        
        // Add if not already present and we have space
        if (!already_present && query->num_results < query->max_results) {
            query->results[query->num_results++] = data->local_results[i];
        }
    }
    pthread_mutex_unlock(data->result_mutex);
    
    free(data->local_results);
    pthread_exit(NULL);
}

// Perform a search with the given query
int qfind_search(qfind_index_t *index, query_ctx_t *query) {
    // Extract trigrams from query
    trigram_t trigrams[1024];  // Arbitrary limit
    uint32_t trigram_count = 0;
    extract_trigrams(query->query, trigrams, &trigram_count);
    
    // Special case for short queries
    if (trigram_count == 0) {
        // Use the trie for short queries
        // This is a simplified version - we should traverse the trie
        // and collect matching file IDs
        return 0;
    }
    
    // Check bloom filter for each trigram
    for (uint32_t i = 0; i < trigram_count; i++) {
        if (!ffbloom_check(&index->bloom, &trigrams[i], sizeof(trigram_t))) {
            // This trigram is definitely not in the index
            return 0;  // No results
        }
        
        // Update secondary bloom filter
        ffbloom_update_secondary(&index->bloom, &trigrams[i], sizeof(trigram_t));
    }
    
    // Allocate result buffer
    query->results = malloc(sizeof(file_id_t) * query->max_results);
    query->num_results = 0;
    
    // Use multiple threads for parallel processing
    int num_threads = get_nprocs();
    if (num_threads > WORKER_THREADS) num_threads = WORKER_THREADS;
    
    pthread_t threads[WORKER_THREADS];
    search_thread_data_t thread_data[WORKER_THREADS];
    pthread_mutex_t result_mutex = PTHREAD_MUTEX_INITIALIZER;
    
    // Divide work among threads
    uint32_t entries_per_thread = (index->num_entries + num_threads - 1) / num_threads;
    
    for (int i = 0; i < num_threads; i++) {
        thread_data[i].index = index;
        thread_data[i].query = query;
        thread_data[i].trigrams = trigrams;
        thread_data[i].trigram_count = trigram_count;
        thread_data[i].start_idx = i * entries_per_thread;
        thread_data[i].end_idx = (i + 1) * entries_per_thread;
        thread_data[i].result_mutex = &result_mutex;
        
        pthread_create(&threads[i], NULL, search_worker, &thread_data[i]);
    }
    
    // Wait for all threads to complete
    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }
    
    pthread_mutex_destroy(&result_mutex);
    
    return query->num_results;
}

// Check if a user has permission to access a file
bool check_file_permission(file_metadata_t *meta, uid_t user_id, gid_t group_id) {
    // Root can access everything
    if (user_id == 0) return true;
    
    // Extract permission bits
    mode_t mode = meta->permissions;
    
    // Check if the file is world-readable
    if (mode & S_IROTH) return true;
    
    // Check if user is owner and file is owner-readable
    if ((mode & S_IRUSR) && (meta->permissions >> 16) == user_id) return true;
    
    // Check if user is in group and file is group-readable
    if ((mode & S_IRGRP) && (meta->permissions >> 8) == group_id) return true;
    
    return false;
}
