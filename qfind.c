#include "qfind.h"
#include <dirent.h>
#include <sys/stat.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <syslog.h>
#include <limits.h>
#include <zstd.h>
#include <math.h>
#include <sys/mman.h>

#define INITIAL_META_CAPACITY 1024
#define META_GROW_FACTOR 2
#define MAX_DIR_DEPTH 64
#define TRIE_PATH_COMPRESS 0xFF
#define MAX_CANDIDATES 100000
#define SCORE_THRESHOLD 0.25f
#define POSTING_CACHE_SIZE 1024
#define MAX_TRIGRAMS 1024 


typedef struct {
    file_id_t file_id;
    float score;
} scored_result_t;

int add_file_to_index(qfind_index_t *index, const char *path, file_id_t id);
static int process_directory(qfind_index_t *index, const char *path, int depth);
static void free_trie(trie_node_t *node);
static int insert_path_to_trie(trie_node_t *root, const char *path, file_id_t id);
static int compare_scores(const void *a, const void *b);
static void process_posting_list(qfind_index_t *index, index_entry_t *entry,
                               file_id_t **candidates, uint32_t *num_candidates);
static float calculate_relevance_score(file_metadata_t *meta,
                                      trigram_t *query_trigrams,
                                      uint32_t trigram_count);
static void search_trie(trie_node_t *node, const char *query, size_t pos,
                       file_id_t *results, uint32_t *count, uint32_t max_results);

qfind_index_t* qfind_init(void) {
    qfind_index_t *index = calloc(1, sizeof(qfind_index_t));
    if (!index) return NULL;

    index->bloom = ffbloom_create(BLOOM_SIZE, BLOOM_SEC_SIZE);
    if (!index->bloom) {
        free(index);
        return NULL;
    }

    index->trie_root = calloc(1, sizeof(trie_node_t));
    if (!index->trie_root) {
        ffbloom_destroy(index->bloom);
        free(index);
        return NULL;
    }

    if (io_context_init(&index->io, IO_RINGSIZE, false) != 0) {
        ffbloom_destroy(index->bloom);
        free(index->trie_root);
        free(index);
        return NULL;
    }

    pthread_rwlock_init(&index->index_lock, NULL);
    index->meta_capacity = 0;
    index->num_files = 0;
    
    return index;
}

void qfind_destroy(qfind_index_t *index) {
    if (!index) return;

    ffbloom_destroy(index->bloom);
    io_context_destroy(&index->io);

    if (index->trie_root) {
        free_trie(index->trie_root);
    }

    free(index->compressed_data);
    free(index->file_metadata);
    free(index->entries);
    pthread_rwlock_destroy(&index->index_lock);
    free(index);
}

static void free_trie(trie_node_t *node) {
    if (!node) return;
    
    for (int i = 0; i < 256; i++) {
        if (node->children[i]) {
            free_trie(node->children[i]);
        }
    }
    free(node);
}

static int process_directory(qfind_index_t *index, const char *base_path, int depth) {
    if (depth > MAX_DIR_DEPTH) {
        syslog(LOG_WARNING, "Max directory depth exceeded: %s", base_path);
        return -ELOOP;
    }

    DIR *dir = opendir(base_path);
    if (!dir) return -errno;

    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) continue;

        char full_path[PATH_MAX];
        if (snprintf(full_path, sizeof(full_path), "%s/%s", base_path, entry->d_name) >= PATH_MAX) {
            syslog(LOG_WARNING, "Path truncated: %s/%s", base_path, entry->d_name);
            continue;
        }

        struct stat st;
        if (lstat(full_path, &st) == -1) {
            syslog(LOG_ERR, "lstat(%s) failed: %m", full_path);
            continue;
        }

        if (S_ISDIR(st.st_mode)) {
            int fd = open(full_path, O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
            if (fd == -1) continue;

            int ret = process_directory(index, full_path, depth + 1);
            close(fd);
            if (ret != 0) {
                closedir(dir);
                return ret;
            }
        }
        else if (S_ISREG(st.st_mode) || S_ISLNK(st.st_mode)) {
            pthread_rwlock_wrlock(&index->index_lock);

            if (index->num_files >= index->meta_capacity) {
                size_t new_cap = index->meta_capacity ? 
                    index->meta_capacity * META_GROW_FACTOR : INITIAL_META_CAPACITY;
                file_metadata_t *new_meta = realloc(index->file_metadata,
                                                  new_cap * sizeof(file_metadata_t));
                if (!new_meta) {
                    pthread_rwlock_unlock(&index->index_lock);
                    closedir(dir);
                    return -ENOMEM;
                }
                index->file_metadata = new_meta;
                index->meta_capacity = new_cap;
            }

            file_metadata_t *meta = &index->file_metadata[index->num_files];
            strncpy(meta->path, full_path, PATH_MAX - 1);
            meta->path[PATH_MAX - 1] = '\0';
            meta->id = index->num_files;
            meta->permissions = st.st_mode;
            meta->modified = st.st_mtime;

            if (add_file_to_index(index, full_path, index->num_files) == 0) {
                index->num_files++;
            }

            pthread_rwlock_unlock(&index->index_lock);
        }
    }

    closedir(dir);
    return 0;
}

int qfind_build_index(qfind_index_t *index, const char *root_path) {
    struct stat st;
    if (lstat(root_path, &st) != 0 || !S_ISDIR(st.st_mode)) return -errno;

    index->file_metadata = malloc(INITIAL_META_CAPACITY * sizeof(file_metadata_t));
    if (!index->file_metadata) return -ENOMEM;

    index->meta_capacity = INITIAL_META_CAPACITY;
    index->num_files = 0;

    int ret = process_directory(index, root_path, 0);
    if (ret == 0) {
        pthread_rwlock_wrlock(&index->index_lock);
        ret = compress_posting_lists(index);
        pthread_rwlock_unlock(&index->index_lock);
    }
    return ret;
}

static int insert_path_to_trie(trie_node_t *root, const char *path, file_id_t id) {
    trie_node_t *current = root;
    const char *p = path;

    while (*p) {
        if (*p == *(p+1)) {
            uint8_t count = 1;
            while (*p == *(p+count) && count < UINT8_MAX) count++;
            
            if (!current->children[TRIE_PATH_COMPRESS]) {
                current->children[TRIE_PATH_COMPRESS] = calloc(1, sizeof(trie_node_t));
                if (!current->children[TRIE_PATH_COMPRESS]) return -1;
                current->children[TRIE_PATH_COMPRESS]->key = TRIE_PATH_COMPRESS;
            }
            current = current->children[TRIE_PATH_COMPRESS];
            current->count = count;
            p += count;
        }
        else {
            unsigned char c = *p;
            if (!current->children[c]) {
                current->children[c] = calloc(1, sizeof(trie_node_t));
                if (!current->children[c]) return -1;
                current->children[c]->key = c;
            }
            current = current->children[c];
            p++;
        }
    }

    current->is_end = true;
    current->file_id = id;
    return 0;
}

int add_file_to_index(qfind_index_t *index, const char *path, file_id_t id) {
    if (insert_path_to_trie(index->trie_root, path, id) != 0) return -1;

    trigram_t trigrams[PATH_MAX];
    uint32_t trigram_count;
    extract_trigrams(path, trigrams, &trigram_count, PATH_MAX);

    pthread_rwlock_wrlock(&index->index_lock);
    for (uint32_t i = 0; i < trigram_count; i++) {
        ffbloom_add(index->bloom, &trigrams[i], sizeof(trigram_t));

        index_entry_t *entry = NULL;
        for (uint32_t j = 0; j < index->num_entries; j++) {
            if (index->entries[j].trigram == trigrams[i]) {
                entry = &index->entries[j];
                break;
            }
        }

        if (!entry) {
            index_entry_t *new_entries = realloc(index->entries, 
                (index->num_entries + 1) * sizeof(index_entry_t));
            if (!new_entries) {
                pthread_rwlock_unlock(&index->index_lock);
                return -1;
            }
            index->entries = new_entries;
            entry = &index->entries[index->num_entries++];
            *entry = (index_entry_t){
                .trigram = trigrams[i],
                .num_files = 0,
                .offset = 0,
                .size = 0
            };
        }
        entry->num_files++;
    }
    pthread_rwlock_unlock(&index->index_lock);
    return 0;
}

int compress_posting_lists(qfind_index_t *index) {
    size_t total_size = 0;
    for (uint32_t i = 0; i < index->num_entries; i++) {
        index->entries[i].offset = total_size;
        index->entries[i].size = ZSTD_compressBound(index->entries[i].num_files * sizeof(file_id_t));
        total_size += index->entries[i].size;
    }

    index->compressed_data = malloc(total_size);
    if (!index->compressed_data) return -1;
    index->compressed_size = total_size;

    for (uint32_t i = 0; i < index->num_entries; i++) {
        file_id_t *dummy_data = malloc(index->entries[i].num_files * sizeof(file_id_t));
        if (!dummy_data) return -1;
        memset(dummy_data, 0, index->entries[i].num_files * sizeof(file_id_t));

        size_t compressed_size = ZSTD_compress(
            (char*)index->compressed_data + index->entries[i].offset,
            index->entries[i].size,
            dummy_data,
            index->entries[i].num_files * sizeof(file_id_t),
            ZSTD_CLEVEL_DEFAULT
        );
        
        free(dummy_data);
        if (ZSTD_isError(compressed_size)) return -1;
        index->entries[i].size = compressed_size;
    }
    return 0;
}

static int compare_scores(const void *a, const void *b) {
    return ((scored_result_t*)b)->score - ((scored_result_t*)a)->score;
}

static void process_posting_list(qfind_index_t *index, index_entry_t *entry,
                               file_id_t **candidates, uint32_t *num_candidates) {
    uint8_t *compressed = (uint8_t*)index->compressed_data + entry->offset;
    uint8_t *decompressed = mmap(NULL, entry->num_files * sizeof(file_id_t),
                                PROT_READ, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
    if (decompressed == MAP_FAILED) return;
    
    size_t decomp_size = ZSTD_decompress(decompressed,
                                        entry->num_files * sizeof(file_id_t),
                                        compressed, entry->size);
    if (ZSTD_isError(decomp_size)) {
        munmap(decompressed, entry->num_files * sizeof(file_id_t));
        return;
    }

    for (size_t i = 0; i < decomp_size / sizeof(file_id_t); i++) {
        file_id_t id;
        memcpy(&id, decompressed + i * sizeof(file_id_t), sizeof(file_id_t));
        
        bool exists = false;
        for (uint32_t j = 0; j < *num_candidates; j++) {
            if ((*candidates)[j] == id) {
                exists = true;
                break;
            }
        }
        
        if (!exists && *num_candidates < MAX_CANDIDATES) {
            (*candidates)[(*num_candidates)++] = id;
        }
    }
    
    munmap(decompressed, entry->num_files * sizeof(file_id_t));
}

static float calculate_relevance_score(file_metadata_t *meta,
                                      trigram_t *query_trigrams,
                                      uint32_t trigram_count) {
    size_t path_len = strlen(meta->path);
    float score = 0.0f;
    
    for (uint32_t i = 0; i < trigram_count; i++) {
        uint32_t trigram_freq = 0;
        const char *pos = meta->path;
        
        while ((pos = strstr(pos, (char*)&query_trigrams[i])) != NULL) {
            trigram_freq++;
            pos += TRIGRAM_SIZE;
        }
        
        float tf = (float)trigram_freq / (path_len - TRIGRAM_SIZE + 1);
        float idf = logf((float)meta->id / (trigram_freq + 1));
        score += tf * idf;
    }
    
    return score / sqrtf(path_len);
}

int qfind_search(qfind_index_t *index, query_ctx_t *query) {
    if (!query || !query->query || query->max_results == 0) return -EINVAL;

    trigram_t query_trigrams[MAX_TRIGRAMS];
    uint32_t trigram_count = 0;
    extract_trigrams(query->query, query_trigrams, &trigram_count, MAX_TRIGRAMS);

    if (trigram_count == 0) {
        query->results = malloc(sizeof(file_id_t) * query->max_results);
        query->num_results = 0;
        search_trie(index->trie_root, query->query, 0,
                   query->results, &query->num_results, query->max_results);
        return query->num_results;
    }

    for (uint32_t i = 0; i < trigram_count; i++) {
        if (!ffbloom_check(index->bloom, &query_trigrams[i], sizeof(trigram_t))) {
            return 0;
        }
    }

    trigram_t candidates[MAX_CANDIDATES];
    uint32_t num_candidates = 0;
    ffbloom_get_candidates(index->bloom, query_trigrams, trigram_count,
                          candidates, &num_candidates);

    pthread_rwlock_rdlock(&index->index_lock);
    
    file_id_t *file_candidates = malloc(MAX_CANDIDATES * sizeof(file_id_t));
    uint32_t file_candidate_count = 0;
    scored_result_t *scored_results = malloc(MAX_CANDIDATES * sizeof(scored_result_t));
    uint32_t scored_count = 0;

    for (uint32_t i = 0; i < num_candidates; i++) {
        for (uint32_t j = 0; j < index->num_entries; j++) {
            if (index->entries[j].trigram == candidates[i]) {
                process_posting_list(index, &index->entries[j],
                                    &file_candidates, &file_candidate_count);
                break;
            }
        }
    }

    for (uint32_t i = 0; i < file_candidate_count; i++) {
        file_metadata_t *meta = &index->file_metadata[file_candidates[i]];
        
        if (check_file_permission(meta, query->user_id, query->group_id)) {
            float score = calculate_relevance_score(meta, query_trigrams, trigram_count);
            if (score >= SCORE_THRESHOLD) {
                scored_results[scored_count++] = (scored_result_t){
                    .file_id = file_candidates[i],
                    .score = score
                };
            }
        }
    }

    qsort(scored_results, scored_count, sizeof(scored_result_t), compare_scores);

    query->num_results = MIN(scored_count, query->max_results);
    query->results = malloc(query->num_results * sizeof(file_id_t));
    
    for (uint32_t i = 0; i < query->num_results; i++) {
        query->results[i] = scored_results[i].file_id;
    }

    pthread_rwlock_unlock(&index->index_lock);
    free(file_candidates);
    free(scored_results);
    
    return query->num_results;
}

static void search_trie(trie_node_t *node, const char *query, size_t pos,
                       file_id_t *results, uint32_t *count, uint32_t max_results) {
    if (!node || *count >= max_results) return;

    if (node->key == TRIE_PATH_COMPRESS) {
        size_t remaining = node->count;
        const char *q = query + pos;
        
        while (remaining > 0 && *q == *(q-1)) {
            remaining--;
            q++;
        }
        
        if (remaining == 0) {
            pos += node->count;
        } else {
            return;
        }
    } else if (node->key != 0) {
        if (pos >= strlen(query) || node->key != query[pos]) return;
        pos++;
    }

    if (node->is_end && pos == strlen(query)) {
        results[(*count)++] = node->file_id;
    }

    for (int i = 0; i < 256; i++) {
        if (node->children[i]) {
            search_trie(node->children[i], query, pos, results, count, max_results);
        }
    }
}
