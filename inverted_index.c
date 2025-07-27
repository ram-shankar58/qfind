#include "qfind.h"
#include <zstd.h>
#include <immintrin.h>
#include <sys/mman.h>
#include <errno.h>
#include <syslog.h>
#include <uthash.h>
#include <math.h>

#define GOLOMB_OPT_WINDOW 64
#define TRIE_GROW_SIZE 256
#define POSTING_CHUNK 4096
#define ALIGNMENT 64
#define MAX_LOAD_FACTOR 0.7
#define INITIAL_CAPACITY (1U << 20)
#define MAX(a, b) ((a) > (b) ? (a) : (b))

typedef struct {
    trigram_t trigram;
    uint32_t *deltas;
    size_t capacity;
    size_t count;
    size_t offset;
    size_t size;
    UT_hash_handle hh;
} trigram_entry;

typedef struct {
    trigram_entry *entries;
    size_t capacity;
    size_t count;
    pthread_rwlock_t lock;
    ZSTD_CCtx *zstd_cctx;
    ZSTD_DCtx *zstd_dctx;
} inverted_index;

static inverted_index idx = {0};


//UNCOMMENT THIS IF YOU HAVE HARDWARE SUPPORT FOR AVX-512
//Check it by running grep avx512 /proc/cpuinfo


// /* AVX-512 optimized histogram */ 
// static uint8_t calculate_golomb_param(const uint32_t *deltas, size_t count) {
//     if (count == 0) return 4;
    
//     __m512i sum = _mm512_setzero_si512();
//     const size_t simd_chunks = count / 16;
    
//     for (size_t i = 0; i < simd_chunks; i++) {
//         __m512i chunk = _mm512_loadu_si512(deltas + i*16);
//         sum = _mm512_add_epi32(sum, chunk);
//     }
    
//     uint32_t total = _mm512_reduce_add_epi32(sum);
//     uint32_t average = total / count;
//     return (uint8_t)(log2(MAX(1, average)) + 0.5);
// }
///* SIMD Golomb-Rice encoding */
// static size_t golomb_encode_avx512(const uint32_t *deltas, size_t count, 
//                                   uint8_t *output, uint8_t k) {
//     const __m512i k_mask = _mm512_set1_epi32((1 << k) - 1);
//     uint8_t *out = output;
    
//     for (size_t i = 0; i < count; i += 16) {
//         __m512i vals = _mm512_load_si512(deltas + i);
//         __m512i quot = _mm512_srli_epi32(vals, k);
//         __m512i rem = _mm512_and_si512(vals, k_mask);

//         while (!_mm512_test_epi32_mask(quot, quot)) {
//             __mmask16 mask = _mm512_cmpgt_epi32_mask(quot, _mm512_setzero_si512());
//             *out++ = (uint8_t)mask;
//             quot = _mm512_sub_epi32(quot, _mm512_set1_epi32(1));
//         }

//         _mm512_storeu_si512(out, rem);
//         out += 16 * sizeof(uint32_t);
//     }
    
//     return out - output;
// }

static uint8_t calculate_golomb_param(const uint32_t *deltas, size_t count) {
    if (count == 0) return 4;
    
    uint64_t total = 0;
    for (size_t i = 0; i < count; i++) {
        total += deltas[i];
    }
    uint32_t average = total / count;
    return (uint8_t)(log2(MAX(1, average)) + 0.5);
}

static size_t golomb_encode_scalar(const uint32_t *deltas, size_t count, 
                                  uint8_t *output, uint8_t k) {
    uint8_t *out = output;
    const uint32_t mask = (1 << k) - 1;
    
    for (size_t i = 0; i < count; i++) {
        uint32_t q = deltas[i] >> k;
        uint32_t r = deltas[i] & mask;
        
        while (q > 0) {
            *out++ = 0xFF;
            q--;
        }
        *out++ = (uint8_t)r;
    }
    
    return out - output;
}



/* Buffer management */
static trigram_entry* create_trigram_entry() {
    trigram_entry *entry = aligned_alloc(ALIGNMENT, sizeof(trigram_entry));
    if (!entry) return NULL;
    
    entry->deltas = aligned_alloc(ALIGNMENT, POSTING_CHUNK * sizeof(uint32_t));
    if (!entry->deltas) {
        free(entry);
        return NULL;
    }
    
    entry->capacity = POSTING_CHUNK;
    entry->count = 0;
    return entry;
}

static void free_trigram_entry(trigram_entry *entry) {
    if (entry) {
        free(entry->deltas);
        free(entry);
    }
}

/* Quadratic probing hash table */
static trigram_entry* find_trigram_entry(trigram_t trigram) {
    size_t index = trigram % idx.capacity;
    size_t probe = 1;
    
    while (true) {
        trigram_entry *entry = &idx.entries[index];
        if (entry->count == 0 || entry->trigram == trigram) {
            return entry;
        }
        index = (index + probe * probe) % idx.capacity;
        probe++;
    }
}

static int resize_trigram_table() {
    const size_t new_capacity = idx.capacity * 2;
    trigram_entry *new_entries = mmap(NULL, new_capacity * sizeof(trigram_entry),
                                     PROT_READ|PROT_WRITE, 
                                     MAP_PRIVATE|MAP_ANONYMOUS|MAP_HUGETLB, -1, 0);
    if (new_entries == MAP_FAILED) {
        new_entries = aligned_alloc(ALIGNMENT, new_capacity * sizeof(trigram_entry));
        if (!new_entries) return -1;
    }

    memset(new_entries, 0, new_capacity * sizeof(trigram_entry));
    
    for (size_t i = 0; i < idx.capacity; i++) {
        trigram_entry *old_entry = &idx.entries[i];
        if (old_entry->count == 0) continue;

        trigram_entry *new_entry = find_trigram_entry(old_entry->trigram);
        memcpy(new_entry, old_entry, sizeof(trigram_entry));
    }

    if (idx.entries != MAP_FAILED) 
        munmap(idx.entries, idx.capacity * sizeof(trigram_entry));
    
    idx.entries = new_entries;
    idx.capacity = new_capacity;
    return 0;
}

// 
int add_file_to_index(qfind_index_t *index, const char *path, file_id_t file_id) {
    trigram_t trigrams[PATH_MAX];
    size_t trigram_count = 0;
    extract_trigrams(path, trigrams, &trigram_count, PATH_MAX);
    
    trie_node_t *node = index->trie_root;
    const char *p = path;
    
    while (*p) {
        char current = *p;
        uint8_t run_length = 1;
        
        while (*(p+1) == current) {
            p++;
            run_length++;
        }

        if (run_length > 1) {
            if (!node->children[0xFF]) {
                node->children[0xFF] = calloc(1, sizeof(trie_node_t));
                if (!node->children[0xFF]) goto oom;
                node->children[0xFF]->key = 0xFF;
            }
            node = node->children[0xFF];
            node->count = run_length;
        } else {
            unsigned char c = *p;
            if (!node->children[c]) {
                node->children[c] = calloc(1, sizeof(trie_node_t));
                if (!node->children[c]) goto oom;
                node->children[c]->key = c;
            }
            node = node->children[c];
        }
        p++;
    }
    
    node->is_end = true;
    node->file_id = file_id;

    pthread_rwlock_wrlock(&idx.lock);
    
    if ((float)idx.count / idx.capacity > MAX_LOAD_FACTOR && resize_trigram_table() != 0) 
        goto oom_unlock;

    for (uint32_t i = 0; i < trigram_count; i++) {
        trigram_entry *entry = find_trigram_entry(trigrams[i]);
        
        if (entry->count == 0) {
            entry->trigram = trigrams[i];
            entry->count = 1;
            entry->deltas = aligned_alloc(ALIGNMENT, POSTING_CHUNK * sizeof(uint32_t));
            if (!entry->deltas) goto oom_unlock;
            entry->capacity = POSTING_CHUNK;
            idx.count++;
        } else {
            if (entry->count >= entry->capacity) {
                size_t new_cap = entry->capacity * 2;
                uint32_t *new_buf = aligned_alloc(ALIGNMENT, new_cap * sizeof(uint32_t));
                if (!new_buf) goto oom_unlock;
                memcpy(new_buf, entry->deltas, entry->count * sizeof(uint32_t));
                free(entry->deltas);
                entry->deltas = new_buf;
                entry->capacity = new_cap;
            }
            entry->deltas[entry->count++] = file_id;
        }
    }
    
    pthread_rwlock_unlock(&idx.lock);
    return 0;

oom_unlock:
    pthread_rwlock_unlock(&idx.lock);
oom:
    syslog(LOG_CRIT, "Out of memory in add_file_to_index");
    return -1;
}

int compress_posting_lists(qfind_index_t *index) {
    pthread_rwlock_wrlock(&idx.lock);
    
    size_t max_compressed = ZSTD_compressBound(idx.count * sizeof(uint32_t));
    void *compressed_buf = mmap(NULL, max_compressed, PROT_READ|PROT_WRITE,
                               MAP_PRIVATE|MAP_ANONYMOUS|MAP_HUGETLB, -1, 0);
    if (compressed_buf == MAP_FAILED) {
        compressed_buf = malloc(max_compressed);
        if (!compressed_buf) goto error;
    }

    size_t total_compressed = 0;
    for (size_t i = 0; i < idx.capacity; i++) {
        trigram_entry *entry = &idx.entries[i];
        if (entry->count == 0) continue;

        qsort(entry->deltas, entry->count, sizeof(uint32_t), 
             (int (*)(const void*, const void*))memcmp);
        
        uint32_t prev = 0;
        for (size_t j = 0; j < entry->count; j++) {
            uint32_t temp = entry->deltas[j];
            entry->deltas[j] -= prev;
            prev = temp;
        }

        uint8_t k = calculate_golomb_param(entry->deltas, entry->count);

        //if you ave avk, use golomb_encode_avk512 below
        size_t gr_size = golomb_encode_scalar(entry->deltas, entry->count,
                                             (uint8_t*)compressed_buf + total_compressed, k);

        size_t zstd_size = ZSTD_compressCCtx(idx.zstd_cctx,
                                            index->compressed_data + total_compressed,
                                            max_compressed - total_compressed,
                                            compressed_buf, gr_size,
                                            ZSTD_CLEVEL_DEFAULT);
        
        if (ZSTD_isError(zstd_size)) {
            syslog(LOG_ERR, "ZSTD error: %s", ZSTD_getErrorName(zstd_size));
            goto error;
        }

        entry->offset = total_compressed;
        entry->size = zstd_size;
        total_compressed += zstd_size;
    }

    index->compressed_size = total_compressed;
    munmap(compressed_buf, max_compressed);
    pthread_rwlock_unlock(&idx.lock);
    return 0;

error:
    if (compressed_buf != MAP_FAILED) munmap(compressed_buf, max_compressed);
    pthread_rwlock_unlock(&idx.lock);
    return -1;
}

int init_inverted_index() {
    idx.entries = mmap(NULL, INITIAL_CAPACITY * sizeof(trigram_entry),
                      PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS|MAP_HUGETLB, -1, 0);
    if (idx.entries == MAP_FAILED) {
        idx.entries = aligned_alloc(ALIGNMENT, INITIAL_CAPACITY * sizeof(trigram_entry));
        if (!idx.entries) return -1;
    }
    
    memset(idx.entries, 0, INITIAL_CAPACITY * sizeof(trigram_entry));
    idx.capacity = INITIAL_CAPACITY;
    pthread_rwlock_init(&idx.lock, NULL);
    
    idx.zstd_cctx = ZSTD_createCCtx();
    idx.zstd_dctx = ZSTD_createDCtx();
    if (!idx.zstd_cctx || !idx.zstd_dctx) return -1;
    
    return 0;
}

void cleanup_inverted_index() {
    pthread_rwlock_destroy(&idx.lock);
    for (size_t i = 0; i < idx.capacity; i++) {
        if (idx.entries[i].deltas) {
            free(idx.entries[i].deltas);
        }
    }
    munmap(idx.entries, idx.capacity * sizeof(trigram_entry));
    ZSTD_freeCCtx(idx.zstd_cctx);
    ZSTD_freeDCtx(idx.zstd_dctx);
}

void extract_trigrams(const char *text, trigram_t *out, size_t *out_count, size_t max_out) {
    // TODO: implement trigram extraction logic
    *out_count = 0;
}
