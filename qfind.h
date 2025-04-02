#ifndef QFIND_H
#define QFIND_H

#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <liburing.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <linux/limits.h>
#include <sys/inotify.h>
#include <sys/types.h>  
#include <sys/wait.h> //for idtype_t
#include <time.h>



#define BLOOM_SIZE (1 << 25)         // 32MB primary bloom filter
#define BLOOM_SEC_SIZE (1 << 24)     // 16MB secondary bloom filter
#define MAX_HASH_FUNCS 8             // Number of hash functions for Bloom filter
#define TRIGRAM_SIZE 3               // Size of n-grams in bytes
#define BATCH_SIZE 128               // I/O batch size
#define WORKER_THREADS 16            // Number of parallel worker threads
#define INDEX_BLOCK_SIZE (1 << 16)   // 64KB blocks for inverted index
#define MAX_RESULTS 10000            // Maximum results to return
#define IO_RINGSIZE 1024             // Size of io_uring queue

typedef uint64_t file_id_t;
typedef uint32_t trigram_t;
typedef struct ffbloom_s *ffbloom_t; //adding to fix compilation errors in bottom up way

/* Compressed Inverted Index Entry */
typedef struct {
    trigram_t trigram;               // The 3-byte sequence
    uint32_t num_files;              // Number of files containing this trigram
    uint32_t offset;                 // Offset to compressed posting list
    uint32_t size;                   // Size of compressed posting list
} index_entry_t;

/* Posting List Entry */
typedef struct {
    file_id_t file_id;               // File identifier
    uint16_t positions[];            // Variable-length array of positions
} posting_t;

/* Suffix Trie Node for short queries */
typedef struct trie_node {
    char key;                        // Character at this node
    bool is_end;                     // Is this the end of a file path?
    file_id_t file_id;               // File ID if is_end is true
    struct trie_node *children[256]; // Children nodes (one per possible byte)
    uint16_t num_children;           // Number of children
} trie_node_t;

/* File Metadata */
typedef struct {
    file_id_t id;                    // Unique file identifier
    char path[PATH_MAX];             // Absolute file path
    uint32_t permissions;            // File permissions
    time_t modified;                 // Last modified timestamp
} file_metadata_t;

/* io_uring Context */
typedef struct {
    struct io_uring ring;            // io_uring instance
    void **registered_buffers;       // Pre-registered buffers
    int num_buffers;                 // Number of registered buffers
    bool use_sqpoll;                 // Whether to use SQPOLL feature
} io_context_t;

/* Main Index Structure */
typedef struct {
    ffbloom_t bloom;                 // Feed-forward Bloom filter
    index_entry_t *entries;          // Array of index entries
    uint32_t num_entries;            // Number of index entries
    void *compressed_data;           // Compressed posting lists
    size_t compressed_size;          // Size of compressed data in bytes
    trie_node_t *trie_root;          // Root of the suffix trie
    file_metadata_t *file_metadata;  // Array of file metadata
    uint32_t num_files;              // Number of files in the index
    io_context_t io;                 // I/O context for async operations
    pthread_rwlock_t index_lock;     // Read-write lock for index access
} qfind_index_t;

/* Query Context */
typedef struct {
    char *query;                     // Search query string
    bool case_sensitive;             // Whether search is case sensitive
    bool regex_enabled;              // Whether regex matching is enabled
    file_id_t *results;              // Result buffer
    uint32_t num_results;            // Number of results found
    uint32_t max_results;            // Maximum results to return
    uid_t user_id;                   // User ID for permission filtering
    gid_t group_id;                  // Group ID for permission filtering
} query_ctx_t;


/* Function Prototypes */

/* Initialization and cleanup */
qfind_index_t* qfind_init(void);
void qfind_destroy(qfind_index_t *index);

/* Index operations */
int qfind_build_index(qfind_index_t *index, const char *root_path);
int qfind_update_index(qfind_index_t *index, const char *path, bool is_add);
int qfind_commit_updates(qfind_index_t *index);
int add_file_to_index(qfind_index_t *index, const char *path, file_id_t file_id);
int compress_posting_lists(qfind_index_t *index);

/* Search operations */
int qfind_search(qfind_index_t *index, query_ctx_t *query);
int qfind_get_results(query_ctx_t *query, file_metadata_t *results, uint32_t *num_results);

/* Bloom filter operations */
ffbloom_t ffbloom_create(size_t primary_size, size_t secondary_size);
void ffbloom_destroy(ffbloom_t bloom);
void ffbloom_add(ffbloom_t bloom, const void *data, size_t len);
bool ffbloom_check(ffbloom_t bloom, const void *data, size_t len);
void ffbloom_update_secondary(ffbloom_t bloom, const void *data, size_t len);

/* Trigram operations */
void extract_trigrams(const char *str, trigram_t *trigrams, uint32_t *count);
uint32_t hash_trigram(trigram_t trigram, uint8_t func_idx);

/* I/O operations */
int io_context_init(io_context_t *ctx, int queue_size, bool use_sqpoll);
int io_context_destroy(io_context_t *ctx);
int io_submit_read(io_context_t *ctx, int fd, void *buf, size_t len, off_t offset);
int io_wait_completions(io_context_t *ctx, int min_completions);

/* Utility functions */
void tokenize_path(const char *path, char **tokens, uint32_t *count);
bool check_file_permission(file_metadata_t *meta, uid_t user_id, gid_t group_id);

#endif /* QFIND_H */
