#include "qfind.h"
#include <zstd.h>
#include <immintrin.h> // For AVX-512 intrinsics

// Golomb-Rice parameter optimization for better compression
#define GOLOMB_PARAM 4

// Extract trigrams from a string
void extract_trigrams(const char *str, trigram_t *trigrams, uint32_t *count) {
    size_t len = strlen(str);
    *count = 0;
    
    if (len < TRIGRAM_SIZE) return;
    
    for (size_t i = 0; i <= len - TRIGRAM_SIZE; i++) {
        trigram_t t = 0;
        memcpy(&t, str + i, TRIGRAM_SIZE);
        trigrams[(*count)++] = t;
    }
}

// Golomb-Rice encoding for delta values in posting lists
static uint8_t* golomb_rice_encode(uint32_t *values, uint32_t count, uint8_t *buffer, size_t *size) {
    // Sort values for delta encoding
    qsort(values, count, sizeof(uint32_t), (int (*)(const void *, const void *))memcmp);
    
    uint8_t *current = buffer;
    uint32_t prev = 0;
    
    for (uint32_t i = 0; i < count; i++) {
        uint32_t delta = values[i] - prev;
        prev = values[i];
        
        // Write quotient in unary
        uint32_t quotient = delta >> GOLOMB_PARAM;
        memset(current, 0xFF, quotient / 8);
        current += quotient / 8;
        
        // Write remaining bits of quotient
        if (quotient % 8 > 0) {
            *current = (0xFF >> (8 - (quotient % 8)));
            current++;
        }
        
        // Write remainder in binary
        uint32_t remainder = delta & ((1 << GOLOMB_PARAM) - 1);
        *current = remainder;
        current++;
    }
    
    *size = current - buffer;
    return buffer;
}

// Golomb-Rice decoding for delta values in posting lists
static uint32_t* golomb_rice_decode(uint8_t *buffer, size_t size, uint32_t *values, uint32_t *count) {
    uint8_t *current = buffer;
    uint8_t *end = buffer + size;
    uint32_t idx = 0;
    uint32_t prev = 0;
    
    while (current < end && idx < *count) {
        // Read quotient in unary
        uint32_t quotient = 0;
        while (*current == 0xFF && current < end) {
            quotient += 8;
            current++;
        }
        
        if (current < end) {
            uint8_t bits = *current;
            uint8_t additional = 0;
            while (bits & 0x80) {
                additional++;
                bits <<= 1;
            }
            quotient += additional;
            current++;
        }
        
        // Read remainder in binary
        uint32_t remainder = 0;
        if (current < end) {
            remainder = *current & ((1 << GOLOMB_PARAM) - 1);
            current++;
        }
        
        // Combine quotient and remainder, add to previous value
        uint32_t delta = (quotient << GOLOMB_PARAM) | remainder;
        prev += delta;
        values[idx++] = prev;
    }
    
    *count = idx;
    return values;
}

// Add a file to the index
int add_file_to_index(qfind_index_t *index, const char *path, file_id_t file_id) {
    // Extract trigrams from the path
    trigram_t trigrams[PATH_MAX];
    uint32_t trigram_count = 0;
    extract_trigrams(path, trigrams, &trigram_count);
    
    // Update bloom filter with all trigrams
    for (uint32_t i = 0; i < trigram_count; i++) {
        ffbloom_add(&index->bloom, &trigrams[i], sizeof(trigram_t));
    }
    
    // Add file path to the trie for short query support
    trie_node_t *node = index->trie_root;
    for (const char *p = path; *p; p++) {
        unsigned char c = (unsigned char)*p;
        if (!node->children[c]) {
            node->children[c] = calloc(1, sizeof(trie_node_t));
            node->children[c]->key = c;
            node->num_children++;
        }
        node = node->children[c];
    }
    node->is_end = true;
    node->file_id = file_id;
    
    // Update inverted index entries for each trigram
    // This is a simplified version - actual implementation would use
    // compressed posting lists with position information
    for (uint32_t i = 0; i < trigram_count; i++) {
        // Find or create index entry for this trigram
        index_entry_t *entry = NULL;
        for (uint32_t j = 0; j < index->num_entries; j++) {
            if (index->entries[j].trigram == trigrams[i]) {
                entry = &index->entries[j];
                break;
            }
        }
        
        if (!entry) {
            // New trigram - add to index
            index->entries = realloc(index->entries, 
                                     (index->num_entries + 1) * sizeof(index_entry_t));
            entry = &index->entries[index->num_entries++];
            entry->trigram = trigrams[i];
            entry->num_files = 0;
            entry->offset = 0;  // Will be set during compression
            entry->size = 0;    // Will be set during compression
        }
        
        // For simplicity, we're not handling the actual posting list compression here
        // In a real implementation, we would add the file_id and positions to the posting list
        entry->num_files++;
    }
    
    return 0;
}

// Compress all posting lists
int compress_posting_lists(qfind_index_t *index) {
    // This is a simplified placeholder for the actual compression process
    // In a real implementation, we would:
    // 1. Allocate buffer for compressed data
    // 2. For each trigram, encode its posting list using Golomb-Rice
    // 3. Use ZSTD to further compress the encoded data
    // 4. Update the offset and size fields in each index entry
    
    size_t total_size = 0;
    
    // First pass to calculate total size needed
    for (uint32_t i = 0; i < index->num_entries; i++) {
        // Assume average of 8 bytes per file entry after compression
        total_size += index->entries[i].num_files * 8;
    }
    
    // Allocate buffer with 20% extra for safety
    total_size = total_size * 1.2;
    index->compressed_data = malloc(total_size);
    if (!index->compressed_data) return -1;
    
    uint8_t *current = index->compressed_data;
    
    // Second pass to actually compress data
    for (uint32_t i = 0; i < index->num_entries; i++) {
        index_entry_t *entry = &index->entries[i];
        entry->offset = current - (uint8_t*)index->compressed_data;
        
        // Here we would actually compress the posting list
        // For simplicity, we're just advancing the pointer
        size_t entry_size = entry->num_files * 8;
        current += entry_size;
        entry->size = entry_size;
    }
    
    index->compressed_size = current - (uint8_t*)index->compressed_data;
    return 0;
}
