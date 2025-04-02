#include "qfind.h"
#include <xxhash.h>

ffbloom_t* ffbloom_create(size_t primary_size, size_t secondary_size) {
    ffbloom_t *bloom = malloc(sizeof(ffbloom_t));
    if (!bloom) return NULL;
    
    bloom->primary = calloc(1, primary_size);
    bloom->secondary = calloc(1, secondary_size);
    if (!bloom->primary || !bloom->secondary) {
        free(bloom->primary);
        free(bloom->secondary);
        free(bloom);
        return NULL;
    }
    
    bloom->primary_size = primary_size;
    bloom->secondary_size = secondary_size;
    bloom->num_hash_funcs = MAX_HASH_FUNCS;
    
    return bloom;
}

void ffbloom_destroy(ffbloom_t *bloom) {
    if (!bloom) return;
    free(bloom->primary);
    free(bloom->secondary);
    free(bloom);
}

// Hash function using XXH3 (very fast modern hash)
static uint64_t bloom_hash(const void *data, size_t len, uint64_t seed) {
    return XXH3_64bits_withSeed(data, len, seed);
}

void ffbloom_add(ffbloom_t *bloom, const void *data, size_t len) {
    for (uint8_t i = 0; i < bloom->num_hash_funcs; i++) {
        uint64_t hash = bloom_hash(data, len, i) % (bloom->primary_size * 8);
        
        // Set bit in primary filter
        bloom->primary[hash / 8] |= (1 << (hash % 8));
    }
}

bool ffbloom_check(ffbloom_t *bloom, const void *data, size_t len) {
    for (uint8_t i = 0; i < bloom->num_hash_funcs; i++) {
        uint64_t hash = bloom_hash(data, len, i) % (bloom->primary_size * 8);
        
        // Check if bit is set in primary filter
        if (!(bloom->primary[hash / 8] & (1 << (hash % 8)))) {
            return false;  // Definitely not in the set
        }
    }
    
    return true;  // Possibly in the set
}

void ffbloom_update_secondary(ffbloom_t *bloom, const void *data, size_t len) {
    // Update secondary filter to track query history
    for (uint8_t i = 0; i < bloom->num_hash_funcs; i++) {
        uint64_t hash = bloom_hash(data, len, i) % (bloom->secondary_size * 8);
        bloom->secondary[hash / 8] |= (1 << (hash % 8));
    }
}
