#include "qfind.h"
#include <dirent.h>
#include <sys/stat.h>
#include <errno.h>
#include <stdlib.h>

qfind_index_t* qfind_init(void) {
    qfind_index_t *index = calloc(1, sizeof(qfind_index_t));
    index->bloom = ffbloom_create(BLOOM_SIZE, BLOOM_SEC_SIZE);
    return index;
}

void qfind_destroy(qfind_index_t *index) {
    if (!index) return;
    ffbloom_destroy(index->bloom);
    free(index->entries);
    free(index->compressed_data);
    free(index);
}

static int traverse_directory(qfind_index_t *index, const char *path) {
    DIR *dir = opendir(path);
    if (!dir) {
        fprintf(stderr, "Error opening %s: %s\n", path, strerror(errno));
        return -1;
    }

    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        /* Skip . and .. entries */
        if (strcmp(entry->d_name, ".") == 0 || 
            strcmp(entry->d_name, "..") == 0) continue;

        /* Construct full path */
        char full_path[PATH_MAX];
        int path_len = snprintf(full_path, sizeof(full_path), 
                               "%s/%s", path, entry->d_name);
        if (path_len >= (int)sizeof(full_path)) {
            fprintf(stderr, "Path too long: %s/%s\n", path, entry->d_name);
            continue;
        }

        /* Get file metadata */
        struct stat st;
        if (lstat(full_path, &st) == -1) {
            fprintf(stderr, "Error stating %s: %s\n", 
                   full_path, strerror(errno));
            continue;
        }

        if (S_ISDIR(st.st_mode)) {
            /* Recursively process directory */
            traverse_directory(index, full_path);
        }
        else if (S_ISREG(st.st_mode)) {  /* Regular file */
            /* Expand metadata array if needed */
            if (index->num_files > 0 && 
               (index->num_files % 1000) == 0) {
                file_metadata_t *new_meta = realloc(index->file_metadata,
                    (index->num_files + 1000) * sizeof(file_metadata_t));
                if (!new_meta) {
                    fprintf(stderr, "Memory allocation failed\n");
                    closedir(dir);
                    return -1;
                }
                index->file_metadata = new_meta;
            }

            /* Add file metadata */
            file_id_t id = index->num_files++;
            file_metadata_t *meta = &index->file_metadata[id];
            
            strncpy(meta->path, full_path, PATH_MAX - 1);
            meta->path[PATH_MAX - 1] = '\0';
            meta->id = id;
            meta->permissions = st.st_mode;
            meta->modified = st.st_mtime;

            /* Add to index structures */
            add_file_to_index(index, full_path, id);
        }
    }

    closedir(dir);
    return 0;
}

int qfind_build_index(qfind_index_t *index, const char *root_path) {
    /* Initialize metadata array */
    index->file_metadata = malloc(1000 * sizeof(file_metadata_t));
    if (!index->file_metadata) {
        fprintf(stderr, "Initial metadata allocation failed\n");
        return -1;
    }
    index->num_files = 0;

    /* Start recursive traversal */
    int result = traverse_directory(index, root_path);
    
    /* Final compression after building index */
    if (result == 0) {
        compress_posting_lists(index);
    }
    
    return result;
}
