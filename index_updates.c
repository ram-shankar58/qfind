#include "qfind.h"
#include <sys/inotify.h>
#include <pthread.h>
#include <limits.h>

#define EVENT_BUF_LEN (10 * (sizeof(struct inotify_event) + NAME_MAX + 1))

// LSM tree node for storing deltas
typedef struct lsm_node {
    file_id_t id;
    char path[PATH_MAX];
    bool is_add;  // true = add, false = delete
    struct lsm_node *next;
} lsm_node_t;

// Global variables for the update thread
static int inotify_fd = -1;
static pthread_t update_thread;
static bool update_thread_running = false;
static lsm_node_t *lsm_head = NULL;
static pthread_mutex_t lsm_mutex = PTHREAD_MUTEX_INITIALIZER;
static qfind_index_t *g_index = NULL;

// Add a change to the LSM tree
static int lsm_add_change(file_id_t id, const char *path, bool is_add) {
    lsm_node_t *node = malloc(sizeof(lsm_node_t));
    if (!node) return -1;
    
    node->id = id;
    strncpy(node->path, path, PATH_MAX - 1);
    node->path[PATH_MAX - 1] = '\0';
    node->is_add = is_add;
    node->next = NULL;
    
    pthread_mutex_lock(&lsm_mutex);
    
    // Check if we already have a change for this file
    lsm_node_t *current = lsm_head;
    lsm_node_t *prev = NULL;
    
    while (current) {
        if (current->id == id) {
            // Update existing node
            current->is_add = is_add;
            free(node);
            pthread_mutex_unlock(&lsm_mutex);
            return 0;
        }
        prev = current;
        current = current->next;
    }
    
    // Add new node
    if (!prev) {
        lsm_head = node;
    } else {
        prev->next = node;
    }
    
    pthread_mutex_unlock(&lsm_mutex);
    return 0;
}

// Process all pending changes in the LSM tree
static int lsm_process_changes(qfind_index_t *index) {
    pthread_mutex_lock(&lsm_mutex);
    
    lsm_node_t *current = lsm_head;
    lsm_head = NULL;  // Reset head before processing
    
    pthread_mutex_unlock(&lsm_mutex);
    
    while (current) {
        if (current->is_add) {
            // Add file to index
            add_file_to_index(index, current->path, current->id);
        } else {
            // Remove file from index
            // This would be implemented in a real system
        }
        
        lsm_node_t *next = current->next;
        free(current);
        current = next;
    }
    
    // After processing all changes, update the compressed posting lists
    compress_posting_lists(index);
    
    return 0;
}

// Watch a directory for changes
static int add_watch(int fd, const char *path) {
    int wd = inotify_add_watch(fd, path, 
                              IN_CREATE | IN_DELETE | IN_MOVE | IN_MODIFY);
    if (wd < 0) {
        perror("inotify_add_watch");
    }
    return wd;
}

// Update thread function
static void* update_thread_func(void *arg) {
    qfind_index_t *index = (qfind_index_t*)arg;
    char buffer[EVENT_BUF_LEN];
    
    while (update_thread_running) {
        // Read events
        ssize_t len = read(inotify_fd, buffer, EVENT_BUF_LEN);
        if (len < 0 && errno != EAGAIN) {
            perror("read");
            break;
        }
        
        if (len <= 0) {
            // No events, sleep a bit
            usleep(100000);  // 100ms
            continue;
        }
        
        // Process events
        char *ptr = buffer;
        while (ptr < buffer + len) {
            struct inotify_event *event = (struct inotify_event*)ptr;
            
            if (event->len) {
                char path[PATH_MAX];
                
                // Construct full path (this is simplified)
                snprintf(path, PATH_MAX, "/some/path/%s", event->name);
                
                if (event->mask & (IN_CREATE | IN_MOVED_TO)) {
                    // File created or moved in
                    file_id_t id = index->num_files++;
                    lsm_add_change(id, path, true);
                }
                else if (event->mask & (IN_DELETE | IN_MOVED_FROM)) {
                    // File deleted or moved out
                    // Find file ID by path
                    file_id_t id = 0;  // This would be a lookup in a real system
                    lsm_add_change(id, path, false);
                }
            }
            
            ptr += sizeof(struct inotify_event) + event->len;
        }
        
        // Periodically process changes
        static time_t last_process = 0;
        time_t now = time(NULL);
        
        if (now - last_process >=.30) {  // Every 30 seconds
            lsm_process_changes(index);
            last_process = now;
        }
    }
    
    pthread_exit(NULL);
}

// Initialize real-time updates
int init_realtime_updates(qfind_index_t *index) {
    // Initialize inotify
    inotify_fd = inotify_init1(IN_NONBLOCK);
    if (inotify_fd < 0) {
        perror("inotify_init1");
        return -1;
    }
    
    // Add watches for key directories
    // In a real system, we would add watches recursively for all directories
    add_watch(inotify_fd, "/");
    add_watch(inotify_fd, "/home");
    add_watch(inotify_fd, "/usr");
    
    // Save index pointer
    g_index = index;
    
    // Start update thread
    update_thread_running = true;
    if (pthread_create(&update_thread, NULL, update_thread_func, index) != 0) {
        perror("pthread_create");
        close(inotify_fd);
        update_thread_running = false;
        return -1;
    }
    
    return 0;
}

// Stop real-time updates
int stop_realtime_updates() {
    update_thread_running = false;
    pthread_join(update_thread, NULL);
    close(inotify_fd);
    
    // Process any remaining changes
    if (g_index) {
        lsm_process_changes(g_index);
    }
    
    return 0;
}

// Update index for a specific path (add or remove)
int qfind_update_index(qfind_index_t *index, const char *path, bool is_add) {
    file_id_t id;
    
    if (is_add) {
        // Add new file
        id = index->num_files++;
        
        // Allocate or expand file metadata array if needed
        if (index->num_files > 0 && (index->num_files % 1000) == 0) {
            index->file_metadata = realloc(index->file_metadata, 
                                         (index->num_files + 1000) * sizeof(file_metadata_t));
            if (!index->file_metadata) return -1;
        }
        
        // Set file metadata
        file_metadata_t *meta = &index->file_metadata[id];
        strncpy(meta->path, path, PATH_MAX - 1);
        meta->path[PATH_MAX - 1] = '\0';
        meta->id = id;
        
        // Get file permissions and timestamp
        struct stat st;
        if (stat(path, &st) == 0) {
            meta->permissions = st.st_mode;
            meta->modified = st.st_mtime;
        }
        
        // Add to index
        return add_file_to_index(index, path, id);
    } else {
        // Find file ID by path
        for (uint32_t i = 0; i < index->num_files; i++) {
            if (strcmp(index->file_metadata[i].path, path) == 0) {
                id = i;
                
                // In a real implementation, we would remove this file from the index
                // This would involve updating the bloom filter, removing entries from
                // the inverted index, and updating the trie.
                
                // Mark as deleted - a real implementation would do more
                index->file_metadata[id].path[0] = '\0';
                return 0;
            }
        }
        
        // File not found
        return -1;
    }
}

// Commit all pending updates
int qfind_commit_updates(qfind_index_t *index) {
    // For real-time updates, trigger a manual update
    return lsm_process_changes(index);
}
