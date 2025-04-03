#include "qfind.h"
#include <sys/inotify.h>
#include <pthread.h>
#include <limits.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <fnmatch.h>
#include <uthash.h>
#include <dirent.h>
#include <stdatomic.h>
#include <syslog.h>

#define EVENT_BUF_LEN (65536)
#define MAX_WATCHES 1024
#define LSM_BATCH_SIZE 5000
#define PATH_CACHE_SIZE 100000

typedef struct watch_mapping {
    int wd;
    char path[PATH_MAX];
    UT_hash_handle hh;
} watch_mapping_t;

typedef struct path_cache_entry {
    char path[PATH_MAX];
    file_id_t id;
    UT_hash_handle hh;
} path_cache_entry_t;

// redundant
//typedef struct lsm_node {
//     file_id_t id;
//     char *path;
//     bool is_add;
//     struct lsm_node *next;
// } lsm_node_t;

// typedef struct lsm_batch {
//     lsm_node_t *head;
//     lsm_node_t *tail;
//     size_t count;
//     pthread_spinlock_t lock;
// } lsm_batch_t;

static struct {
    int inotify_fd;
    pthread_t update_thread;
    atomic_bool running;
    pthread_rwlock_t watch_lock;
    watch_mapping_t *watches;
    path_cache_entry_t *path_cache;
    lsm_batch_t pending_adds;
    lsm_batch_t pending_dels;
    pthread_spinlock_t cache_lock;
    qfind_index_t *index;
} realtime_ctx;

static void process_inotify_events(qfind_index_t *index);
static void handle_file_event(qfind_index_t *index, const struct inotify_event *event, const char *base_path);
static file_id_t path_to_id(const char *path);
static void cache_path(const char *path, file_id_t id);
static void uncache_path(const char *path);

static void* update_thread_func(void *arg) {
    qfind_index_t *index = (qfind_index_t*)arg;
    struct pollfd pfd = { .fd = realtime_ctx.inotify_fd, .events = POLLIN };
    
    while (atomic_load(&realtime_ctx.running)) {
        int ready = poll(&pfd, 1, 30000);
        if (ready > 0) {
            process_inotify_events(index);
        }
        else if (ready < 0 && errno != EINTR) {
            syslog(LOG_ERR, "inotify poll error: %s", strerror(errno));
            break;
        }
        
        if (realtime_ctx.pending_adds.count >= LSM_BATCH_SIZE ||
            realtime_ctx.pending_dels.count >= LSM_BATCH_SIZE) {
            qfind_commit_updates(index);
        }
    }
    return NULL;
}

static void process_inotify_events(qfind_index_t *index) {
    char buffer[EVENT_BUF_LEN] __attribute__ ((aligned(__alignof__(struct inotify_event))));
    ssize_t len;
    
    while ((len = read(realtime_ctx.inotify_fd, buffer, sizeof(buffer))) > 0) {
        for (char *ptr = buffer; ptr < buffer + len; ) {
            struct inotify_event *event = (struct inotify_event*)ptr;
            pthread_rwlock_rdlock(&realtime_ctx.watch_lock);
            watch_mapping_t *wm;
            HASH_FIND_INT(realtime_ctx.watches, &event->wd, wm);
            pthread_rwlock_unlock(&realtime_ctx.watch_lock);
            
            if (wm) {
                char full_path[PATH_MAX];
                if (snprintf(full_path, sizeof(full_path), "%s/%s", wm->path, event->name) >= (int)sizeof(full_path)) {
                    syslog(LOG_WARNING, "Path too long: %s/%s", wm->path, event->name);
                    continue;
                }
                handle_file_event(index, event, full_path);
            }
            
            ptr += sizeof(struct inotify_event) + event->len;
        }
    }
}

static void handle_file_event(qfind_index_t *index, const struct inotify_event *event, const char *path) {
    struct stat st;
    if (lstat(path, &st) == -1) {
        syslog(LOG_ERR, "Failed to stat %s: %s", path, strerror(errno));
        return;
    }

    if (fnmatch(".*", event->name, FNM_PERIOD) == 0) return;

    if (event->mask & (IN_CREATE|IN_MOVED_TO|IN_MODIFY)) {
        if (S_ISREG(st.st_mode)) {
            file_id_t id = atomic_fetch_add(&index->num_files, 1);
            
            // Expand metadata array if needed
            if (index->num_files % 1000 == 0) {
                file_metadata_t *new_meta = realloc(index->file_metadata, 
                    (index->num_files + 1000) * sizeof(file_metadata_t));
                if (!new_meta) {
                    syslog(LOG_CRIT, "Memory allocation failed for file metadata");
                    return;
                }
                index->file_metadata = new_meta;
            }

            // Initialize metadata
            file_metadata_t *meta = &index->file_metadata[id];
            strncpy(meta->path, path, PATH_MAX-1);
            meta->path[PATH_MAX-1] = '\0';
            meta->id = id;
            meta->permissions = st.st_mode;
            meta->modified = st.st_mtime;

            lsm_node_t *node = malloc(sizeof(lsm_node_t));
            if (!node) {
                syslog(LOG_CRIT, "Failed to allocate LSM node");
                return;
            }
            node->path = strdup(path);
            node->is_add = true;
            node->id = id;
            node->next = NULL;

            pthread_spin_lock(&realtime_ctx.cache_lock);
            cache_path(path, id);
            pthread_spin_unlock(&realtime_ctx.cache_lock);

            pthread_spin_lock(&realtime_ctx.pending_adds.lock);
            if (!realtime_ctx.pending_adds.head) {
                realtime_ctx.pending_adds.head = node;
            } else {
                realtime_ctx.pending_adds.tail->next = node;
            }
            realtime_ctx.pending_adds.tail = node;
            realtime_ctx.pending_adds.count++;
            pthread_spin_unlock(&realtime_ctx.pending_adds.lock);
        }
        else if (S_ISDIR(st.st_mode)) {
            add_watch_recursive(path);
        }
    }
    else if (event->mask & (IN_DELETE|IN_MOVED_FROM|IN_DELETE_SELF)) {
        pthread_spin_lock(&realtime_ctx.cache_lock);
        file_id_t id = path_to_id(path);
        if (id != INVALID_FILE_ID) {
            lsm_node_t *node = malloc(sizeof(lsm_node_t));
            if (!node) {
                syslog(LOG_CRIT, "Failed to allocate LSM node");
                pthread_spin_unlock(&realtime_ctx.cache_lock);
                return;
            }
            node->path = strdup(path);
            node->is_add = false;
            node->id = id;
            node->next = NULL;

            pthread_spin_lock(&realtime_ctx.pending_dels.lock);
            if (!realtime_ctx.pending_dels.head) {
                realtime_ctx.pending_dels.head = node;
            } else {
                realtime_ctx.pending_dels.tail->next = node;
            }
            realtime_ctx.pending_dels.tail = node;
            realtime_ctx.pending_dels.count++;
            pthread_spin_unlock(&realtime_ctx.pending_dels.lock);

            uncache_path(path);
        }
        pthread_spin_unlock(&realtime_ctx.cache_lock);
    }
}

int init_realtime_updates(qfind_index_t *index) {
    realtime_ctx.inotify_fd = inotify_init1(IN_NONBLOCK|IN_CLOEXEC);
    if (realtime_ctx.inotify_fd < 0) {
        syslog(LOG_ERR, "inotify_init failed: %s", strerror(errno));
        return -1;
    }

    pthread_rwlock_init(&realtime_ctx.watch_lock, NULL);
    pthread_spin_init(&realtime_ctx.cache_lock, PTHREAD_PROCESS_PRIVATE);
    pthread_spin_init(&realtime_ctx.pending_adds.lock, PTHREAD_PROCESS_PRIVATE);
    pthread_spin_init(&realtime_ctx.pending_dels.lock, PTHREAD_PROCESS_PRIVATE);
    atomic_store(&realtime_ctx.running, true);
    realtime_ctx.index = index;

    const char *watch_paths[] = { "/" };
    for (size_t i = 0; i < sizeof(watch_paths)/sizeof(watch_paths[0]); i++) {
        if (add_watch_recursive(watch_paths[i]) < 0) {
            syslog(LOG_ERR, "Failed to initialize watch points");
            stop_realtime_updates();
            return -1;
        }
    }

    if (pthread_create(&realtime_ctx.update_thread, NULL, update_thread_func, index)) {
        syslog(LOG_ERR, "Failed to start update thread: %s", strerror(errno));
        close(realtime_ctx.inotify_fd);
        return -1;
    }

    return 0;
}

int add_watch_recursive(const char *path) {
    char resolved_path[PATH_MAX];
    if (!realpath(path, resolved_path)) {
        syslog(LOG_ERR, "Invalid path %s: %s", path, strerror(errno));
        return -1;
    }

    int wd = inotify_add_watch(realtime_ctx.inotify_fd, resolved_path,
                              IN_CREATE|IN_DELETE|IN_MOVED_FROM|IN_MOVED_TO|IN_MODIFY|IN_ONLYDIR);
    if (wd < 0) {
        syslog(LOG_ERR, "Failed to watch %s: %s", resolved_path, strerror(errno));
        return -1;
    }

    watch_mapping_t *wm = malloc(sizeof(watch_mapping_t));
    if (!wm) {
        syslog(LOG_CRIT, "Memory allocation failed for watch mapping");
        return -1;
    }
    wm->wd = wd;
    strncpy(wm->path, resolved_path, PATH_MAX-1);
    wm->path[PATH_MAX-1] = '\0';

    pthread_rwlock_wrlock(&realtime_ctx.watch_lock);
    HASH_ADD_INT(realtime_ctx.watches, wd, wm);
    pthread_rwlock_unlock(&realtime_ctx.watch_lock);

    DIR *dir = opendir(resolved_path);
    if (dir) {
        struct dirent *ent;
        while ((ent = readdir(dir))) {
            if (ent->d_type == DT_DIR && strcmp(ent->d_name, ".") && strcmp(ent->d_name, "..")) {
                char subpath[PATH_MAX];
                snprintf(subpath, PATH_MAX, "%s/%s", resolved_path, ent->d_name);
                add_watch_recursive(subpath);
            }
        }
        closedir(dir);
    }

    return wd;
}

int qfind_commit_updates(qfind_index_t *index) {
    lsm_batch_t adds = {0}, dels = {0};

    pthread_spin_lock(&realtime_ctx.pending_adds.lock);
    adds = realtime_ctx.pending_adds;
    realtime_ctx.pending_adds = (lsm_batch_t){0};
    pthread_spin_unlock(&realtime_ctx.pending_adds.lock);

    pthread_spin_lock(&realtime_ctx.pending_dels.lock);
    dels = realtime_ctx.pending_dels;
    realtime_ctx.pending_dels = (lsm_batch_t){0};
    pthread_spin_unlock(&realtime_ctx.pending_dels.lock);

    // Process additions
    for (lsm_node_t *node = adds.head; node; node = node->next) {
        add_file_to_index(index, node->path, node->id);
        free(node->path);
        free(node);
    }

    // Process deletions
    for (lsm_node_t *node = dels.head; node; node = node->next) {
        for (uint32_t i = 0; i < index->num_files; i++) {
            if (strcmp(index->file_metadata[i].path, node->path) == 0) {
                index->file_metadata[i].path[0] = '\0';
                break;
            }
        }
        free(node->path);
        free(node);
    }

    compress_posting_lists(index);
    return 0;
}

int stop_realtime_updates() {
    atomic_store(&realtime_ctx.running, false);
    pthread_join(realtime_ctx.update_thread, NULL);

    // Cleanup watches
    watch_mapping_t *wm, *tmp;
    pthread_rwlock_wrlock(&realtime_ctx.watch_lock);
    HASH_ITER(hh, realtime_ctx.watches, wm, tmp) {
        HASH_DEL(realtime_ctx.watches, wm);
        free(wm);
    }
    pthread_rwlock_unlock(&realtime_ctx.watch_lock);

    close(realtime_ctx.inotify_fd);
    qfind_commit_updates(realtime_ctx.index);
    return 0;
}

static file_id_t path_to_id(const char *path) {
    path_cache_entry_t *entry;
    HASH_FIND_STR(realtime_ctx.path_cache, path, entry);
    return entry ? entry->id : INVALID_FILE_ID;
}

static void cache_path(const char *path, file_id_t id) {
    path_cache_entry_t *entry = malloc(sizeof(path_cache_entry_t));
    strncpy(entry->path, path, PATH_MAX-1);
    entry->path[PATH_MAX-1] = '\0';
    entry->id = id;
    HASH_ADD_STR(realtime_ctx.path_cache, path, entry);
}

static void uncache_path(const char *path) {
    path_cache_entry_t *entry;
    HASH_FIND_STR(realtime_ctx.path_cache, path, entry);
    if (entry) {
        HASH_DEL(realtime_ctx.path_cache, entry);
        free(entry);
    }
}
