#include "qfind.h"
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <pwd.h>
#include <grp.h>

#define VERSION "1.0.0"

void print_usage(const char *prog_name) {
    printf("Usage: %s [OPTION]... PATTERN...\n", prog_name);
    printf("Quickly search for files by name.\n\n");
    printf("Options:\n");
    printf("  -d, --database=DBPATH     use DBPATH as database\n");
    printf("  -i, --ignore-case         ignore case distinctions\n");
    printf("  -r, --regexp              pattern is a regular expression\n");
    printf("  -u, --update              update the database\n");
    printf("  -h, --help                display this help\n");
    printf("  -v, --version             display version information\n");
}

int main(int argc, char *argv[]) {
    char *db_path = NULL;
    bool ignore_case = false;
    bool use_regex = false;
    bool update_db = false;
    
    static struct option long_options[] = {
        {"database", required_argument, 0, 'd'},
        {"ignore-case", no_argument, 0, 'i'},
        {"regexp", no_argument, 0, 'r'},
        {"update", no_argument, 0, 'u'},
        {"help", no_argument, 0, 'h'},
        {"version", no_argument, 0, 'v'},
        {0, 0, 0, 0}
    };
    
    int opt;
    int option_index = 0;
    
    while ((opt = getopt_long(argc, argv, "d:iruhv", long_options, &option_index)) != -1) {
        switch (opt) {
            case 'd':
                db_path = optarg;
                break;
            case 'i':
                ignore_case = true;
                break;
            case 'r':
                use_regex = true;
                break;
            case 'u':
                update_db = true;
                break;
            case 'h':
                print_usage(argv[0]);
                return 0;
            case 'v':
                printf("qfind %s\n", VERSION);
                return 0;
            default:
                print_usage(argv[0]);
                return 1;
        }
    }
    
    // Initialize the index
    qfind_index_t *index = qfind_init();
    if (!index) {
        fprintf(stderr, "Failed to initialize index\n");
        return 1;
    }
    
    // Load database or update it if requested
    if (update_db) {
        printf("Updating database...\n");
        qfind_build_index(index, "/");  // Start from root
        printf("Database updated.\n");
        return 0;
    }
    
    // Check if we have a pattern to search for
    if (optind >= argc) {
        fprintf(stderr, "No search pattern provided\n");
        print_usage(argv[0]);
        qfind_destroy(index);
        return 1;
    }
    
    // Set up query context
    query_ctx_t query = {0};
    query.query = argv[optind];
    query.case_sensitive = !ignore_case;
    query.regex_enabled = use_regex;
    query.max_results = MAX_RESULTS;
    
    // Get user and group ID for permission checking
    query.user_id = getuid();
    query.group_id = getgid();
    
    // Perform search
    int result_count = qfind_search(index, &query);
    
    // Display results
    if (result_count > 0) {
        printf("Found %d results:\n", result_count);
        for (uint32_t i = 0; i < query.num_results; i++) {
            file_id_t id = query.results[i];
            printf("%s\n", index->file_metadata[id].path);
        }
    } else {
        printf("No matching files found.\n");
    }
    
    // Clean up
    free(query.results);
    qfind_destroy(index);
    
    return 0;
}
