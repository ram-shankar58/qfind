#include "qfind.h"
#include <liburing.h>
#include <pthread.h>

int io_context_init(io_context_t *ctx, int queue_size, bool use_sqpoll) {
    struct io_uring_params params = {0};
    
    if (use_sqpoll) {
        params.flags = IORING_SETUP_SQPOLL;
        params.sq_thread_idle = 2000; // milliseconds
    }
    
    // Initialize io_uring with the specified queue size
    int ret = io_uring_queue_init_params(queue_size, &ctx->ring, &params);
    if (ret < 0) {
        return ret;
    }
    
    ctx->registered_buffers = NULL;
    ctx->num_buffers = 0;
    ctx->use_sqpoll = use_sqpoll;
    
    return 0;
}

int io_context_destroy(io_context_t *ctx) {
    io_uring_queue_exit(&ctx->ring);
    for (int i = 0; i < ctx->num_buffers; i++) {
        free(ctx->registered_buffers[i]);
    }
    free(ctx->registered_buffers);
    return 0;
}

// Register a buffer with io_uring
int io_register_buffer(io_context_t *ctx, void *buf, size_t len) {
    struct iovec iov = {
        .iov_base = buf,
        .iov_len = len
    };
    
    int ret = io_uring_register_buffers(&ctx->ring, &iov, 1);
    if (ret < 0) {
        return ret;
    }
    
    // Keep track of registered buffer
    ctx->registered_buffers = realloc(ctx->registered_buffers, 
                                     (ctx->num_buffers + 1) * sizeof(void*));
    ctx->registered_buffers[ctx->num_buffers++] = buf;
    
    return 0;
}

// Submit an asynchronous read operation
int io_submit_read(io_context_t *ctx, int fd, void *buf, size_t len, off_t offset) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&ctx->ring);
    if (!sqe) {
        return -1;
    }
    
    // Check if this buffer is already registered
    int buf_index = -1;
    for (int i = 0; i < ctx->num_buffers; i++) {
        if (ctx->registered_buffers[i] == buf) {
            buf_index = i;
            break;
        }
    }
    
    if (buf_index >= 0) {
        // Use registered buffer
        io_uring_prep_read_fixed(sqe, fd, buf, len, offset, buf_index);
    } else {
        // Use regular read
        io_uring_prep_read(sqe, fd, buf, len, offset);
    }
    
    // Submit to the kernel
    return io_uring_submit(&ctx->ring);
}

// Wait for completions
int io_wait_completions(io_context_t *ctx, int min_completions) {
    int completed = 0;
    struct io_uring_cqe *cqe;
    
    while (completed < min_completions) {
        int ret = io_uring_wait_cqe(&ctx->ring, &cqe);
        if (ret < 0) {
            return ret;
        }
        
        // Process completion
        if (cqe->res < 0) {
            // Handle error
            fprintf(stderr, "I/O error: %s\n", strerror(-cqe->res));
        }
        
        io_uring_cqe_seen(&ctx->ring, cqe);
        completed++;
    }
    
    return completed;
}

// Clone buffer registration from one ring to another (Linux 5.12+)
int io_clone_buffers(io_context_t *dst, io_context_t *src) {
#ifdef IORING_REGISTER_BUFFERS2
    struct io_uring_rsrc_update2 update = {
        .offset = 0,
        .data = (__u64)src->registered_buffers,
        .len = src->num_buffers * sizeof(void*),
        .resv = 0,
        .tags = 0
    };
    
    return io_uring_register_buffers2(&dst->ring, &update, IORING_RSRC_BUFFER_RING_CLONE);
#else
    // Fallback for older kernels: re-register the same buffers
    for (int i = 0; i < src->num_buffers; i++) {
        int ret = io_register_buffer(dst, src->registered_buffers[i], 0);
        if (ret < 0) return ret;
    }
    return 0;
#endif
}
