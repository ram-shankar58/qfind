#include "qfind.h"
#include <string.h>
#include <sys/uio.h>
#include <stdatomic.h>
#include <errno.h>

static const char *io_error_str(int error) {
    static const char *errors[] = {
        [EPERM] = "Operation not permitted",
        [ENOBUFS] = "No buffer space",
        [ENOMEM] = "Out of memory",
        [EINVAL] = "Invalid parameters",
        [EBUSY] = "Resource busy",
        [EFAULT] = "Bad address",
        [ENOSPC] = "No space left",
        [ENOTSUP] = "Operation not supported"
    };
    return (error > 0 || -error >= (int)(sizeof(errors)/sizeof(errors[0]))) ? 
        "Unknown error" : errors[-error];
}

int io_context_init(io_context_t *ctx, int queue_size, bool use_sqpoll) {
    struct io_uring_params params = {0};
    
    if (use_sqpoll) {
        params.flags = IORING_SETUP_SQPOLL;
        params.sq_thread_idle = 2000;
    }
    
    int ret = io_uring_queue_init_params(queue_size, &ctx->ring, &params);
    if (ret < 0) return ret;

    memset(ctx->buffers, 0, sizeof(ctx->buffers));
    ctx->num_buffers = 0;
    ctx->use_sqpoll = use_sqpoll;
    pthread_spin_init(&ctx->buffer_lock, PTHREAD_PROCESS_PRIVATE);
    
    return 0;
}

int io_context_destroy(io_context_t *ctx) {
    if (ctx->num_buffers > 0) {
        struct iovec iovs[MAX_REG_BUFFERS];
        for (int i = 0; i < ctx->num_buffers; i++) {
            iovs[i].iov_base = ctx->buffers[i].buf;
            iovs[i].iov_len = ctx->buffers[i].len;
        }
        io_uring_unregister_buffers(&ctx->ring);
    }
    
    io_uring_queue_exit(&ctx->ring);
    pthread_spin_destroy(&ctx->buffer_lock);
    return 0;
}

int io_register_buffers(io_context_t *ctx, struct iovec *iovs, int nr_iovs) {
    if (nr_iovs <= 0 || nr_iovs > MAX_REG_BUFFERS) return -EINVAL;
    
    pthread_spin_lock(&ctx->buffer_lock);
    
    if (ctx->num_buffers + nr_iovs > MAX_REG_BUFFERS) {
        pthread_spin_unlock(&ctx->buffer_lock);
        return -ENOSPC;
    }
    
    int ret = io_uring_register_buffers(&ctx->ring, iovs, nr_iovs);
    if (ret < 0) {
        pthread_spin_unlock(&ctx->buffer_lock);
        return ret;
    }

    // Kernel returns actual buffer indices in case of partial registration
    for (int i = 0; i < nr_iovs; i++) {
        ctx->buffers[ctx->num_buffers] = (reg_buffer_t){
            .buf = iovs[i].iov_base,
            .len = iovs[i].iov_len,
            .refcount = 0,
            .kernel_idx = ctx->num_buffers // Kernel uses 0-based index
        };
        ctx->num_buffers++;
    }
    
    pthread_spin_unlock(&ctx->buffer_lock);
    return 0;
}

static int find_buffer_index(io_context_t *ctx, void *buf) {
    for (int i = 0; i < ctx->num_buffers; i++) {
        if (ctx->buffers[i].buf == buf) {
            return i;
        }
    }
    return -1;
}

int io_submit_read(io_context_t *ctx, int fd, void *buf, size_t len, off_t offset) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&ctx->ring);
    if (!sqe) return -EBUSY;

    pthread_spin_lock(&ctx->buffer_lock);
    int buf_idx = find_buffer_index(ctx, buf);
    if (buf_idx >= 0) {
        if (ctx->buffers[buf_idx].len < len) {
            pthread_spin_unlock(&ctx->buffer_lock);
            return -EINVAL;
        }
        atomic_fetch_add(&ctx->buffers[buf_idx].refcount, 1);
        io_uring_prep_read_fixed(sqe, fd, buf, len, offset, buf_idx);
    } else {
        io_uring_prep_read(sqe, fd, buf, len, offset);
    }
    pthread_spin_unlock(&ctx->buffer_lock);
    
    sqe->flags |= IOSQE_ASYNC;
    return io_uring_submit(&ctx->ring);
}

int io_submit_write(io_context_t *ctx, int fd, void *buf, size_t len, off_t offset) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&ctx->ring);
    if (!sqe) return -EBUSY;

    pthread_spin_lock(&ctx->buffer_lock);
    int buf_idx = find_buffer_index(ctx, buf);
    if (buf_idx >= 0) {
        if (ctx->buffers[buf_idx].len < len) {
            pthread_spin_unlock(&ctx->buffer_lock);
            return -EINVAL;
        }
        atomic_fetch_add(&ctx->buffers[buf_idx].refcount, 1);
        io_uring_prep_write_fixed(sqe, fd, buf, len, offset, buf_idx);
    } else {
        io_uring_prep_write(sqe, fd, buf, len, offset);
    }
    pthread_spin_unlock(&ctx->buffer_lock);
    
    sqe->flags |= IOSQE_ASYNC;
    return io_uring_submit(&ctx->ring);
}

int io_wait_completions(io_context_t *ctx, int min_completions, 
                        io_cqe_t *cqes, int *num_cqes) {
    struct io_uring_cqe *ring_cqes[CQE_BATCH_SIZE];
    int completed = 0;
    int total = 0;
    int ret;
    
    do {
        ret = io_uring_peek_batch_cqe(&ctx->ring, ring_cqes, CQE_BATCH_SIZE);
        if (ret < 0) {
            if (ret == -EAGAIN && completed >= min_completions) break;
            return ret;
        }

        for (int i = 0; i < ret; i++) {
            if (cqes && total < *num_cqes) {
                cqes[total].user_data = ring_cqes[i]->user_data;
                cqes[total].res = ring_cqes[i]->res;
                cqes[total].flags = ring_cqes[i]->flags;
            }

            // Handle buffer references
            if (ring_cqes[i]->flags & IORING_CQE_F_BUFFER) {
                int buf_idx = ring_cqes[i]->flags >> IORING_CQE_BUFFER_SHIFT;
                pthread_spin_lock(&ctx->buffer_lock);
                if (buf_idx >= 0 && buf_idx < ctx->num_buffers) {
                    atomic_fetch_sub(&ctx->buffers[buf_idx].refcount, 1);
                }
                pthread_spin_unlock(&ctx->buffer_lock);
            }

            io_uring_cqe_seen(&ctx->ring, ring_cqes[i]);
            completed++;
            total++;
        }

        if (ret < CQE_BATCH_SIZE && completed < min_completions) {
            ret = io_uring_wait_cqe(&ctx->ring, &ring_cqes[0]);
            if (ret < 0) {
                if (ret == -EINTR) continue;
                return ret;
            }
        }
    } while (completed < min_completions);

    if (num_cqes) *num_cqes = total;
    return completed;
}

int io_unregister_buffer(io_context_t *ctx, void *buf) {
    pthread_spin_lock(&ctx->buffer_lock);
    
    int idx = find_buffer_index(ctx, buf);
    if (idx < 0) {
        pthread_spin_unlock(&ctx->buffer_lock);
        return -ENOENT;
    }
    
    if (atomic_load(&ctx->buffers[idx].refcount) > 0) {
        pthread_spin_unlock(&ctx->buffer_lock);
        return -EBUSY;
    }

    struct iovec iov = {
        .iov_base = buf,
        .iov_len = ctx->buffers[idx].len
    };
    
    int ret = io_uring_unregister_buffers(&ctx->ring);
    if (ret == 0) {
        // Shift array left
        memmove(&ctx->buffers[idx], &ctx->buffers[idx+1], 
               (ctx->num_buffers - idx - 1) * sizeof(reg_buffer_t));
        ctx->num_buffers--;
    }
    
    pthread_spin_unlock(&ctx->buffer_lock);
    return ret;
}

const char *io_strerror(int error) {
    return io_error_str(error);
}
