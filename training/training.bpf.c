#include <uapi/linux/ptrace.h>
#include <linux/nvme.h>
#include <linux/types.h>
#include <linux/blk-mq.h>
#include <linux/blkdev.h>
#include <linux/blk_types.h>

#define CHUNK_SIZE (1024 * 1024) // Chunk size: 1MB (1048576 bytes)
#define LBA_SIZE (4 * 1024) // LBA size: 4KB (4096 bytes)
#define LBAS_PER_CHUNK (CHUNK_SIZE / LBA_SIZE) // 256 LBAs (1MB / 4KB)
#define UPDATE_COUNT_MAX 65535 // 65535 (u16 max)
#define SECTOR_SHIFT 9

struct chunk_info {
    // survival analysis labels: time to event and event indicator
    u32 sv_time2event_ts;
    u8 sv_event_indicator : 1;  // Boolean value
    // survival analysis feature vectors: update count, last update
    u16 update_count;
    u32 last_update_ts;

    u64 last_accessed_lba;
};

#define __MAX_CHUNKS__ 0 // Maximum number of chunks (injected from user space at runtime)

#ifndef BPF_PROBE_WRITE_KERNEL_BIO_WRITE_STREAM_ID
#define BPF_PROBE_WRITE_KERNEL_BIO_WRITE_STREAM_ID 212
#endif
static long (*bpf_probe_write_kernel_bio_write_stream)(struct bio *bio, u8 stream_id) = (void *)BPF_PROBE_WRITE_KERNEL_BIO_WRITE_STREAM_ID;

// Chunk array for O(1) access
BPF_ARRAY(chunk_array, struct chunk_info, 1);

/* kprobe: req->q->limits.logical_block_size로 lba_shift 계산 (ns/head 불필요). */
int kprobe__nvme_setup_cmd(struct pt_regs *ctx) {
    void *req = (void *)PT_REGS_PARM2(ctx);
    if (!req) return 0;

    u32 cmd_flags = 0;
    if (bpf_probe_read_kernel(&cmd_flags, sizeof(cmd_flags), (void *)req + offsetof(struct request, cmd_flags)) != 0)
        return 0;
    if ((cmd_flags & REQ_OP_MASK) != REQ_OP_WRITE)
        return 0;

    u64 sector = 0;
    u32 bytes = 0;
    if (bpf_probe_read_kernel(&sector, sizeof(sector), (void *)req + offsetof(struct request, __sector)) != 0)
        return 0;
    if (bpf_probe_read_kernel(&bytes, sizeof(bytes), (void *)req + offsetof(struct request, __data_len)) != 0)
        return 0;

    /* lba_shift = log2(logical_block_size) from req->q->limits.logical_block_size */
    void *q = NULL;
    u32 logical_block_size = 0;
    if (bpf_probe_read_kernel(&q, sizeof(q), (void *)req + offsetof(struct request, q)) != 0 || !q)
        return 0;
    if (bpf_probe_read_kernel(&logical_block_size, sizeof(logical_block_size),
            (void *)q + offsetof(struct request_queue, limits) + offsetof(struct queue_limits, logical_block_size)) != 0)
        return 0;
    if (logical_block_size == 0)
        return 0;
    u8 lba_shift = 0;
    if (logical_block_size == 512)
        lba_shift = 9;
    else if (logical_block_size == 4096)
        lba_shift = 12;
    else
        return 0;

    /* slba = sector >> (lba_shift - SECTOR_SHIFT), nlb_count = bytes >> lba_shift */
    u64 slba = sector >> (lba_shift - SECTOR_SHIFT);
    u64 nlb_count = bytes >> lba_shift;
    u64 end_lba = slba + nlb_count;
   
    /* chunk_info update */
    u32 chunk_idx = slba / LBAS_PER_CHUNK;
    if (chunk_idx < __MAX_CHUNKS__) {
        struct chunk_info *info = chunk_array.lookup(&chunk_idx);
        if (info) {
            bpf_trace_printk("last_accessed_lba: %llu slba: %llu\n", info->last_accessed_lba, slba);
            if (info->last_accessed_lba != (u64)-1 && slba == info->last_accessed_lba) {
                info->last_accessed_lba = end_lba;
                bpf_trace_printk("Sequential write: %llu -> %llu\n", info->last_accessed_lba, end_lba);
            } else {
                if (info->update_count < UPDATE_COUNT_MAX)
                    info->update_count += 1;
                u64 now_sec = bpf_ktime_get_ns() / 1000000000ULL;
                info->last_update_ts = (u32)now_sec;
                info->sv_time2event_ts = (u32)now_sec;
                info->sv_event_indicator = 1;
                info->last_accessed_lba = end_lba;
            }
        }
    }

    u64 bio_ptr = 0;
    bpf_probe_read_kernel(&bio_ptr, sizeof(bio_ptr), (void *)req + offsetof(struct request, bio));
    if (!bio_ptr) return 0;

    u8 stream_id = 0x5;
    long write_ret = bpf_probe_write_kernel_bio_write_stream((struct bio *)bio_ptr, stream_id);
    if (write_ret < 0)
        bpf_trace_printk("Write Failed! Error: %d slba: %llu nlb: %llu\n", (int)write_ret, slba, nlb_count);
    return 0;
}
