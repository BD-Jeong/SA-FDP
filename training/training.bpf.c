#include <uapi/linux/ptrace.h>
#include <linux/nvme.h>
#include <linux/types.h>
#include <linux/blk-mq.h>
#include <linux/blkdev.h>
#include <linux/blk_types.h>

#define CHUNK_SIZE (1024 * 1024) // Chunk size: 1MB (must match user space)
#define __LBA_SHIFT__ 0        // injected: 9 (512B) or 12 (4K)
#define __LBAS_PER_CHUNK__ 0   // injected: CHUNK_SIZE / logical_block_size
#define UPDATE_COUNT_MAX 65535 // 65535 (u16 max)
/* Injected: after increment, set sv_t2e / event when update_count == this (>=1). */
#define __SV_EVENT_AT_UPDATE__ 1
#define SECTOR_SHIFT 9

/* CLOCK_MONOTONIC ms (bpf_ktime_get_ns() / 1e6). */
struct window_times {
	u64 window_start_ms;
	u64 window_end_ms;
};

struct chunk_info {
	/* survival anlysis labels: time to event and event indicator */
	u16 sv_time2event_ms;
	u8 sv_event_indicator : 1;

	/* survival anlysis feature vectors */
    u16 update_count;
	u16 last_update_interval_ms;
	u64 last_update_ts_ms;

	u64 last_accessed_lba;
};

#define __MAX_CHUNKS__ 0 // Maximum number of chunks (injected from user space at runtime)
/* Namespace block device path (injected, for trace/docs). Match I/O via bd_dev below. */
#define __TRACKED_NS_PATH__ ""
#define __TRACK_BD_MAJOR__ 0
#define __TRACK_BD_MINOR__ 0

#ifndef MINORBITS
#define MINORBITS 20
#endif
#ifndef MINORMASK
#define MINORMASK ((1U << MINORBITS) - 1)
#endif

#ifndef BPF_PROBE_WRITE_KERNEL_BIO_WRITE_STREAM_ID
#define BPF_PROBE_WRITE_KERNEL_BIO_WRITE_STREAM_ID 212
#endif
static long (*bpf_probe_write_kernel_bio_write_stream)(struct bio *bio, u8 stream_id) = (void *)BPF_PROBE_WRITE_KERNEL_BIO_WRITE_STREAM_ID;

// Chunk array for O(1) access
BPF_ARRAY(chunk_array, struct chunk_info, 1);

/* Key 0: current window [start_ms, end_ms); userspace updates each interval. */
BPF_ARRAY(window_cfg, struct window_times, 1);

/* kprobe: LBA shift / LBAs per 1MB chunk injected from user space (blockdev --getss). */
int kprobe__nvme_setup_cmd(struct pt_regs *ctx) {
	u64 now_ms = bpf_ktime_get_ns() / 1000000ULL;
	u32 wcfg_key = 0;
	struct window_times *win = window_cfg.lookup(&wcfg_key);
	if (win && (win->window_start_ms || win->window_end_ms)) {
		if (now_ms <= win->window_start_ms)
			return 0;
		if (now_ms >= win->window_end_ms)
			return 0;
	}

	void *req = (void *)PT_REGS_PARM2(ctx);
	if (!req) return 0;

    /* Only the namespace block device we track (matches user space ns_path via st_rdev). */
    u64 bio_ptr = 0;
    if (bpf_probe_read_kernel(&bio_ptr, sizeof(bio_ptr), (void *)req + offsetof(struct request, bio)) != 0 || !bio_ptr)
        return 0;
    struct block_device *bdev = NULL;
    if (bpf_probe_read_kernel(&bdev, sizeof(bdev), (void *)bio_ptr + offsetof(struct bio, bi_bdev)) != 0 || !bdev)
        return 0;
    u32 bd_dev = 0;
    if (bpf_probe_read_kernel(&bd_dev, sizeof(bd_dev), (void *)bdev + offsetof(struct block_device, bd_dev)) != 0)
        return 0;
    {
        unsigned int maj = bd_dev >> MINORBITS;
        unsigned int min = bd_dev & MINORMASK;
        if (maj != (unsigned int)__TRACK_BD_MAJOR__ || min != (unsigned int)__TRACK_BD_MINOR__)
            return 0;
    }

    /* Only NVMe Write I/O (opcode 0x01). */
    u32 cmd_flags = 0;
    if (bpf_probe_read_kernel(&cmd_flags, sizeof(cmd_flags), (void *)req + offsetof(struct request, cmd_flags)) != 0)
        return 0;
    if ((cmd_flags & REQ_OP_MASK) != REQ_OP_WRITE)
        return 0;

    /* Extract SLBA, NLB from NVMe Command */
    u64 sector = 0;
    u32 bytes = 0;
    if (bpf_probe_read_kernel(&sector, sizeof(sector), (void *)req + offsetof(struct request, __sector)) != 0)
        return 0;
    if (bpf_probe_read_kernel(&bytes, sizeof(bytes), (void *)req + offsetof(struct request, __data_len)) != 0)
        return 0;

    u64 slba = sector >> (__LBA_SHIFT__ - SECTOR_SHIFT);
    u64 nlb_count = bytes >> __LBA_SHIFT__;
    u64 end_lba = slba + nlb_count;

    /* chunk_info update */
    u32 chunk_idx = slba / __LBAS_PER_CHUNK__;
    if (chunk_idx < __MAX_CHUNKS__) {
        struct chunk_info *info = chunk_array.lookup(&chunk_idx);
        if (info) {
            if (info->last_accessed_lba != (u64)-1 && slba == info->last_accessed_lba) {
                info->last_accessed_lba = end_lba;
                // bpf_trace_printk("Sequential write: %llu -> %llu\n", info->last_accessed_lba, end_lba);
            } else {
                if (info->update_count < UPDATE_COUNT_MAX)
                    info->update_count += 1;
                
                if (info->last_update_ts_ms == 0) {
                    info->last_update_ts_ms = now_ms;
                    info->last_update_interval_ms = 0;
                } else {
                    info->last_update_interval_ms = (u16)(now_ms - info->last_update_ts_ms);
                    info->last_update_ts_ms = now_ms;
                }
                if (info->update_count == __SV_EVENT_AT_UPDATE__) {
                    info->sv_time2event_ms = (u16)(now_ms - win->window_start_ms);
                    info->sv_event_indicator = 1;
                }
                info->last_accessed_lba = end_lba;
            }
        }
    }
    /*
    u8 stream_id = 0x5;
    long write_ret = bpf_probe_write_kernel_bio_write_stream((struct bio *)bio_ptr, stream_id);
    if (write_ret < 0)
        bpf_trace_printk("Write Failed! Error: %d slba: %llu nlb: %llu\n", (int)write_ret, slba, nlb_count);
    */
    return 0;
}
