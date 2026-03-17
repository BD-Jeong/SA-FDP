# BPF helper 212: probe_write_kernel_bio_write_stream

Kernel patch: adds a BPF helper that allows writing **only** to the **bi_write_stream** field of **struct bio**.

- **Helper**: `bpf_probe_write_kernel_bio_write_stream` (ID 212)
- **Args**: `(struct bio *bio, u8 stream_id)`  
  Returns `-EINVAL` if `bio` is NULL; otherwise performs only `copy_to_kernel_nofault(&bio->bi_write_stream, &stream_id, 1)`.
- **Files**: `include/uapi/linux/bpf.h`, `kernel/trace/bpf_trace.c`

---

## Security / Defensive Perspective Summary

This helper does **not** provide arbitrary kernel writes. It enforces a **three-layer defense** by restricting the target type, the writable field, and the write size.

### 1. Target type restriction (who can write)

- The first argument is declared as **`struct bio *bio`**.
- Callers must pass a value that the BPF verifier can treat as a **pointer to struct bio**; the verifier enforces this at load time.
- Passing an arbitrary kernel address (e.g. a different object type) causes the verifier to reject the program, so the helper cannot be invoked.
- The current prototype uses `ARG_ANYTHING`, but the intended semantics are “write only to a bio-typed object.”

### 2. Field whitelist (which field is written)

- The write address is **not** taken from an external argument; the helper uses a **single fixed field** internally.
- It always performs `copy_to_kernel_nofault` only on **`&bio->bi_write_stream`**.
- Other fields of **struct bio** (e.g. bi_iter, bi_opf) cannot be written through this helper.

### 3. Size restriction (how much is written)

- The write size is fixed at **`sizeof(stream_id)`**; `stream_id` is `u8`, so **exactly one byte** is written.
- `bi_write_stream` is a u8, so one byte is correct and no overflow into adjacent fields occurs.

### Summary

- **Target**: restricted to `struct bio *` (type/semantic constraint).
- **Field**: only the single field `bi_write_stream` is allowed (field whitelist).
- **Size**: fixed at 1 byte (overflow prevention).

Together, these ensure the operation is a **limited write to the bi_write_stream field of struct bio only**, not a general kernel memory write.

---

## Apply

**Prerequisite**: The patch is intended for a kernel tree that **does not yet have** helper 212. If another patch already uses helper ID 212, revert it before applying this one.

```bash
./apply_patches.sh /path/to/linux-6.17.12
```

If the script fails, apply `0003-bpf-add-probe_write_kernel-helper.patch` manually.

## Verify

From the kernel source root:

```bash
grep -q 'probe_write_kernel_bio_write_stream.*212' include/uapi/linux/bpf.h && echo "bpf.h OK"
grep -q '#include <linux/blk_types.h>' kernel/trace/bpf_trace.c && echo "include OK"
grep -q 'bpf_probe_write_kernel_bio_write_stream' kernel/trace/bpf_trace.c && echo "helper OK"
grep -q 'BPF_FUNC_probe_write_kernel_bio_write_stream' kernel/trace/bpf_trace.c && echo "switch OK"
```

After building, installing, and rebooting the kernel, BPF programs can call `bpf_probe_write_kernel_bio_write_stream(bio_ptr, stream_id)`.
