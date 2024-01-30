# 1️⃣🐝🏎️ The One Billion Row Challenge in Rust 🦀

My own implementation of the [1BRC](https://github.com/gunnarmorling/1brc) but in Rust.

## Test result on my local machine

Test are run under WSL2 (so using Hyper-V).

Hardware:

- CPU: AMD Ryzen 9 7950X 16-Core Processor (with 32 logical processors)
- RAM: 32G
- Disk: NVME

Before running anything. There is a huge performance difference when data are in memory cache or not.
So for every result, I will separate first run and real benchmark.

**NOTE**: I am cleaning memory cache using following command as root:

```sh
sync; echo 1 > /proc/sys/vm/drop_caches
```

Few utilities to deal with this can be found under the [utils](./utils/) directory.

### Java implementation

I do not have GraalVm, so I will only run the best pure Java implementation.

```sh
# First run:
❯ \time -f 'Elapsed=%E' ./calculate_average_yourwass.sh > /dev/null
Elapsed=0:03.14

# After few runs (so cache is now warm):
❯ \time -f 'Elapsed=%E' ./calculate_average_yourwass.sh > /dev/null
Elapsed=0:00.93
```

### My implementation results

```sh
# First run:
\time -f 'Elapsed=%E' target/release/one-brc-rs
Inside main total duration: 2.781221427s
Elapsed=0:02.96

# After few runs (so cache is now warm):
\time -f 'Elapsed=%E' target/release/one-brc-rs
Inside main total duration: 705.814095ms
Elapsed=0:00.81
```

## Inspiration and ideas

What works:

1. I start from [mtb0x1](https://github.com/mtb0x1/1brc) implementation
2. I have optimize a lot the line parsing using GodBolt compiler explorer (so is is only 46 assembly instruction now 😎).
3. I compute `fxhash::hash64(...)` only once and use raw `hashbrown::HashTable`.
4. I use fixed point number computation only at the last moment.
5. I have update thread stack size to have more CPU cache for the data.
6. I do not `munmap` the data a let the OS releasing the memory.

What does not work:

- Changing global allocator: using `strace` we can check no allocation are done after file is memory mapped.
  Tests have been done with [mimalloc](https://docs.rs/mimalloc/latest/mimalloc/) and jemalloc.

- Manual line split: `rayon` crate does the job pretty well.
- Updating `mmap` flags.
- Manually setting [core_affinity](https://docs.rs/core_affinity/latest/core_affinity/): it looks like the OS does a pretty good job.

Ideas:

- Using custom `HashMap`?
