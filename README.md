# wimage

Clean-room C implementation of the Windows Imaging (WIM) format. Creates, extracts, and inspects WIM archives compatible with Microsoft DISM, ImageX, and wimlib-imagex.

Not a fork of wimlib. Every component (XPRESS Huffman codec, SHA-1 deduplication, metadata serializer, bitstream encoder) was written from scratch based on format analysis alone.

## Build

```
cmake -B build
cmake --build build
```

## Quick start

```sh
# Capture a directory
./build/wimage capture /my/files backup.wim "My Backup" --compress=xpress --threads=4

# List contents
./build/wimage dir backup.wim

# Extract
./build/wimage apply backup.wim 1 /restore/here

# Verify integrity
./build/wimage capture /data archive.wim "Data" --check
./build/wimage verify archive.wim
```

## Commands

```
wimage capture <source> <wimfile> [name] [--compress=xpress|none] [--threads=N] [--check]
wimage apply   <wimfile> [image] <target>
wimage info    <wimfile> [--header] [--xml] [--blobs]
wimage dir     <wimfile> [--detailed]
wimage verify  <wimfile>
wimage extract <wimfile> <image> [paths...] [--dest-dir=DIR]
```

## Features

- XPRESS Huffman compression and decompression
- SHA-1 content-addressed deduplication
- Multi-threaded compression
- Integrity table generation and verification
- Full interoperability with wimlib-imagex and Microsoft DISM

## Embeddable library

`libwim/` is a self-contained pure C11 library with zero external dependencies. Copy the directory into your project and compile:

```c
#include "wim_read.h"

WimCtx ctx;
wim_ctx_init(&ctx);
wim_open(&ctx, "image.wim");
wim_select_image(&ctx, 1);
wim_extract_tree(&ctx, wim_get_root(&ctx, 1), "/output");
wim_close(&ctx);
```

Designed for constrained environments: bootloaders, firmware, OS installers.

## Benchmarks

Linux x86_64, GCC -O2, OpenSSL SHA-1.

### Compression (MB/s)

| Tool | 32 MB | 64 MB | 128 MB |
|---|---:|---:|---:|
| wimage SMP1 | 834 | 867 | 933 |
| wimage SMP4 | 1528 | 1324 | 1519 |
| wimlib-imagex | 829 | 736 | 966 |

### Decompression (MB/s)

| Direction | 32 MB | 64 MB | 128 MB |
|---|---:|---:|---:|
| wimage > wimage | 815 | 868 | 873 |
| wimlib > wimage | 826 | 880 | 857 |
| wimlib > wimlib | 1056 | 1059 | 1074 |

## Tests

```sh
# Unit tests
cmake -B build -DENABLE_TESTS=ON && cmake --build build
./build/wimage_tests    # 106 tests

# Integration tests (requires wimlib-imagex)
cd scripts && python3 test_compare.py && python3 test_random250.py
```

## License

GPL-2.0-or-later
