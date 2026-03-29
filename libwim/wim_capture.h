/*
 * Copyright (C) 2026 Ahmed ARIF <arif.ing@outlook.com>
 *
 * wim_capture.h - Directory tree walker for WIM capture (pure C)
 *
 */

#ifndef WIM_CAPTURE_H
#define WIM_CAPTURE_H

#include "wim_types.h"

/* Callback: write blob data, receive SHA-1. Return 0 on success. */
typedef int (*wim_blob_writer_fn)(const uint8_t* data, uint64_t size,
                                  uint8_t sha1_out[20], void* user);

/* Capture a directory tree into a WimDentry tree, writing file blobs via callback. */
int wim_capture_dir(const char* source_dir, WimDentry* root,
                    wim_blob_writer_fn writer, void* user);

#endif /* WIM_CAPTURE_H */
