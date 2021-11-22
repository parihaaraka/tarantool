/*
 * Copyright 2010-2021, Tarantool AUTHORS, please see AUTHORS file.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */
#include "tuple_compression.h"
#include "space.h"
#include "tuple.h"

#include <zstd.h>

struct tuple *
tuple_compression(struct tuple *old_tuple, struct space *space)
{
        const char *data = tuple_data(old_tuple);
        const char *end = data + tuple_bsize(old_tuple);
#if 0
        for (uint32_t i = 0; i < space->index_count; i++) {
                struct key_def *key_def = space->index[i]->def->key_def;
                for (uint32_t j = 0; j < key_def->part_count; j++)
                        fprintf(stderr, "%d bINDEX FIELDS %u %u\n", i, key_def->parts[j].fieldno, space->def->id);
        }
#endif
        struct tuple *new_tuple =
                space->format->vtab.tuple_new(space->format, data, end);
        return new_tuple;
}

struct tuple *
tuple_decompression(struct tuple *old_tuple, struct space *space)
{
        const char *data = tuple_data(old_tuple);
        const char *end = data + tuple_bsize(old_tuple);
        struct tuple *new_tuple =
                space->format->vtab.tuple_new(space->format, data, end);
        return new_tuple;
}

struct tuple *
tuple_new_with_compression(struct space *space, const char *data,
                           const char *end)
{
        struct tuple *tmp =
                space->format->vtab.tuple_new(space->format, data, end);
        if (tmp == NULL)
                return NULL;
        struct tuple *new_tuple = tuple_compression(tmp, space);
        space->format->vtab.tuple_delete(space->format, tmp);
        return new_tuple;
}
