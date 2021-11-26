/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2010-2021, Tarantool AUTHORS, please see AUTHORS file.
 */
#include "memory.h"
#include "fiber.h"
#include "tuple.h"
#include "zstd.h"
#include "msgpuck.h"

#include <iostream>
#include <vector>
#include <random>
#include <cstdint>
#include <algorithm>

#ifdef RAPID_JSON_FOUND
#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>
#endif


template <uint64_t MIN, uint64_t MAX>
static uint64_t
uniform_distribution_n(void)
{
	static std::random_device rd;
	static std::mt19937 gen(rd());
	static std::uniform_int_distribution<uint64_t> distrib(MIN, MAX);
	return distrib(gen);
}

/**
 * Class that compress and decompress tuples.
 */
class TupleCompressor
{
public:
	TupleCompressor(): ctx(NULL), dctx(NULL) {};
	~TupleCompressor()
	{
		ZSTD_freeCCtx(ctx);
		ZSTD_freeDCtx(dctx);
	}
	void
	settings(ZSTD_strategy strategy, int lvl)
	{
		size_t rc;
		ctx = ZSTD_createCCtx();
		dctx = ZSTD_createDCtx();
		if (ctx == NULL || dctx == NULL)
			abort();
		rc = ZSTD_CCtx_setParameter(ctx, ZSTD_c_strategy, strategy);
		if (ZSTD_isError(rc))
			abort();
		rc = ZSTD_CCtx_setParameter(ctx, ZSTD_c_compressionLevel, lvl);
		if (ZSTD_isError(rc))
			abort();
	}
	box_tuple_t *
	try_to_compress(box_tuple_t *src) const
	{
		size_t used = region_used(&fiber()->gc);
		size_t src_tuple_data_size = tuple_bsize(src);
		size_t compress_bound =
			ZSTD_compressBound(src_tuple_data_size);
		if (ZSTD_isError(compress_bound))
			abort();
		box_tuple_t *new_tuple = tuple_alloc(src, compress_bound);
		char *data = (char *)new_tuple + tuple_data_offset(src);
		size_t new_tuple_data_size = (ctx == NULL ?
			ZSTD_compress(data, compress_bound, tuple_data(src),
				      src_tuple_data_size, ZSTD_maxCLevel()) :
			ZSTD_compress2(ctx, data, compress_bound,
				       tuple_data(src), src_tuple_data_size));
		if (ZSTD_isError(new_tuple_data_size))
			abort();
		region_truncate(&fiber()->gc, used + tuple_data_offset(src) +
				new_tuple_data_size);
		tuple_set_size(src, new_tuple, new_tuple_data_size, true);
		return new_tuple;
	}
	box_tuple_t *
	try_to_decompress(box_tuple_t *src) const
	{
		size_t used = region_used(&fiber()->gc);
		size_t src_tuple_data_size = tuple_bsize(src);
		size_t decompress_bound =
			ZSTD_getFrameContentSize(tuple_data(src),
						 src_tuple_data_size);
		if (ZSTD_isError(decompress_bound))
			abort();
		box_tuple_t *new_tuple = tuple_alloc(src, decompress_bound);
		char *data = (char *)new_tuple + tuple_data_offset(src);
		size_t new_tuple_data_size = (dctx == NULL ?
			ZSTD_decompress(data, decompress_bound, tuple_data(src),
					src_tuple_data_size) :
			ZSTD_decompressDCtx(dctx, data, decompress_bound,
					    tuple_data(src),
					    src_tuple_data_size));
		if (ZSTD_isError(new_tuple_data_size))
			abort();
		region_truncate(&fiber()->gc, used + tuple_data_offset(src) +
				new_tuple_data_size);
		tuple_set_size(src, new_tuple, new_tuple_data_size, false);
		return new_tuple;
	}
private:
	box_tuple_t *
	tuple_alloc(box_tuple_t *old_tuple, size_t bound) const
	{
		size_t new_tuple_size_max =
			tuple_data_offset(old_tuple) + bound;
		box_tuple_t *new_tuple = (box_tuple_t *)
			region_alloc(&fiber()->gc, new_tuple_size_max);
		if (new_tuple == NULL)
			abort();
		memcpy(new_tuple, old_tuple, tuple_data_offset(old_tuple));
		return new_tuple;
	}
	void
	tuple_set_size(box_tuple_t *old_tuple, box_tuple_t *new_tuple,
		       size_t new_tuple_data_size, bool is_compressed) const
	{
		if (tuple_is_compact(old_tuple)) {
			new_tuple->data_offset_bsize_raw &= ~(0xff);
			new_tuple->data_offset_bsize_raw |= new_tuple_data_size;
		} else {
			new_tuple->bsize_bulky = new_tuple_data_size;
		}
		new_tuple->is_compressed = is_compressed;
	}
private:
	TupleCompressor& operator=(const TupleCompressor &);
	TupleCompressor(const TupleCompressor &);

	ZSTD_CCtx *ctx;
	ZSTD_DCtx *dctx;
};

/**
 *  Class that creates and destroys tuple format.
 */
class TupleFormat {
public:
	TupleFormat(struct tuple_format_vtab *vtab, void *engine,
		    std::vector<uint32_t> index_fieldno)
	{
		std::vector<struct key_part_def> kdp(index_fieldno.size());
		for (size_t i = 0; i < kdp.size(); i++) {
			memset(&kdp[i], 0, sizeof(struct key_part_def));
			kdp[i].fieldno = index_fieldno[i];
			kdp[i].type = FIELD_TYPE_UNSIGNED;
			index_fieldno_.push_back(index_fieldno[i]);
		}
		std::sort(index_fieldno_.begin(), index_fieldno_.end(),
			  std::less<uint32_t>());
		kd_ = key_def_new(kdp.data(), kdp.size(), false);
		fmt_ = tuple_format_new(vtab, engine, &kd_, 1, NULL, 0, 0,
				       NULL, false, false, NULL);
		tuple_format_ref(fmt_);
	}
	~TupleFormat()
	{
		tuple_format_unref(fmt_);
		key_def_delete(kd_);
	}
	const struct tuple_format *format() const { return fmt_; }
	const struct key_def *key_def() const { return kd_; }
	const std::vector<uint32_t> &fieldno() const { return index_fieldno_; };
	template <size_t ... index_fields>
	static const TupleFormat &default_format()
	{
		std::vector <uint32_t> index_fieldno{index_fields ...};
		struct tuple_format *fmt = box_tuple_format_default();
		static TupleFormat instance(&fmt->vtab, NULL, index_fieldno);
		return instance;
	}

private:
	TupleFormat(const TupleFormat &);
	TupleFormat& operator=(const TupleFormat &);

	std::vector<uint32_t> index_fieldno_;
	struct key_def *kd_;
	struct tuple_format *fmt_;
};

enum mp_data_type {
	MP_DATA_TYPE_UNSIGNED,
#ifdef  RAPID_JSON_FOUND
	MP_DATA_TYPE_JSON,
#endif
	MP_DATA_TYPE_MAX
};

/**
 * Generator of random msgpack array.
 */
template <size_t MP_DATA_SIZE>
class MpData {
public:
	void
	create(const TupleFormat &format, enum mp_data_type type)
	{
		size_t remaning = create_index_fields(format, type);
		switch (type) {
		case MP_DATA_TYPE_UNSIGNED:
			create_unsigned_fields(remaning);
			break;
#ifdef RAPID_JSON_FOUND
		case MP_DATA_TYPE_JSON:
			create_json_fields(remaning);
			break;
#endif
		default:
			abort();
		}
		if (data_end - data > MP_DATA_SIZE)
			abort();

	}
	const char *begin() const { return data; }
	const char *end() const { return data_end; }
private:
	void
	create_unsigned_fields(size_t remaning)
	{
		size_t add_nums = remaning / 9;
		for (size_t i = 0; i < add_nums; i++) {
			uint64_t val = uniform_distribution_n<1, UINT64_MAX>();
			data_end = mp_encode_uint(data_end, val);
		}
	}
#ifdef RAPID_JSON_FOUND
	void
	create_json_fields(size_t remaning)
	{
		rapidjson::Value json_val;
		rapidjson::Document doc;
		rapidjson::StringBuffer buffer;
		rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
		uint64_t value;
		auto& allocator = doc.GetAllocator();
		doc.SetObject();
		do {
			value = uniform_distribution_n<1, UINT64_MAX>();
			json_val.SetUint64(value);
			doc.AddMember("id", json_val, allocator);
			json_val.SetString("user", allocator);
			doc.AddMember("name", json_val, allocator);
			value = uniform_distribution_n<1, UINT64_MAX>();
			json_val.SetUint64(value);
			doc.AddMember("phone", json_val, allocator);
			doc.Accept(writer);
		} while (strlen(buffer.GetString()) < remaning);
		data_end = mp_encode_str(data_end, buffer.GetString(),
					 remaning);
	}
#endif
	size_t
	create_index_fields(const TupleFormat &format, enum mp_data_type type)
	{
		std::vector<uint32_t> index_fields;
		/** Generate values for indexed fields. */
		for (size_t i = 0; i < format.fieldno().size(); i++) {
			uint64_t val = uniform_distribution_n<1, UINT64_MAX>();
			index_fields.push_back(val);
		}
		/**
		 * All fields between indexed fields are filled with a
		 * string "tuple" (lenght 5). Max msgpuck array header
		 * size is also equal to 5. First msgpuck array field is
		 * MP_NIL.
		 */
		size_t common_size = 5 +  mp_sizeof_str(5) *
			(format.fieldno().back() - format.fieldno().size()) +
			mp_sizeof_nil();
		for (uint32_t i = 0; i < index_fields.size(); i++)
			common_size += mp_sizeof_uint(index_fields[i]);
		/**
		 * Creates remaining msgpuck fields, according to mp_data_type.
		 */
		size_t add_nums = (type == MP_DATA_TYPE_UNSIGNED ?
			(MP_DATA_SIZE - common_size) / 9 : 1);
		data_end = data;
		data_end = mp_encode_array(data_end, format.fieldno().back() +
					   add_nums + 1);
		data_end = mp_encode_nil(data_end);
		for (size_t i = 0; i < format.fieldno().size(); i ++) {
			size_t fieldno = (i == 0 ? 1 :
				format.fieldno()[i - 1] + 1);
			for (size_t j = fieldno; j < format.fieldno()[i]; j++)
				data_end = mp_encode_str(data_end, "tuple", 5);
			data_end = mp_encode_uint(data_end, index_fields[i]);
		}
		if (data_end - data > common_size)
			abort();
		return MP_DATA_SIZE - common_size;
	}
private:
	char data[MP_DATA_SIZE];
	char *data_end;
};

/**
 * Generator of set of random msgpack arrays.
 */
template <size_t MP_DATA_COUNT, size_t MAX_MP_DATA_SIZE>
class MpDataSet {
public:
	MpDataSet(const TupleFormat &format, enum mp_data_type type)
	{
		for (size_t i = 0; i < MP_DATA_COUNT; i++)
			data[i].create(format, type);
	}
	const MpData<MAX_MP_DATA_SIZE> &
	operator[](size_t i) const { return data[i]; }
private:
	MpData<MAX_MP_DATA_SIZE> data[MP_DATA_COUNT];
};

/**
 * Generator of a set of random tuples.
 */
template <size_t TUPLE_COUNT, size_t TUPLE_DATA_SIZE>
class TupleSet
{
public:
	TupleSet(const TupleFormat &fmt, const TupleCompressor &compressor,
		 enum mp_data_type type)
	: fmt_(fmt)
	, compressor_(compressor)
	{
		/**
		 * Unable to allocate big object on stack.
		 */
		MpDataSet<TUPLE_COUNT, TUPLE_DATA_SIZE> *dataset;
		try {
			dataset = new MpDataSet<TUPLE_COUNT,
						TUPLE_DATA_SIZE>(fmt, type);
		} catch (std::bad_alloc) {
			abort();
		}
		struct tuple_format *format =
			const_cast<struct tuple_format *>(fmt_.format());
		struct key_def * kd =
			const_cast<struct key_def *>(fmt_.key_def());
		data_.resize(TUPLE_COUNT);
		for (size_t i = 0; i < data_.size(); i++) {
			data_[i] = box_tuple_new(format,
						 (*dataset)[i].begin(),
						 (*dataset)[i].end());
			tuple_ref(data_[i]);
		}
		delete dataset;
	}
	~TupleSet()
	{
		for (size_t i = 0; i < TUPLE_COUNT; i++)
			tuple_unref(data_[i]);
	}
	std::vector<tuple *>::iterator begin() { return data_.begin(); }
	std::vector<tuple *>::iterator end() { return data_.end(); }
	box_tuple_t *operator[](size_t i) { return data_[i]; }
	void
	sort(void)
	{
		std::sort(begin(), end(),
			  [this](const box_tuple_t * ca,
				 const box_tuple_t * cb)
		{
			box_tuple_t *a = const_cast<box_tuple_t *>(ca);
			box_tuple_t *b = const_cast<box_tuple_t *>(cb);
			struct key_def  *kd =
				const_cast<struct key_def *>(fmt_.key_def());
			return (box_tuple_compare(a, b, kd) < 0);
		});
	}
	bool
	search(box_tuple_t *tuple, uint32_t &count)
	{
		return std::binary_search(begin(), end(), tuple,
					  [this, &count](const box_tuple_t *ca,
							 const box_tuple_t *cb)
		{
			box_tuple_t *a = const_cast<box_tuple_t *>(ca), *da = a;
			box_tuple_t *b = const_cast<box_tuple_t *>(cb), *db = b;
			if (a->is_compressed) {
				da = compressor_.try_to_decompress(a);
				tuple_ref(da);
			}
			if (b->is_compressed) {
				db = compressor_.try_to_decompress(b);
				tuple_ref(db);
			}
			struct key_def  *kd =
				const_cast<struct key_def *>(fmt_.key_def());
			bool rc = (box_tuple_compare(da, db, kd) < 0);
			if (a->is_compressed)
				tuple_unref(da);
			if (b->is_compressed)
				tuple_unref(db);
			count++;
			return rc;
		});
	}
private:
	TupleSet(const TupleSet &);
	TupleSet& operator=(const TupleSet &);

	const TupleFormat &fmt_;
	const TupleCompressor &compressor_;
	std::vector<box_tuple_t *>data_;
};

/**
 * Сlass that provides initialization and deinitialization of
 * resources which required for all tests.
 */
class PerfTestInit
{
public:
	PerfTestInit()
	{
		srand(time(NULL));
		memory_init();
		fiber_init(fiber_c_invoke);
		tuple_init(NULL);
	}
	~PerfTestInit()
	{
		tuple_free();
		fiber_free();
		memory_free();
	}
};
static PerfTestInit instance;

static void
show_warning_if_debug()
{
#ifndef NDEBUG
	std::cerr << "#######################################################\n"
		  << "#######################################################\n"
		  << "#######################################################\n"
		  << "###                                                 ###\n"
		  << "###                    WARNING!                     ###\n"
		  << "###   The performance test is run in debug build!   ###\n"
		  << "###   Test results are definitely inappropriate!    ###\n"
		  << "###                                                 ###\n"
		  << "#######################################################\n"
		  << "#######################################################\n"
		  << "#######################################################\n";
#endif // #ifndef NDEBUG
}

static struct DebugWarning {
	DebugWarning() { show_warning_if_debug(); }
} debug_warning;
