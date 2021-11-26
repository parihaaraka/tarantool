#include "helpers.h"

#include "memtx_engine.h"
#include <allocator.h>

#include <benchmark/benchmark.h>

const size_t NUM_TEST_TUPLES = 4096;
const size_t TUPLE_DATA_SIZE = 512;
const TupleFormat &format = TupleFormat::default_format<4>();

// Class that creates and destroys tuple format for private memtx engine.
class MemtxEngine {
public:
	static MemtxEngine &instance(const TupleFormat &format)
	{
		static MemtxEngine instance(format);
		return instance;
	}
	const struct tuple_format *format() const { return fmt.format(); }
	const struct key_def *key_def() const { return fmt.key_def(); }
private:
	MemtxEngine(const TupleFormat &format): fmt(format)
	{
		memset(&memtx, 0, sizeof(memtx));

		quota_init(&memtx.quota, QUOTA_MAX);

		int rc;
		rc = slab_arena_create(&memtx.arena, &memtx.quota,
				       16 * 1024 * 1024, 16 * 1024 * 1024,
				       SLAB_ARENA_PRIVATE);
		if (rc != 0)
			abort();

		slab_cache_create(&memtx.slab_cache, &memtx.arena);

		float actual_alloc_factor;
		allocator_settings alloc_settings;
		allocator_settings_init(&alloc_settings, &memtx.slab_cache,
					16, 8, 1.1, &actual_alloc_factor,
					&memtx.quota);
		SmallAlloc::create(&alloc_settings);
		memtx_set_tuple_format_vtab("small");

		memtx.max_tuple_size = 1024 * 1024;
	}
	~MemtxEngine()
	{
		SmallAlloc::destroy();
		slab_cache_destroy(&memtx.slab_cache);
		tuple_arena_destroy(&memtx.arena);
	}

	struct memtx_engine memtx;
	const TupleFormat &fmt;
};

// box_tuple_new benchmark.
static void
bench_tuple_new(benchmark::State& state)
{
	size_t total_count = 0;
	struct tuple_format *fmt = const_cast<struct tuple_format *>
		(MemtxEngine::instance(format).format());
	MpDataSet<NUM_TEST_TUPLES, TUPLE_DATA_SIZE>
		dataset(format, MP_DATA_TYPE_UNSIGNED);
	struct tuple *tuples[NUM_TEST_TUPLES];
	size_t i = 0;

	for (auto _ : state) {
		if (i == NUM_TEST_TUPLES) {
			total_count += i;
			state.PauseTiming();
			for (size_t k = 0; k < NUM_TEST_TUPLES; k++)
				tuple_unref(tuples[k]);
			i = 0;
			state.ResumeTiming();
		}
		tuples[i] = box_tuple_new(fmt,
					  dataset[i].begin(),
					  dataset[i].end());
		tuple_ref(tuples[i]);
		++i;
	}
	total_count += i;
	state.SetItemsProcessed(total_count);

	for (size_t k = 0; k < i; k++)
		tuple_unref(tuples[k]);
}

BENCHMARK(bench_tuple_new);

// memtx_tuple_delete benchmark.
static void
bench_tuple_delete(benchmark::State& state)
{
	size_t total_count = 0;
	struct tuple_format *fmt = const_cast<struct tuple_format *>
		(MemtxEngine::instance(format).format());
	MpDataSet<NUM_TEST_TUPLES, TUPLE_DATA_SIZE>
		dataset(format, MP_DATA_TYPE_UNSIGNED);
	struct tuple *tuples[NUM_TEST_TUPLES];

	size_t i = NUM_TEST_TUPLES;
	for (auto _ : state) {
		if (i == NUM_TEST_TUPLES) {
			total_count += i;
			state.PauseTiming();
			for (size_t k = 0; k < NUM_TEST_TUPLES; k++) {
				tuples[k] = box_tuple_new(fmt,
							  dataset[k].begin(),
							  dataset[k].end());
				tuple_ref(tuples[k]);
			}
			i = 0;
			state.ResumeTiming();
		}
		tuple_unref(tuples[i++]);
	}
	total_count += i;
	state.SetItemsProcessed(total_count);

	for (size_t k = i; k < NUM_TEST_TUPLES; k++)
		tuple_unref(tuples[k]);
}

BENCHMARK(bench_tuple_delete);

static void
bench_tuple_ref_unref_low(benchmark::State& state)
{
	TupleSet<NUM_TEST_TUPLES, TUPLE_DATA_SIZE>
		tuples(format, TupleCompressor(), MP_DATA_TYPE_UNSIGNED);
	size_t total_count = 0;
	const size_t NUM_REFS = 32;
	for (auto _ : state) {
		for (size_t k = 0; k < NUM_REFS; k++)
			for (size_t i = 0; i < NUM_TEST_TUPLES; i++)
				tuple_ref(tuples[i]);
		for (size_t k = 0; k < NUM_REFS; k++)
			for (size_t i = 0; i < NUM_TEST_TUPLES; i++)
				tuple_unref(tuples[i]);
		total_count += NUM_REFS * NUM_TEST_TUPLES;
	}
	state.SetItemsProcessed(total_count);
}

BENCHMARK(bench_tuple_ref_unref_low);

static void
bench_tuple_ref_unref_high(benchmark::State& state)
{
	TupleSet<NUM_TEST_TUPLES, TUPLE_DATA_SIZE>
		tuples(format, TupleCompressor(), MP_DATA_TYPE_UNSIGNED);
	size_t total_count = 0;
	const size_t NUM_REFS = 1024;
	for (auto _ : state) {
		for (size_t k = 0; k < NUM_REFS; k++)
			for (size_t i = 0; i < NUM_TEST_TUPLES; i++)
				tuple_ref(tuples[i]);
		for (size_t k = 0; k < NUM_REFS; k++)
			for (size_t i = 0; i < NUM_TEST_TUPLES; i++)
				tuple_unref(tuples[i]);
		total_count += NUM_REFS * NUM_TEST_TUPLES;
	}
	state.SetItemsProcessed(total_count);
}

BENCHMARK(bench_tuple_ref_unref_high);

// struct tuple member access benchmark.
static void
tuple_access_members(benchmark::State& state)
{
	TupleSet<NUM_TEST_TUPLES, TUPLE_DATA_SIZE>
		tuples(format, TupleCompressor(), MP_DATA_TYPE_UNSIGNED);
	size_t i = 0;
	size_t total_count = 0;
	for (auto _ : state) {
		if (i == NUM_TEST_TUPLES) {
			total_count += i;
			i = 0;
		}
		struct tuple *t = tuples[i++];
		benchmark::DoNotOptimize(bool(t->is_dirty));
		benchmark::DoNotOptimize(uint16_t(t->format_id));
	}
	total_count += i;
	state.SetItemsProcessed(total_count);
}

BENCHMARK(tuple_access_members);

// tuple_data benchmark.
static void
tuple_access_data(benchmark::State& state)
{
	TupleSet<NUM_TEST_TUPLES, TUPLE_DATA_SIZE>
		tuples(format, TupleCompressor(), MP_DATA_TYPE_UNSIGNED);
	size_t i = 0;
	size_t total_count = 0;
	for (auto _ : state) {
		if (i == NUM_TEST_TUPLES) {
			total_count += i;
			i = 0;
		}
		struct tuple *t = tuples[i++];
		benchmark::DoNotOptimize(*tuple_data(t));
	}
	total_count += i;
	state.SetItemsProcessed(total_count);
}

BENCHMARK(tuple_access_data);

// tuple_data_range benchmark.
static void
tuple_access_data_range(benchmark::State& state)
{
	TupleSet<NUM_TEST_TUPLES, TUPLE_DATA_SIZE>
		tuples(format, TupleCompressor(), MP_DATA_TYPE_UNSIGNED);
	size_t i = 0;
	size_t total_count = 0;
	for (auto _ : state) {
		if (i == NUM_TEST_TUPLES) {
			total_count += i;
			i = 0;
		}
		struct tuple *t = tuples[i++];
		uint32_t size;
		benchmark::DoNotOptimize(*tuple_data_range(t, &size));
		benchmark::DoNotOptimize(size);
	}
	total_count += i;
	state.SetItemsProcessed(total_count);
}

BENCHMARK(tuple_access_data_range);

// benchmark of access of non-indexed field.
static void
tuple_access_unindexed_field(benchmark::State& state)
{
	TupleSet<NUM_TEST_TUPLES, TUPLE_DATA_SIZE>
		tuples(format, TupleCompressor(), MP_DATA_TYPE_UNSIGNED);
	size_t i = 0;
	size_t total_count = 0;
	for (auto _ : state) {
		if (i == NUM_TEST_TUPLES) {
			total_count += i;
			i = 0;
		}
		struct tuple *t = tuples[i++];
		benchmark::DoNotOptimize(*tuple_field(t, 3));
	}
	total_count += i;
	state.SetItemsProcessed(total_count);
}

BENCHMARK(tuple_access_unindexed_field);

// benchmark of access of indexed field.
static void
tuple_access_indexed_field(benchmark::State& state)
{
	TupleSet<NUM_TEST_TUPLES, TUPLE_DATA_SIZE>
		tuples(format, TupleCompressor(), MP_DATA_TYPE_UNSIGNED);
	size_t i = 0;
	size_t total_count = 0;
	for (auto _ : state) {
		if (i == NUM_TEST_TUPLES) {
			total_count += i;
			i = 0;
		}
		struct tuple *t = tuples[i];
		benchmark::DoNotOptimize(*tuple_field(t, 4));
		++i;
	}
	total_count += i;
	state.SetItemsProcessed(total_count);
}

BENCHMARK(tuple_access_indexed_field);

// benchmark of tuple compare.
static void
tuple_tuple_compare(benchmark::State& state)
{
	TupleSet<NUM_TEST_TUPLES, TUPLE_DATA_SIZE>
		tuples(format, TupleCompressor(), MP_DATA_TYPE_UNSIGNED);
	size_t i = 0;
	size_t j = 0;
	struct key_def *kd = const_cast<struct key_def *>
		(MemtxEngine::instance(format).key_def());
	size_t total_count = 0;
	for (auto _ : state) {
		if (i == NUM_TEST_TUPLES) {
			total_count += i;
			i = 0;
		}
		if (j >= NUM_TEST_TUPLES)
			j -= NUM_TEST_TUPLES;
		struct tuple *t1 = tuples[i];
		struct tuple *t2 = tuples[j];
		benchmark::DoNotOptimize(tuple_compare(t1, 0, t2, 0, kd));
		++i;
		j += 3;
	}
	total_count += i;
	state.SetItemsProcessed(total_count);
}

BENCHMARK(tuple_tuple_compare);

// benchmark of tuple hints compare.
static void
tuple_tuple_compare_hint(benchmark::State& state)
{
	TupleSet<NUM_TEST_TUPLES, TUPLE_DATA_SIZE>
		tuples(format, TupleCompressor(), MP_DATA_TYPE_UNSIGNED);
	size_t i = 0;
	size_t j = 0;
	struct key_def *kd = const_cast<struct key_def *>
		(MemtxEngine::instance(format).key_def());
	size_t total_count = 0;
	for (auto _ : state) {
		if (i == NUM_TEST_TUPLES) {
			total_count += i;
			i = 0;
		}
		if (j >= NUM_TEST_TUPLES)
			j -= NUM_TEST_TUPLES;
		struct tuple *t1 = tuples[i];
		struct tuple *t2 = tuples[j];
		hint_t h1 = tuple_hint(t1, kd);
		hint_t h2 = tuple_hint(t2, kd);
		benchmark::DoNotOptimize(tuple_compare(t1, h1, t2, h2, kd));
		++i;
		j += 3;
	}
	total_count += i;
	state.SetItemsProcessed(total_count);
}

BENCHMARK(tuple_tuple_compare_hint);

BENCHMARK_MAIN();
