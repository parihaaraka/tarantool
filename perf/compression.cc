#include "helpers.h"

#include <iterator>
#include <array>

#include <benchmark/benchmark.h>

const size_t NUM_TEST_TUPLES = 4096;
const int64_t TUPLE_SEARCH_IDX = (int64_t)uniform_distribution_n<0, NUM_TEST_TUPLES>();

template <size_t TUPLE_DATA_SIZE, bool is_compressed, bool with_settings>
static void
tuple_binary_search(benchmark::State& state)
{
	int64_t idx = state.range(0);
	enum mp_data_type type = (enum mp_data_type)state.range(1);
	TupleCompressor compressor;
	if (with_settings) {
		ZSTD_strategy strategy = (ZSTD_strategy)state.range(2);
		int level = state.range(3);
		compressor.settings(strategy, level);
	}
	TupleSet<NUM_TEST_TUPLES, TUPLE_DATA_SIZE>
		tuples(TupleFormat::default_format<1, 2, 4, 8>(),
		       compressor, type);
	tuples.sort();
	ssize_t compressed_bytes = 0;
	if (is_compressed) {
		std::for_each(tuples.begin(), tuples.end(),
			      [&compressor, &compressed_bytes]
				(box_tuple_t *&tuple)
		{
			box_tuple_t *tmp = tuple;
			tuple = compressor.try_to_compress(tmp);
			compressed_bytes += (ssize_t)tuple_bsize(tmp) -
				(ssize_t)tuple_bsize(tuple);
			tuple_unref(tmp);
			tuple_ref(tuple);
		});
	}
	if (compressed_bytes < 0) {
		state.SkipWithError("Compressed tuples are greater "
				    "than not compressed");
	}
	state.counters["compressed bytes"] =
		compressed_bytes / NUM_TEST_TUPLES;
	box_tuple_t *searchable = tuples[TUPLE_SEARCH_IDX];
	uint32_t count = 0;
	for (auto _ : state) {
		if (!tuples.search(searchable, count)) {
			state.SkipWithError("Failed to found tuple, "
					    "which should be exist");
		}

	}
	state.SetItemsProcessed(count);
}

static void
compression_settings(benchmark::internal::Benchmark* b)
{
	static std::array<int, 2> strategy = { ZSTD_fast, ZSTD_btultra2 };
	static std::array<int, 2> level = { 1, ZSTD_maxCLevel() };
	for (int type = 0; type < MP_DATA_TYPE_MAX; ++type) {
		for (int s = 0; s < strategy.size(); ++s) {
			for (int lvl = 0; lvl < level.size(); ++lvl) {
				b->Args({TUPLE_SEARCH_IDX, type,
					 strategy[s], level[lvl]});
			}
		}
	}
}
BENCHMARK_TEMPLATE(tuple_binary_search, 1024, false, false)
	->ArgsProduct({
		{TUPLE_SEARCH_IDX},
		benchmark::CreateDenseRange(0, MP_DATA_TYPE_MAX - 1, 1),
	});
BENCHMARK_TEMPLATE(tuple_binary_search, 2048, false, false)
	->ArgsProduct({
		{TUPLE_SEARCH_IDX},
		benchmark::CreateDenseRange(0, MP_DATA_TYPE_MAX - 1, 1)
	});
BENCHMARK_TEMPLATE(tuple_binary_search, 4096, false, false)
	->ArgsProduct({
		{TUPLE_SEARCH_IDX},
		benchmark::CreateDenseRange(0, MP_DATA_TYPE_MAX - 1, 1)
	});
BENCHMARK_TEMPLATE(tuple_binary_search, 1024, true, false)
	->ArgsProduct({
		{TUPLE_SEARCH_IDX},
		benchmark::CreateDenseRange(0, MP_DATA_TYPE_MAX - 1, 1)
	});
BENCHMARK_TEMPLATE(tuple_binary_search, 2048, true, false)
	->ArgsProduct({
		{TUPLE_SEARCH_IDX},
		benchmark::CreateDenseRange(0, MP_DATA_TYPE_MAX - 1, 1)
	});
BENCHMARK_TEMPLATE(tuple_binary_search, 4096, true, false)
	->ArgsProduct({
		{TUPLE_SEARCH_IDX},
		benchmark::CreateDenseRange(0, MP_DATA_TYPE_MAX - 1, 1)
	});
BENCHMARK_TEMPLATE(tuple_binary_search, 1024, true, true)
	->Apply(compression_settings);
BENCHMARK_TEMPLATE(tuple_binary_search, 2048, true, true)
	->Apply(compression_settings);
BENCHMARK_TEMPLATE(tuple_binary_search, 4096, true, true)
	->Apply(compression_settings);

BENCHMARK_MAIN();
