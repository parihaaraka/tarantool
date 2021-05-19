#include "memory.h"
#include "fiber.h"
#include "tuple.h"

#include <benchmark/benchmark.h>

char *array_start;
char *updates_start;

static void
fill_arrays(int array_size, int updates_num, char *array_end,
	    char *updates_end, bool increase_order)
{
	mp_encode_array(array_start, array_size);
	for(int i = 0; i < array_size; ++i)
		array_end = mp_encode_uint(array_end, 100);

	updates_end = mp_encode_array(updates_start, updates_num);
	for(int i = 0; i < updates_num; ++i) {
		updates_end = mp_encode_str(updates_end, "=", 1);
		uint32_t pos;
		if (increase_order)
			pos = array_size - 2 * updates_num + 2 * i;
		else
			pos = array_size - 2 * i - 2;
		updates_end =mp_encode_uint(updates_end, pos);

		updates_end = mp_encode_str(updates_end, "=", 1);
		updates_end = mp_encode_uint(updates_end, 200);
	}
}

template<int array_size, int updates_num>
static void
bad_order(benchmark::State& state)
{
	array_start = (char *)malloc(array_size * 2 + 100);
	updates_start = (char *)malloc(updates_num * 100 + 100);
	char *array_end;
	char *updates_end;
	fill_arrays(array_size, updates_num, array_end, updates_end, false);
	struct region *gc = &fiber()->gc;
	size_t svp = region_used(gc);
	for (auto _ : state) {
		uint32_t result_size;
		uint64_t column_mask;
		xrow_update_execute(updates_start, updates_end,
					    array_start, array_end,
					    box_tuple_format_default(),
					    &result_size, 1, &column_mask);
	}
	region_truncate(gc, svp);
	free(array_start);
	free(updates_start);
}

template<int array_size, int updates_num>
static void
good_order(benchmark::State& state)
{
	array_start = (char *)malloc(array_size * 2 + 100);
	updates_start = (char *)malloc(updates_num * 100 + 100);
	char *array_end;
	char *updates_end;
	fill_arrays(array_size, updates_num, array_end, updates_end, true);
	struct region *gc = &fiber()->gc;
	size_t svp = region_used(gc);
	for (auto _ : state) {
		uint32_t result_size;
		uint64_t column_mask;
		xrow_update_execute(updates_start, updates_end,
					    array_start, array_end,
					    box_tuple_format_default(),
					    &result_size, 1, &column_mask);
	}
	region_truncate(gc, svp);
	free(array_start);
	free(updates_start);
}

BENCHMARK_TEMPLATE(bad_order,  10000000, 1000)->Name("bad_order__10000000_1000");
BENCHMARK_TEMPLATE(good_order, 10000000, 1000)->Name("good_order_10000000_1000");
BENCHMARK_TEMPLATE(bad_order,  10000000, 100)->Name("bad_order__10000000_100");
BENCHMARK_TEMPLATE(good_order, 10000000, 100)->Name("good_order_10000000_100");
BENCHMARK_TEMPLATE(bad_order,  10000000, 10)->Name("bad_order__10000000_10");
BENCHMARK_TEMPLATE(good_order, 10000000, 10)->Name("good_order_10000000_10");
BENCHMARK_TEMPLATE(bad_order,  10000000, 5)->Name("bad_order__10000000_5");
BENCHMARK_TEMPLATE(good_order, 10000000, 5)->Name("good_order_10000000_5");

BENCHMARK_TEMPLATE(bad_order,  1000000, 1000)->Name("bad_order__1000000_1000");
BENCHMARK_TEMPLATE(good_order, 1000000, 1000)->Name("good_order_1000000_1000");
BENCHMARK_TEMPLATE(bad_order,  1000000, 100)->Name("bad_order__1000000_100");
BENCHMARK_TEMPLATE(good_order, 1000000, 100)->Name("good_order_1000000_100");
BENCHMARK_TEMPLATE(bad_order,  1000000, 10)->Name("bad_order__1000000_10");
BENCHMARK_TEMPLATE(good_order, 1000000, 10)->Name("good_order_1000000_10");
BENCHMARK_TEMPLATE(bad_order,  1000000, 5)->Name("bad_order__1000000_5");
BENCHMARK_TEMPLATE(good_order, 1000000, 5)->Name("good_order_1000000_5");

BENCHMARK_TEMPLATE(bad_order,  100000, 100)->Name("bad_order__100000_100");
BENCHMARK_TEMPLATE(good_order, 100000, 100)->Name("good_order_100000_100");
BENCHMARK_TEMPLATE(bad_order,  100000, 10)->Name("bad_order__100000_10");
BENCHMARK_TEMPLATE(good_order, 100000, 10)->Name("good_order_100000_10");
BENCHMARK_TEMPLATE(bad_order,  100000, 5)->Name("bad_order__100000_5");
BENCHMARK_TEMPLATE(good_order, 100000, 5)->Name("good_order_100000_5");

BENCHMARK_TEMPLATE(bad_order,  10000, 100)->Name("bad_order__10000_100");
BENCHMARK_TEMPLATE(good_order, 10000, 100)->Name("good_order_10000_100");
BENCHMARK_TEMPLATE(bad_order,  10000, 10)->Name("bad_order__10000_10");
BENCHMARK_TEMPLATE(good_order, 10000, 10)->Name("good_order_10000_10");
BENCHMARK_TEMPLATE(bad_order,  10000, 5)->Name("bad_order__10000_5");
BENCHMARK_TEMPLATE(good_order, 10000, 5)->Name("good_order_10000_5");

BENCHMARK_TEMPLATE(bad_order,  100, 10)->Name("bad_order__100_10");
BENCHMARK_TEMPLATE(good_order, 100, 10)->Name("good_order_100_10");
BENCHMARK_TEMPLATE(bad_order,  100, 5)->Name("bad_order__100_5");
BENCHMARK_TEMPLATE(good_order, 100, 5)->Name("good_order_100_5");

int main(int argc, char **argv)
{
	memory_init();
	fiber_init(fiber_c_invoke);
	tuple_init(NULL);

	::benchmark::Initialize(&argc, argv);
	if (::benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;
	::benchmark::RunSpecifiedBenchmarks();

	tuple_free();
	fiber_free();
	memory_free();
}