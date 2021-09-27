#include "tx_man.h"

#include <vector>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <benchmark/benchmark.h>

constexpr size_t TXM_BENCH_STMT_CNT = 10;
constexpr size_t TXM_BENCH_LOOP_CNT = 100000;

class TxmBenchMsgPack;
using TxmBenchMsgPackSeq = std::vector<TxmBenchMsgPack>;

class TxmBenchMsgPack {
public:
	static constexpr uint32_t TXM_BENCH_MAX_TUPLE_DATA_SIZE = 256;
	TxmBenchMsgPack() = default;

	TxmBenchMsgPack(uint64_t key, const char *value, uint32_t len = 0)
	{
		char *begin = data_, *end = begin;
		end = mp_encode_array(end, 2);
		end = mp_encode_uint(end, key);
		len = std::min(len == 0 ? (uint32_t) strlen(value) : len,
			       TXM_BENCH_MAX_TUPLE_DATA_SIZE -
			       (uint32_t)(end - begin));
		end = mp_encode_str(end, value, len);
		size_ = end - begin;
	}

	const char *
	begin() const
	{
		return data_;
	}

	const char *
	end() const
	{
		return data_ + size_;
	}

	static int
	gen_rand_mp(TxmBenchMsgPack *mp, uint64_t key, uint32_t max_val_len,
		    uint32_t val_alph_size)
	{
		char val_buf[TXM_BENCH_MAX_TUPLE_DATA_SIZE];
		max_val_len = std::min(max_val_len,
				       TXM_BENCH_MAX_TUPLE_DATA_SIZE);
		val_alph_size = std::min(val_alph_size,
					 1 + (uint32_t)('z' - 'a'));

		uint32_t val_len = 1 + rand() % max_val_len;
		char *val_end = val_buf + val_len;
		for (char *symb = val_buf; symb < val_end; ++symb)
			*symb = 'a' + rand() % val_alph_size;

		*mp = TxmBenchMsgPack(key, val_buf, val_len);

		return 0;
	}

	static int
	gen_rand_mp_seq(TxmBenchMsgPackSeq *data, uint64_t max_key,
			uint32_t max_val_len, uint32_t val_alph_size)
	{
		TxmBenchMsgPackSeq mp_seq;
		for (auto &mp : *data) {
			gen_rand_mp(&mp, rand() % max_key, max_val_len,
				    val_alph_size);
		}

		return 0;
	}

private:
	uint32_t size_ = 0;
	char data_[TXM_BENCH_MAX_TUPLE_DATA_SIZE];
};

/**
 * Insert tuples from into space with id = @a space_id;
 */
int
txm_bench_fill_space(uint32_t space_id, const TxmBenchMsgPack *data, size_t cnt)
{
	const TxmBenchMsgPack *mp_end = data + cnt;
	for (const TxmBenchMsgPack *mp = data; mp != mp_end; ++mp) {
		box_insert(space_id, mp->begin(), mp->end(), NULL);
	}

	return 0;
}

/**
 * Run non-yieldind transaction on space with id = @a space_id.
 */
int
txm_bench_run_txn_no_yield(const char *name, uint32_t space_id,
			   const TxmBenchMsgPack *data, size_t cnt)
{
	// The implementation must be consistent with the fiber_start() call
	auto fiber_f = [](va_list args) {
		const TxmBenchMsgPack *data
			= va_arg(args, const TxmBenchMsgPack *);
		size_t cnt = va_arg(args, size_t);
		int space_id = va_arg(args, uint32_t);
		if (box_txn_begin() != 0)
			return -1;
		
		const TxmBenchMsgPack *mp_end = data + cnt;
		for (const TxmBenchMsgPack *mp = data; mp != mp_end; ++mp) {
			box_replace(space_id, mp->begin(), mp->end(), NULL);
		}
		// Must commit successfully as no yields occur.
		if (box_txn_commit() != 0)
			return -1;

		return 0;
	};

	struct fiber *f = fiber_new(name, fiber_f);
	if (f == NULL)
		return -1;

	fiber_set_joinable(f, true);
	fiber_start(f, data, cnt, space_id);
	return fiber_join(f);
}

int
txm_bench_init(unsigned seed)
{
	srand(seed);
	return 0;
}

double
txm_bench_run(unsigned space_id)
{
	uint64_t cnt = 0;
	TxmBenchMsgPackSeq data(TXM_BENCH_STMT_CNT * TXM_BENCH_LOOP_CNT);
	TxmBenchMsgPack::gen_rand_mp_seq(&data, UINT64_MAX, 3, 3);
	txm_bench_fill_space(space_id, data.data(), data.size());

	fprintf(stderr, "[START BENCH]\n");
	TxmBenchMsgPack::gen_rand_mp_seq(&data, TXM_BENCH_STMT_CNT, 3, 3);
	auto start_ts = std::chrono::steady_clock::now();
	for (size_t i = 0; i < TXM_BENCH_LOOP_CNT; ++i) {
		if (txm_bench_run_txn_no_yield(
			"tx", space_id, data.data() + i * TXM_BENCH_STMT_CNT,
			TXM_BENCH_STMT_CNT) != 0)
			abort();
	}
	auto end_ts = std::chrono::steady_clock::now();
	double delta = std::chrono::duration<double>(end_ts - start_ts).count();
	fprintf(stderr, "[STOP BENCH]\n");
	fprintf(stderr, "delta = %lg\n", delta);

	return delta;
}
