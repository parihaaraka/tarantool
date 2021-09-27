#ifndef TX_MAN_H_INCLUDED
#define TX_MAN_H_INCLUDED

#include <module.h>
#include <msgpuck.h>

API_EXPORT int
txm_bench_init(unsigned seed);

API_EXPORT double
txm_bench_run(unsigned space_id);

#endif // TX_MAN_H_INCLUDED