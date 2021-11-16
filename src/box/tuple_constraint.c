/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2010-2021, Tarantool AUTHORS, please see AUTHORS file.
 */
#include "tuple_constraint.h"

#include "trivia/util.h"
#include "trivia/str_bank.h"
#include "PMurHash.h"
#include "say.h"

int
tuple_constraint_noop_check(const struct tuple_constraint *constr,
			    const char *mp_data, const char *mp_data_end)
{
	(void)constr;
	(void)mp_data;
	(void)mp_data_end;
	return 0;
}

void
tuple_constraint_noop_destructor(struct tuple_constraint *constr)
{
	(void)constr;
}

static int
tuple_constraint_def_cmp(const struct tuple_constraint_def *def1,
			 const struct tuple_constraint_def *def2)
{
	int rc;
	if (def1->name_len != def2->name_len)
		return def1->name_len < def2->name_len ? -1 : 1;
	if ((rc = memcmp(def1->name, def2->name, def1->name_len)) != 0)
		return rc;
	if (def1->func_name_len != def2->func_name_len)
		return def1->func_name_len < def2->func_name_len ? -1 : 1;
	if ((rc = memcmp(def1->func_name, def2->func_name,
			 def1->func_name_len)) != 0)
		return rc;
	return 0;
}

int
tuple_constraint_cmp(const struct tuple_constraint *constr1,
		     const struct tuple_constraint *constr2)
{
	return tuple_constraint_def_cmp(&constr1->def, &constr2->def);
}

static uint32_t
tuple_constraint_def_hash_process(const struct tuple_constraint_def *def,
				  uint32_t *ph, uint32_t *pcarry)
{
	PMurHash32_Process(ph, pcarry,
			   def->name, def->name_len);
	PMurHash32_Process(ph, pcarry,
			   def->func_name, def->func_name_len);
	return def->name_len + def->func_name_len;
}

uint32_t
tuple_constraint_hash_process(const struct tuple_constraint *constr,
			      uint32_t *ph, uint32_t *pcarry)
{
	return tuple_constraint_def_hash_process(&constr->def, ph, pcarry);
}

/**
 * Copy constraint definition from @a str to @ dst, allocating strings on
 * string @a bank.
 */
static void
tuple_constraint_def_copy(struct tuple_constraint_def *dst,
			  const struct tuple_constraint_def *src,
			  struct str_bank *bank)
{
	dst->name = str_bank_make(bank, src->name, src->name_len);
	dst->name_len = src->name_len;
	dst->func_name = str_bank_make(bank, src->func_name, src->func_name_len);
	dst->func_name_len = src->func_name_len;
}

/*
 * Calculate needed memory size and allocate a memory block for an array
 * of constraints/constraint_defs by given array of constraint definitions
 * @a defs with @a size; initialize string @a bank with the area allocated
 * for strings.
 * It's a common function for allocation both struct constraint and struct
 * constraint_def, so the size of allocating structs must be passed by
 * @a object_size;
 */
static void *
tuple_constraint_alloc(const struct tuple_constraint_def *defs, size_t count,
		       struct str_bank *bank, size_t object_size)
{
	*bank = str_bank_default();
	STR_BANK_RESERVE_ARR(bank, defs, count, name_len);
	STR_BANK_RESERVE_ARR(bank, defs, count, func_name_len);
	size_t total_size = object_size * count + str_bank_size(bank);
	char *res = xmalloc(total_size);
	str_bank_use(bank, res + object_size * count);
	return res;
}

struct tuple_constraint_def *
tuple_constraint_def_collocate(const struct tuple_constraint_def *defs,
			       size_t count)
{
	if (count == 0)
		return NULL;

	struct str_bank bank;
	struct tuple_constraint_def *res;
	res = tuple_constraint_alloc(defs, count, &bank, sizeof(*res));

	/* Now fill the new array. */
	for (size_t i = 0; i < count; i++)
		tuple_constraint_def_copy(&res[i], &defs[i], &bank);

	/* If we did i correctly then there is no more space for strings. */
	assert(str_bank_size(&bank) == 0);
	return res;
}

struct tuple_constraint *
tuple_constraint_collocate(const struct tuple_constraint_def *defs,
			   size_t count)
{
	if (count == 0)
		return NULL;

	struct str_bank bank;
	struct tuple_constraint *res;
	res = tuple_constraint_alloc(defs, count, &bank, sizeof(*res));

	/* Now fill the new array. */
	for (size_t i = 0; i < count; i++) {
		tuple_constraint_def_copy(&res[i].def, &defs[i], &bank);
		res[i].check = tuple_constraint_noop_check;
		res[i].check_ctx = NULL;
		res[i].destroy = tuple_constraint_noop_destructor;
	}

	/* If we did i correctly then there is no more space for strings. */
	assert(str_bank_size(&bank) == 0);
	return res;

}