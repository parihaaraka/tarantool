/*
 * Copyright 2010-2016, Tarantool AUTHORS, please see AUTHORS file.
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
#include "applier.h"

#include <msgpuck.h>

#include "xlog.h"
#include "fiber.h"
#include "fiber_cond.h"
#include "coio.h"
#include "coio_buf.h"
#include "wal.h"
#include "xrow.h"
#include "replication.h"
#include "iproto_constants.h"
#include "version.h"
#include "trigger.h"
#include "xrow_io.h"
#include "error.h"
#include "errinj.h"
#include "session.h"
#include "cfg.h"
#include "schema.h"
#include "txn.h"
#include "box.h"
#include "xrow.h"
#include "scoped_guard.h"
#include "txn_limbo.h"
#include "journal.h"
#include "raft.h"

STRS(applier_state, applier_STATE);

enum {
	/**
	 * How often to log received row count. Used during join and register.
	 */
	ROWS_PER_LOG = 100000,
};

static inline void
applier_set_state(struct applier *applier, enum applier_state state)
{
	applier->state = state;
	say_debug("=> %s", applier_state_strs[state] +
		  strlen("APPLIER_"));
	trigger_run_xc(&applier->on_state, applier);
}

/**
 * Write a nice error message to log file on SocketError or ClientError
 * in applier_f().
 */
static inline void
applier_log_error(struct applier *applier, struct error *e)
{
	uint32_t errcode = box_error_code(e);
	if (applier->last_logged_errcode == errcode)
		return;
	switch (applier->state) {
	case APPLIER_CONNECT:
		say_info("can't connect to master");
		break;
	case APPLIER_CONNECTED:
	case APPLIER_READY:
		say_info("can't join/subscribe");
		break;
	case APPLIER_AUTH:
		say_info("failed to authenticate");
		break;
	case APPLIER_SYNC:
	case APPLIER_FOLLOW:
	case APPLIER_INITIAL_JOIN:
	case APPLIER_FINAL_JOIN:
		say_info("can't read row");
		break;
	default:
		break;
	}
	error_log(e);
	switch (errcode) {
	case ER_LOADING:
	case ER_CFG:
	case ER_ACCESS_DENIED:
	case ER_NO_SUCH_USER:
	case ER_SYSTEM:
	case ER_UNKNOWN_REPLICA:
	case ER_PASSWORD_MISMATCH:
	case ER_XLOG_GAP:
	case ER_TOO_EARLY_SUBSCRIBE:
	case ER_SYNC_QUORUM_TIMEOUT:
	case ER_SYNC_ROLLBACK:
		say_info("will retry every %.2lf second",
			 replication_reconnect_interval());
		break;
	default:
		break;
	}
	applier->last_logged_errcode = errcode;
}

/**
 * A helper function which switches the applier to FOLLOW state
 * if it has synchronized with its master.
 */
static inline void
applier_check_sync(struct applier *applier)
{
	/*
	 * Stay 'orphan' until appliers catch up with
	 * the remote vclock at the time of SUBSCRIBE
	 * and the lag is less than configured.
	 */
	if (applier->state == APPLIER_SYNC &&
	    applier->lag <= replication_sync_lag &&
	    vclock_compare_ignore0(&applier->remote_vclock_at_subscribe,
				   &replicaset.vclock) <= 0) {
		/* Applier is synced, switch to "follow". */
		applier_set_state(applier, APPLIER_FOLLOW);
	}
}

/*
 * Fiber function to write vclock to replication master.
 * To track connection status, replica answers master
 * with encoded vclock. In addition to DML requests,
 * master also sends heartbeat messages every
 * replication_timeout seconds (introduced in 1.7.7).
 * On such requests replica also responds with vclock.
 */
static int
applier_writer_f(va_list ap)
{
	struct applier *applier = va_arg(ap, struct applier *);
	struct ev_io io;
	coio_create(&io, applier->io.fd);

	/* ID is permanent while applier is alive */
	uint32_t replica_id = applier->instance_id;

	while (!fiber_is_cancelled()) {
		/*
		 * Tarantool >= 1.7.7 sends periodic heartbeat
		 * messages so we don't need to send ACKs every
		 * replication_timeout seconds any more.
		 */
		if (!applier->has_acks_to_send) {
			if (applier->version_id >= version_id(1, 7, 7))
				fiber_cond_wait_timeout(&applier->writer_cond,
							TIMEOUT_INFINITY);
			else
				fiber_cond_wait_timeout(&applier->writer_cond,
							replication_timeout);
		}
		/*
		 * A writer fiber is going to be awaken after a commit or
		 * a heartbeat message. So this is an appropriate place to
		 * update an applier status because the applier state could
		 * yield and doesn't fit into a commit trigger.
		 */
		applier_check_sync(applier);

		/* Send ACKs only when in FOLLOW mode ,*/
		if (applier->state != APPLIER_SYNC &&
		    applier->state != APPLIER_FOLLOW)
			continue;
		try {
			applier->has_acks_to_send = false;
			struct xrow_header xrow;
			xrow_encode_vclock(&xrow, &replicaset.vclock);
			/*
			 * For relay lag statistics we report last
			 * written transaction timestamp in tm field.
			 *
			 * If user delete the node from _cluster space,
			 * we obtain a nil pointer here.
			 */
			struct replica *r = replica_by_id(replica_id);
			if (likely(r != NULL))
				xrow.tm = r->applier_txn_last_tm;
			coio_write_xrow(&io, &xrow);
			ERROR_INJECT(ERRINJ_APPLIER_SLOW_ACK, {
				fiber_sleep(0.01);
			});
			/*
			 * Even if new ACK is requested during the
			 * write, don't send it again right away.
			 * Otherwise risk to stay in this loop for
			 * a long time.
			 */
		} catch (SocketError *e) {
			/*
			 * There is no point trying to send ACKs if
			 * the master closed its end - we would only
			 * spam the log - so exit immediately.
			 */
			if (e->saved_errno == EPIPE)
				break;
			/*
			 * Do not exit, if there is a network error,
			 * the reader fiber will reconnect for us
			 * and signal our cond afterwards.
			 */
			e->log();
		} catch (Exception *e) {
			/*
			 * Out of memory encoding the message, ignore
			 * and try again after an interval.
			 */
			e->log();
		}
		fiber_gc();
	}
	return 0;
}

static int
apply_snapshot_row(struct xrow_header *row)
{
	int rc;
	struct request request;
	if (xrow_decode_dml(row, &request, dml_request_key_map(row->type)) != 0)
		return -1;
	struct space *space = space_cache_find(request.space_id);
	if (space == NULL)
		return -1;
	struct txn *txn = txn_begin();
	if (txn == NULL)
		return -1;
	/*
	 * Do not wait for confirmation when fetching a snapshot.
	 * Master only sends confirmed rows during join.
	 */
	txn_set_flags(txn, TXN_FORCE_ASYNC);
	if (txn_begin_stmt(txn, space, request.type) != 0)
		goto rollback;
	/* no access checks here - applier always works with admin privs */
	struct tuple *unused;
	if (space_execute_dml(space, txn, &request, &unused) != 0)
		goto rollback_stmt;
	if (txn_commit_stmt(txn, &request))
		goto rollback;
	rc = txn_commit(txn);
	fiber_gc();
	return rc;
rollback_stmt:
	txn_rollback_stmt(txn);
rollback:
	txn_abort(txn);
	fiber_gc();
	return -1;
}

/**
 * Process a no-op request.
 *
 * A no-op request does not affect any space, but it
 * promotes vclock and is written to WAL.
 */
static int
process_nop(struct request *request)
{
	assert(request->type == IPROTO_NOP);
	struct txn *txn = in_txn();
	if (txn_begin_stmt(txn, NULL, request->type) != 0)
		return -1;
	return txn_commit_stmt(txn, request);
}

static int
apply_row(struct xrow_header *row)
{
	struct request request;
	assert(!iproto_type_is_synchro_request(row->type));
	if (xrow_decode_dml(row, &request, dml_request_key_map(row->type)) != 0)
		return -1;
	if (request.type == IPROTO_NOP)
		return process_nop(&request);
	struct space *space = space_cache_find(request.space_id);
	if (space == NULL)
		return -1;
	if (box_process_rw(&request, space, NULL) != 0) {
		say_error("error applying row: %s", request_str(&request));
		return -1;
	}
	return 0;
}

/**
 * Connect to a remote host and authenticate the client.
 */
void
applier_connect(struct applier *applier)
{
	struct ev_io *coio = &applier->io;
	struct ibuf *ibuf = &applier->ibuf;
	if (coio->fd >= 0)
		return;
	char greetingbuf[IPROTO_GREETING_SIZE];
	struct xrow_header row;

	struct uri *uri = &applier->uri;
	/*
	 * coio_connect() stores resolved address to \a &applier->addr
	 * on success. &applier->addr_len is a value-result argument which
	 * must be initialized to the size of associated buffer (addrstorage)
	 * before calling coio_connect(). Since coio_connect() performs
	 * DNS resolution under the hood it is theoretically possible that
	 * applier->addr_len will be different even for same uri.
	 */
	applier->addr_len = sizeof(applier->addrstorage);
	applier_set_state(applier, APPLIER_CONNECT);
	coio_connect(coio, uri, &applier->addr, &applier->addr_len);
	assert(coio->fd >= 0);
	coio_readn(coio, greetingbuf, IPROTO_GREETING_SIZE);
	applier->last_row_time = ev_monotonic_now(loop());

	/* Decode instance version and name from greeting */
	struct greeting greeting;
	if (greeting_decode(greetingbuf, &greeting) != 0)
		tnt_raise(LoggedError, ER_PROTOCOL, "Invalid greeting");

	if (strcmp(greeting.protocol, "Binary") != 0) {
		tnt_raise(LoggedError, ER_PROTOCOL,
			  "Unsupported protocol for replication");
	}

	if (applier->version_id != greeting.version_id) {
		say_info("remote master %s at %s running Tarantool %u.%u.%u",
			 tt_uuid_str(&greeting.uuid),
			 sio_strfaddr(&applier->addr, applier->addr_len),
			 version_id_major(greeting.version_id),
			 version_id_minor(greeting.version_id),
			 version_id_patch(greeting.version_id));
	}

	/* Save the remote instance version and UUID on connect. */
	applier->uuid = greeting.uuid;
	applier->version_id = greeting.version_id;

	/* Don't display previous error messages in box.info.replication */
	diag_clear(&fiber()->diag);

	/*
	 * Send an IPROTO_VOTE request to fetch the master's ballot
	 * before proceeding to "join". It will be used for leader
	 * election on bootstrap.
	 */
	xrow_encode_vote(&row);
	coio_write_xrow(coio, &row);
	coio_read_xrow(coio, ibuf, &row);
	if (row.type == IPROTO_OK) {
		xrow_decode_ballot_xc(&row, &applier->ballot);
	} else try {
		xrow_decode_error_xc(&row);
	} catch (ClientError *e) {
		if (e->errcode() != ER_UNKNOWN_REQUEST_TYPE)
			e->raise();
		/*
		 * Master isn't aware of IPROTO_VOTE request.
		 * It's OK - we can proceed without it.
		 */
	}

	applier_set_state(applier, APPLIER_CONNECTED);

	/* Detect connection to itself */
	if (tt_uuid_is_equal(&applier->uuid, &INSTANCE_UUID))
		tnt_raise(ClientError, ER_CONNECTION_TO_SELF);

	/* Perform authentication if user provided at least login */
	if (!uri->login)
		goto done;

	/* Authenticate */
	applier_set_state(applier, APPLIER_AUTH);
	xrow_encode_auth_xc(&row, greeting.salt, greeting.salt_len, uri->login,
			    uri->login_len,
			    uri->password != NULL ? uri->password : "",
			    uri->password_len);
	coio_write_xrow(coio, &row);
	coio_read_xrow(coio, ibuf, &row);
	applier->last_row_time = ev_monotonic_now(loop());
	if (row.type != IPROTO_OK)
		xrow_decode_error_xc(&row); /* auth failed */

	/* auth succeeded */
	say_info("authenticated");
done:
	applier_set_state(applier, APPLIER_READY);
}

static uint64_t
applier_wait_snapshot(struct applier *applier)
{
	struct ev_io *coio = &applier->io;
	struct ibuf *ibuf = &applier->ibuf;
	struct xrow_header row;

	/**
	 * Tarantool < 1.7.0: if JOIN is successful, there is no "OK"
	 * response, but a stream of rows from checkpoint.
	 */
	if (applier->version_id >= version_id(1, 7, 0)) {
		/* Decode JOIN/FETCH_SNAPSHOT response */
		coio_read_xrow(coio, ibuf, &row);
		if (iproto_type_is_error(row.type)) {
			xrow_decode_error_xc(&row); /* re-throw error */
		} else if (row.type != IPROTO_OK) {
			tnt_raise(ClientError, ER_UNKNOWN_REQUEST_TYPE,
				  (uint32_t) row.type);
		}
		/*
		 * Start vclock. The vclock of the checkpoint
		 * the master is sending to the replica.
		 * Used to initialize the replica's initial
		 * vclock in bootstrap_from_master()
		 */
		xrow_decode_vclock_xc(&row, &replicaset.vclock);
	}

	coio_read_xrow(coio, ibuf, &row);
	if (row.type == IPROTO_JOIN_META) {
		/* Read additional metadata. Empty at the moment. */
		do {
			coio_read_xrow(coio, ibuf, &row);
			if (iproto_type_is_error(row.type)) {
				xrow_decode_error_xc(&row);
			} else if (iproto_type_is_promote_request(row.type)) {
				struct synchro_request req;
				if (xrow_decode_synchro(&row, &req) != 0)
					diag_raise();
				txn_limbo_process(&txn_limbo, &req);
			} else if (iproto_type_is_raft_request(row.type)) {
				struct raft_request req;
				if (xrow_decode_raft(&row, &req, NULL) != 0)
					diag_raise();
				box_raft_recover(&req);
			} else if (row.type != IPROTO_JOIN_SNAPSHOT) {
				tnt_raise(ClientError, ER_UNKNOWN_REQUEST_TYPE,
					  (uint32_t)row.type);
			}
		} while (row.type != IPROTO_JOIN_SNAPSHOT);
		coio_read_xrow(coio, ibuf, &row);
	}

	/*
	 * Receive initial data.
	 */
	uint64_t row_count = 0;
	while (true) {
		applier->last_row_time = ev_monotonic_now(loop());
		if (iproto_type_is_dml(row.type)) {
			if (apply_snapshot_row(&row) != 0)
				diag_raise();
			if (++row_count % ROWS_PER_LOG == 0)
				say_info("%.1fM rows received", row_count / 1e6);
		} else if (row.type == IPROTO_OK) {
			if (applier->version_id < version_id(1, 7, 0)) {
				/*
				 * This is the start vclock if the
				 * server is 1.6. Since we have
				 * not initialized replication
				 * vclock yet, do it now. In 1.7+
				 * this vclock is not used.
				 */
				xrow_decode_vclock_xc(&row, &replicaset.vclock);
			}
			break; /* end of stream */
		} else if (iproto_type_is_error(row.type)) {
			xrow_decode_error_xc(&row);  /* rethrow error */
		} else {
			tnt_raise(ClientError, ER_UNKNOWN_REQUEST_TYPE,
				  (uint32_t) row.type);
		}
		coio_read_xrow(coio, ibuf, &row);
	}

	return row_count;
}

static void
applier_fetch_snapshot(struct applier *applier)
{
	/* Send FETCH SNAPSHOT request */
	struct ev_io *coio = &applier->io;
	struct xrow_header row;

	memset(&row, 0, sizeof(row));
	row.type = IPROTO_FETCH_SNAPSHOT;
	coio_write_xrow(coio, &row);

	applier_set_state(applier, APPLIER_FETCH_SNAPSHOT);
	applier_wait_snapshot(applier);
	applier_set_state(applier, APPLIER_FETCHED_SNAPSHOT);
	applier_set_state(applier, APPLIER_READY);
}

struct applier_tx {
	/** How many rows there are in the transaction. */
	int row_count;
	int  start_idx;
	/**
	 * Transaction rows. The rows are allocated on the applier's auxiliary
	 * buffer imediately after the tx struct. That's why we don't care about
	 * the rows' offsets. Even when the buffer is reallocated (which happens
	 * extremely rarely) the rows retain their positions relative to the
	 * transaction struct.
	 *
	 * XXX: rewrite comment.
	 */
	struct xrow_header rows[];
};

/*
 * Check that since we allocate struct applier_tx and struct xrow_header in the
 * same continuous memory chunk.
 */
static_assert(alignof(struct applier_tx) == alignof(struct xrow_header),
	      "Alignments of struct applier_tx and struct xrow_header match");

static struct applier_tx *
applier_read_tx(struct applier *applier, uint64_t *row_count,  double timeout);

static int
apply_final_join_tx(struct applier *applier, struct applier_tx *tx);

/**
 * Defragment the applier's input buffer: move its contents, if any, to the
 * beginning.
 */
static inline void
applier_ibuf_defragment(struct applier *applier)
{
	struct ibuf *ibuf = &applier->ibuf;
	size_t used = ibuf_used(ibuf);
	if (used == 0) {
		ibuf_reset(ibuf);
	} else {
		size_t cap = ibuf_capacity(ibuf);
		/*
		 * Defragment the buffer by reserving all the available space.
		 */
		ibuf_reserve(ibuf, cap - used);
	}
}

static uint64_t
applier_wait_register(struct applier *applier, uint64_t row_count)
{
	/*
	 * Tarantool < 1.7.0: there is no "final join" stage.
	 * Proceed to "subscribe" and do not finish bootstrap
	 * until replica id is received.
	 */
	if (applier->version_id < version_id(1, 7, 0))
		return row_count;

	uint64_t next_log_cnt =
		row_count + ROWS_PER_LOG - row_count % ROWS_PER_LOG;
	/*
	 * Receive final data.
	 */
	while (true) {
		struct applier_tx *tx = applier_read_tx(applier, &row_count,
							TIMEOUT_INFINITY);
		while (row_count >= next_log_cnt) {
			say_info("%.1fM rows received", next_log_cnt / 1e6);
			next_log_cnt += ROWS_PER_LOG;
		}
		if (tx->rows[0].type == IPROTO_OK) {
			/* Current vclock. This is not used now, ignore. */
			assert(tx->row_count == 1);
			break;
		}
		if (apply_final_join_tx(applier, tx) != 0)
			diag_raise();
		/* @sa applier_subscribe(). */
		applier->ibuf.rpos = applier->ibuf.xpos;
		applier_ibuf_defragment(applier);
		ibuf_reset(&applier->aux_buf);
	}

	ibuf_reset(&applier->ibuf);
	ibuf_reset(&applier->aux_buf);
	return row_count;
}

static void
applier_register(struct applier *applier, bool was_anon)
{
	/* Send REGISTER request */
	struct ev_io *coio = &applier->io;
	struct xrow_header row;

	memset(&row, 0, sizeof(row));
	/*
	 * Send this instance's current vclock together
	 * with REGISTER request.
	 */
	xrow_encode_register(&row, &INSTANCE_UUID, box_vclock);
	row.type = IPROTO_REGISTER;
	coio_write_xrow(coio, &row);

	/*
	 * Register may serve as a retry for final join. Set corresponding
	 * states to unblock anyone who's waiting for final join to start or
	 * end.
	 */
	applier_set_state(applier, was_anon ? APPLIER_REGISTER :
					      APPLIER_FINAL_JOIN);
	applier_wait_register(applier, 0);
	applier_set_state(applier, was_anon ? APPLIER_REGISTERED :
					      APPLIER_JOINED);
	applier_set_state(applier, APPLIER_READY);
}

/**
 * Execute and process JOIN request (bootstrap the instance).
 */
static void
applier_join(struct applier *applier)
{
	/* Send JOIN request */
	struct ev_io *coio = &applier->io;
	struct xrow_header row;
	uint64_t row_count;

	xrow_encode_join_xc(&row, &INSTANCE_UUID);
	coio_write_xrow(coio, &row);

	applier_set_state(applier, APPLIER_INITIAL_JOIN);

	row_count = applier_wait_snapshot(applier);

	say_info("initial data received");

	applier_set_state(applier, APPLIER_FINAL_JOIN);

	if (applier_wait_register(applier, row_count) == row_count) {
		/*
		 * We didn't receive any rows during registration.
		 * Proceed to "subscribe" and do not finish bootstrap
		 * until replica id is received.
		 */
		return;
	}

	say_info("final data received");

	applier_set_state(applier, APPLIER_JOINED);
	applier_set_state(applier, APPLIER_READY);
}

static struct xrow_header *
applier_read_tx_row(struct applier *applier, double timeout)
{
	struct ev_io *coio = &applier->io;
	struct ibuf *ibuf = &applier->ibuf;
	struct ibuf *aux_buf = &applier->aux_buf;
	size_t size = sizeof(struct xrow_header);
	struct xrow_header *row =
			(struct xrow_header *)ibuf_alloc(aux_buf, size);

	if (row == NULL)
		tnt_raise(OutOfMemory, size, "ibuf_alloc", "row");

	ERROR_INJECT_YIELD(ERRINJ_APPLIER_READ_TX_ROW_DELAY);

	coio_read_xrow_ex_timeout_xc(coio, ibuf, row, timeout);

	if (row->tm > 0)
		applier->lag = ev_now(loop()) - row->tm;
	applier->last_row_time = ev_monotonic_now(loop());
	return row;
}

static int64_t
set_next_tx_row(struct applier_tx *tx, struct xrow_header *row, int64_t tsn)
{
	if (iproto_type_is_error(row->type))
		xrow_decode_error_xc(row);

	/* Replication request. */
	if (row->replica_id >= VCLOCK_MAX) {
		/*
		 * A safety net, this can only occur if we're fed a strangely
		 * broken xlog. row->replica_id == 0, when reading heartbeats
		 * from an anonymous instance.
		 */
		tnt_raise(ClientError, ER_UNKNOWN_REPLICA,
			  int2str(row->replica_id),
			  tt_uuid_str(&REPLICASET_UUID));
	}
	if (tsn == 0) {
		/*
		 * Transaction id must be derived from the log sequence number
		 * of the first row in the transaction.
		 */
		tsn = row->tsn;
		if (row->lsn != tsn)
			tnt_raise(ClientError, ER_PROTOCOL,
				  "Transaction id must be equal to LSN of the "
				  "first row in the transaction.");
	} else if (tsn != row->tsn) {
		tnt_raise(ClientError, ER_UNSUPPORTED, "replication",
			  "interleaving transactions");
	}

	assert(row->bodycnt <= 1);
	if (row->is_commit) {
		/* Signal the caller that we've reached the tx end. */
		tsn = 0;
	}

	tx->row_count++;
	return tsn;
}

/**
 * Read one transaction from network using applier's input buffer.
 *
 * The input buffer is reused to store row bodies. The only two problems to
 * deal with are sporadic input buffer reallocation and defragmentation.
 * We have to adjust row body pointers each time any of the two occur.
 *
 * Defragmentation is done manually in between the transaction reads, so it
 * **never** happens inside this function (ibuf->rpos always points at
 * the very beginning of the ibuf).
 *
 * Speaking of buffer reallocation, it only happens during the "saturation"
 * phase, until the input buffer reaches the size big enough to hold a single
 * transaction. Moreover, each next reallocation is exponentially less likely
 * to happen, because the buffer size is doubled every time.
 */
static struct applier_tx *
applier_read_tx(struct applier *applier, uint64_t *cnt, double timeout)
{
	int64_t tsn = 0;
	struct applier_tx *tx =
		(struct applier_tx *)ibuf_alloc(&applier->aux_buf, sizeof(*tx));
	if (tx == NULL) {
		tnt_raise(OutOfMemory, sizeof(*tx), "ibuf_alloc",
			   "applier_tx");
	}
	memset(tx, 0, sizeof(*tx));
	uint64_t row_count = 0;
	do {
		const char *old_rpos = applier->ibuf.rpos;
		struct xrow_header *row = applier_read_tx_row(applier, timeout);
		/*
		 * Detect aux buf relocation and adjust tx pointer
		 * accordingly.
		 */
		if (unlikely(&tx->rows[row_count] != row)) {
			tx = container_of(row - row_count, struct applier_tx,
					  rows[0]);
		}
		/* Detect ibuf reallocation or defragmentation. */
		ssize_t delta = applier->ibuf.rpos - old_rpos;
		if (unlikely(delta != 0)) {
			for (int i = 0; i < tx->row_count; i++) {
				struct xrow_header *cur = &tx->rows[i];
				if (cur->bodycnt == 0)
					continue;
				/*
				 * The row body's offset relative to ibuf->rpos
				 * is constant, so they all were moved by the
				 * same delta as rpos.
				 */
				cur->body->iov_base =
					(char *)cur->body->iov_base + delta;
			}
		}
		tsn = set_next_tx_row(tx, row, tsn);
		++row_count;
	} while (tsn != 0);
	*cnt += tx->row_count;
	return tx;
}

static void
applier_rollback_by_wal_io(int64_t signature)
{
	/*
	 * Setup shared applier diagnostic area.
	 *
	 * FIXME: We should consider redesign this
	 * moment and instead of carrying one shared
	 * diag use per-applier diag instead all the time
	 * (which actually already present in the structure).
	 *
	 * But remember that WAL writes are asynchronous and
	 * rollback may happen a way later after it was passed to
	 * the journal engine.
	 */
	diag_set_txn_sign(signature);
	diag_set_error(&replicaset.applier.diag,
		       diag_last_error(diag_get()));

	/* Broadcast the rollback across all appliers. */
	trigger_run(&replicaset.applier.on_rollback, NULL);

	/* Rollback applier vclock to the committed one. */
	vclock_copy(&replicaset.applier.vclock, &replicaset.vclock);
}

static int
applier_txn_rollback_cb(struct trigger *trigger, void *event)
{
	(void) trigger;
	struct txn *txn = (struct txn *) event;
	/*
	 * Synchronous transaction rollback due to receiving a
	 * ROLLBACK entry is a normal event and requires no
	 * special handling.
	 */
	if (txn->signature != TXN_SIGNATURE_SYNC_ROLLBACK)
		applier_rollback_by_wal_io(txn->signature);
	return 0;
}

struct replica_cb_data {
	/** Replica ID the data belongs to. */
	uint32_t replica_id;
	/**
	 * Timestamp of a transaction to be accounted
	 * for relay lag. Usually it is a last row in
	 * a transaction.
	 */
	double txn_last_tm;
};

/** Update replica associated data once write is complete. */
static void
replica_txn_wal_write_cb(struct replica_cb_data *rcb)
{
	struct replica *r = replica_by_id(rcb->replica_id);
	if (likely(r != NULL))
		r->applier_txn_last_tm = rcb->txn_last_tm;
}

static int
applier_txn_wal_write_cb(struct trigger *trigger, void *event)
{
	(void) event;

	struct replica_cb_data *rcb =
		(struct replica_cb_data *)trigger->data;
	replica_txn_wal_write_cb(rcb);

	/* Broadcast the WAL write across all appliers. */
	trigger_run(&replicaset.applier.on_wal_write, NULL);
	return 0;
}

struct synchro_entry {
	/** Request to process when WAL write is done. */
	struct synchro_request *req;
	/** Fiber created the entry. To wakeup when WAL write is done. */
	struct fiber *owner;
	/** Replica associated data. */
	struct replica_cb_data *rcb;
	/**
	 * The base journal entry. It has unsized array and then must be the
	 * last entry in the structure. But can workaround it via a union
	 * adding the needed tail as char[].
	 */
	union {
		struct journal_entry base;
		char base_buf[sizeof(base) + sizeof(base.rows[0])];
	};
};

/**
 * Async write journal completion.
 */
static void
apply_synchro_row_cb(struct journal_entry *entry)
{
	assert(entry->complete_data != NULL);
	struct synchro_entry *synchro_entry =
		(struct synchro_entry *)entry->complete_data;
	if (entry->res < 0) {
		applier_rollback_by_wal_io(entry->res);
	} else {
		replica_txn_wal_write_cb(synchro_entry->rcb);
		txn_limbo_process(&txn_limbo, synchro_entry->req);
		trigger_run(&replicaset.applier.on_wal_write, NULL);
	}
	fiber_wakeup(synchro_entry->owner);
}

/** Process a synchro request. */
static int
apply_synchro_row(uint32_t replica_id, struct xrow_header *row)
{
	assert(iproto_type_is_synchro_request(row->type));

	struct synchro_request req;
	if (xrow_decode_synchro(row, &req) != 0)
		goto err;

	struct replica_cb_data rcb_data;
	struct synchro_entry entry;
	/*
	 * Rows array is cast from *[] to **, because otherwise g++ complains
	 * about out of array bounds access.
	 */
	struct xrow_header **rows;
	rows = entry.base.rows;
	rows[0] = row;
	journal_entry_create(&entry.base, 1, xrow_approx_len(row),
			     apply_synchro_row_cb, &entry);
	entry.req = &req;
	entry.owner = fiber();

	rcb_data.replica_id = replica_id;
	rcb_data.txn_last_tm = row->tm;
	entry.rcb = &rcb_data;

	/*
	 * The WAL write is blocking. Otherwise it might happen that a CONFIRM
	 * or ROLLBACK is sent to WAL, and it would empty the limbo, but before
	 * it is written, more transactions arrive with a different owner. They
	 * won't be able to enter the limbo due to owner ID mismatch. Hence the
	 * synchro rows must block receipt of new transactions.
	 *
	 * Don't forget to return -1 both if the journal write failed right
	 * away, and if it failed inside of WAL thread (res < 0). Otherwise the
	 * caller would propagate committed vclock to this row thinking it was
	 * a success.
	 *
	 * XXX: in theory it could be done vice-versa. The write could be made
	 * non-blocking, and instead the potentially conflicting transactions
	 * could try to wait for all the current synchro WAL writes to end
	 * before trying to commit. But that requires extra steps from the
	 * transactions side, including the async ones.
	 */
	if (journal_write(&entry.base) != 0)
		goto err;
	if (entry.base.res < 0) {
		diag_set_journal_res(entry.base.res);
		goto err;
	}
	return 0;
err:
	diag_log();
	return -1;
}

static int
applier_handle_raft(struct applier *applier, struct xrow_header *row)
{
	assert(iproto_type_is_raft_request(row->type));
	if (applier->instance_id == 0) {
		diag_set(ClientError, ER_PROTOCOL, "Can't apply a Raft request "
			 "from an instance without an ID");
		return -1;
	}

	struct raft_request req;
	struct vclock candidate_clock;
	if (xrow_decode_raft(row, &req, &candidate_clock) != 0)
		return -1;
	return box_raft_process(&req, applier->instance_id);
}

static int
apply_plain_tx(struct applier *applier, struct applier_tx *tx,
	       bool skip_conflict, bool use_triggers)
{
	/*
	 * Explicitly begin the transaction so that we can
	 * control fiber->gc life cycle and, in case of apply
	 * conflict safely access failed xrow object and allocate
	 * IPROTO_NOP on gc.
	 */
	struct txn *txn = txn_begin();
	if (txn == NULL)
		 return -1;

	for (int i = tx->start_idx; i < tx->row_count; i++) {
		struct xrow_header *row = &tx->rows[i];
		int res = apply_row(row);
		if (res != 0 && skip_conflict) {
			struct error *e = diag_last_error(diag_get());
			/*
			 * In case of ER_TUPLE_FOUND error and enabled
			 * replication_skip_conflict configuration
			 * option, skip applying the foreign row and
			 * replace it with NOP in the local write ahead
			 * log.
			 */
			if (e->type == &type_ClientError &&
			    box_error_code(e) == ER_TUPLE_FOUND) {
				diag_clear(diag_get());
				row->type = IPROTO_NOP;
				row->bodycnt = 0;
				res = apply_row(row);
			}
		}
		if (res != 0)
			goto fail;
	}

	/*
	 * We are going to commit so it's a high time to check if
	 * the current transaction has non-local effects.
	 */
	if (txn_is_distributed(txn)) {
		/*
		 * A transaction mixes remote and local rows.
		 * Local rows must be replicated back, which
		 * doesn't make sense since the master likely has
		 * new changes which local rows may overwrite.
		 * Raise an error.
		 */
		diag_set(ClientError, ER_UNSUPPORTED, "Replication",
			 "distributed transactions");
		goto fail;
	}

	if (use_triggers) {
		/* We are ready to submit txn to wal. */
		struct trigger *on_rollback, *on_wal_write;
		size_t size;
		on_rollback = region_alloc_object(&txn->region, typeof(*on_rollback),
						  &size);
		on_wal_write = region_alloc_object(&txn->region, typeof(*on_wal_write),
						   &size);
		if (on_rollback == NULL || on_wal_write == NULL) {
			diag_set(OutOfMemory, size, "region_alloc_object",
				 "on_rollback/on_wal_write");
			goto fail;
		}

		struct replica_cb_data *rcb;
		rcb = region_alloc_object(&txn->region, typeof(*rcb), &size);
		if (rcb == NULL) {
			diag_set(OutOfMemory, size, "region_alloc_object", "rcb");
			goto fail;
		}

		trigger_create(on_rollback, applier_txn_rollback_cb, NULL, NULL);
		txn_on_rollback(txn, on_rollback);

		/*
		 * We use *last* entry timestamp because ack comes up to
		 * last entry in transaction. Same time this shows more
		 * precise result because we're interested in how long
		 * transaction traversed network + remote WAL bundle before
		 * ack get received.
		 */
		rcb->replica_id = applier->instance_id;
		rcb->txn_last_tm = tx->rows[tx->row_count - 1].tm;

		trigger_create(on_wal_write, applier_txn_wal_write_cb, rcb, NULL);
		txn_on_wal_write(txn, on_wal_write);
	}

	return txn_commit_try_async(txn);
fail:
	txn_abort(txn);
	return -1;
}

/** A simpler version of applier_apply_tx() for final join stage. */
static int
apply_final_join_tx(struct applier *applier, struct applier_tx *tx)
{
	int rc = 0;
	/* WAL isn't enabled yet, so follow vclock manually. */
	vclock_follow_xrow(&replicaset.vclock, &tx->rows[tx->row_count - 1]);
	if (unlikely(iproto_type_is_synchro_request(tx->rows[0].type))) {
		assert(tx->row_count == 1);
		rc = apply_synchro_row(applier->instance_id, &tx->rows[0]);
	} else {
		rc = apply_plain_tx(applier, tx, false, false);
	}
	fiber_gc();
	return rc;
}

/**
 * When elections are enabled we must filter out synchronous rows coming
 * from an instance that fell behind the current leader. This includes
 * both synchronous tx rows and rows for txs following unconfirmed
 * synchronous transactions.
 * The rows are replaced with NOPs to preserve the vclock consistency.
 */
static void
applier_synchro_filter_tx(struct applier_tx *tx)
{
	/*
	 * XXX: in case raft is disabled, synchronous replication still works
	 * but without any filtering. That might lead to issues with
	 * unpredictable confirms after rollbacks which are supposed to be
	 * fixed by the filtering.
	 */
	if (!raft_is_enabled(box_raft()))
		return;
	struct xrow_header *first_row = &tx->rows[tx->start_idx];
	struct xrow_header *last_row = &tx->rows[tx->row_count - 1];
	/*
	 * It  may happen that we receive the instance's rows via some third
	 * node, so cannot check for applier->instance_id here.
	 */
	if (!txn_limbo_is_replica_outdated(&txn_limbo, first_row->replica_id))
		return;

	if (last_row->wait_sync)
		goto nopify;

	/*
	 * Not waiting for sync and not a synchro request - this make it already
	 * NOP or an asynchronous transaction not depending on any synchronous
	 * ones - let it go as is.
	 */
	if (!iproto_type_is_synchro_request(first_row->type))
		return;
	/*
	 * Do not NOPify promotion, otherwise won't even know who is the limbo
	 * owner now.
	 */
	if (iproto_type_is_promote_request(first_row->type))
		return;
nopify:;
	for (int i = tx->start_idx; i < tx->row_count; i++) {
		tx->rows[i].type = IPROTO_NOP;
		/*
		 * Row body will be discarded together with the remaining
		 * input.
		 */
		tx->rows[i].bodycnt = 0;
	}
}

/**
 * Apply all rows in the rows queue as a single transaction.
 *
 * Return 0 for success or -1 in case of an error.
 */
static int
applier_apply_tx(struct applier *applier, struct applier_tx *tx)
{
	assert(tx->row_count > 0);
	/*
	 * Initially we've been filtering out data if it came from
	 * an applier which instance_id doesn't match raft->leader,
	 * but this prevents from obtaining valid leader's data when
	 * it comes from intermediate node. For example a series of
	 * replica hops
	 *
	 *  master -> replica 1 -> replica 2
	 *
	 * where each replica carries master's initiated transaction
	 * in xrow->replica_id field and master's data get propagated
	 * indirectly.
	 *
	 * Finally we dropped such "sender" filtration and use transaction
	 * "initiator" filtration via xrow->replica_id only.
	 */
	struct xrow_header *first_row = &tx->rows[tx->start_idx];
	struct xrow_header *last_row = &tx->rows[tx->row_count - 1];
	struct replica *replica = replica_by_id(first_row->replica_id);
	int rc = 0;
	/*
	 * In a full mesh topology, the same set of changes
	 * may arrive via two concurrently running appliers.
	 * Hence we need a latch to strictly order all changes
	 * that belong to the same server id.
	 */
	struct latch *latch = (replica ? &replica->order_latch :
			       &replicaset.applier.order_latch);
	latch_lock(latch);
	if (vclock_get(&replicaset.applier.vclock,
		       last_row->replica_id) >= last_row->lsn) {
		goto finish;
	} else if (vclock_get(&replicaset.applier.vclock,
			      first_row->replica_id) >= first_row->lsn) {
		/*
		 * We've received part of the tx from an old
		 * instance not knowing of tx boundaries.
		 * Skip the already applied part.
		 */
		for (int i = tx->start_idx; i < tx->row_count; i++) {
			struct xrow_header *row = &tx->rows[i];
			if (row->lsn > vclock_get(&replicaset.applier.vclock,
						   row->replica_id)) {
				tx->start_idx = i;
				break;
			}
		}
	}
	applier_synchro_filter_tx(tx);
	if (unlikely(iproto_type_is_synchro_request(first_row->type))) {
		/*
		 * Synchro messages are not transactions, in terms
		 * of DML. Always sent and written isolated from
		 * each other.
		 */
		assert(tx->row_count == 1);
		rc = apply_synchro_row(applier->instance_id, first_row);
	} else {
		rc = apply_plain_tx(applier, tx, replication_skip_conflict,
				    true);
	}
	if (rc != 0)
		goto finish;

	vclock_follow(&replicaset.applier.vclock, last_row->replica_id,
		      last_row->lsn);
finish:
	latch_unlock(latch);
	fiber_gc();
	return rc;
}

/**
 * Notify the applier's write fiber that there are more ACKs to
 * send to master.
 */
static inline void
applier_signal_ack(struct applier *applier)
{
	fiber_cond_signal(&applier->writer_cond);
	applier->has_acks_to_send = true;
}

/*
 * A trigger to update an applier state after WAL write.
 */
static int
applier_on_wal_write(struct trigger *trigger, void *event)
{
	(void) event;
	struct applier *applier = (struct applier *)trigger->data;
	applier_signal_ack(applier);
	return 0;
}

/*
 * A trigger to update an applier state after a replication rollback.
 */
static int
applier_on_rollback(struct trigger *trigger, void *event)
{
	(void) event;
	struct applier *applier = (struct applier *)trigger->data;
	/* Setup a shared error. */
	if (!diag_is_empty(&replicaset.applier.diag)) {
		diag_set_error(&applier->diag,
			       diag_last_error(&replicaset.applier.diag));
	}
	/* Stop the applier fiber. */
	fiber_cancel(applier->reader);
	return 0;
}

/**
 * Execute and process SUBSCRIBE request (follow updates from a master).
 */
static void
applier_subscribe(struct applier *applier)
{
	/* Send SUBSCRIBE request */
	struct ev_io *coio = &applier->io;
	struct ibuf *ibuf = &applier->ibuf;
	struct xrow_header row;
	struct tt_uuid cluster_id = uuid_nil;

	struct vclock vclock;
	vclock_create(&vclock);
	vclock_copy(&vclock, &replicaset.vclock);

	ERROR_INJECT(ERRINJ_REPLICASET_VCLOCK, {
		vclock_create(&vclock);
	});

	/*
	 * Stop accepting local rows coming from a remote
	 * instance as soon as local WAL starts accepting writes.
	 */
	uint32_t id_filter = box_is_orphan() ? 0 : 1 << instance_id;
	xrow_encode_subscribe_xc(&row, &REPLICASET_UUID, &INSTANCE_UUID,
				 &vclock, replication_anon, id_filter);
	coio_write_xrow(coio, &row);

	/* Read SUBSCRIBE response */
	if (applier->version_id >= version_id(1, 6, 7)) {
		coio_read_xrow(coio, ibuf, &row);
		if (iproto_type_is_error(row.type)) {
			xrow_decode_error_xc(&row);  /* error */
		} else if (row.type != IPROTO_OK) {
			tnt_raise(ClientError, ER_PROTOCOL,
				  "Invalid response to SUBSCRIBE");
		}
		/*
		 * In case of successful subscribe, the server
		 * responds with its current vclock.
		 *
		 * Tarantool > 2.1.1 also sends its cluster id to
		 * the replica, and replica has to check whether
		 * its and master's cluster ids match.
		 */
		xrow_decode_subscribe_response_xc(&row, &cluster_id,
					&applier->remote_vclock_at_subscribe);
		applier->instance_id = row.replica_id;
		/*
		 * If master didn't send us its cluster id
		 * assume that it has done all the checks.
		 * In this case cluster_id will remain zero.
		 */
		if (!tt_uuid_is_nil(&cluster_id) &&
		    !tt_uuid_is_equal(&cluster_id, &REPLICASET_UUID)) {
			tnt_raise(ClientError, ER_REPLICASET_UUID_MISMATCH,
				  tt_uuid_str(&cluster_id),
				  tt_uuid_str(&REPLICASET_UUID));
		}

		say_info("subscribed");
		say_info("remote vclock %s local vclock %s",
			 vclock_to_string(&applier->remote_vclock_at_subscribe),
			 vclock_to_string(&vclock));
	}
	/*
	 * Tarantool < 1.6.7:
	 * If there is an error in subscribe, it's sent directly
	 * in response to subscribe.  If subscribe is successful,
	 * there is no "OK" response, but a stream of rows from
	 * the binary log.
	 */

	if (applier->state == APPLIER_READY) {
		/*
		 * Tarantool < 1.7.7 does not send periodic heartbeat
		 * messages so we cannot enable applier synchronization
		 * for it without risking getting stuck in the 'orphan'
		 * mode until a DML operation happens on the master.
		 */
		if (applier->version_id >= version_id(1, 7, 7))
			applier_set_state(applier, APPLIER_SYNC);
		else
			applier_set_state(applier, APPLIER_FOLLOW);
	} else {
		/*
		 * Tarantool < 1.7.0 sends replica id during
		 * "subscribe" stage. We can't finish bootstrap
		 * until it is received.
		 */
		assert(applier->state == APPLIER_FINAL_JOIN);
		assert(applier->version_id < version_id(1, 7, 0));
	}

	/* Re-enable warnings after successful execution of SUBSCRIBE */
	applier->last_logged_errcode = 0;
	if (applier->version_id >= version_id(1, 7, 4)) {
		/* Enable replication ACKs for newer servers */
		assert(applier->writer == NULL);

		char name[FIBER_NAME_MAX];
		int pos = snprintf(name, sizeof(name), "applierw/");
		uri_format(name + pos, sizeof(name) - pos, &applier->uri, false);

		applier->writer = fiber_new_xc(name, applier_writer_f);
		fiber_set_joinable(applier->writer, true);
		fiber_start(applier->writer, applier);
	}

	applier->lag = TIMEOUT_INFINITY;

	/*
	 * Register triggers to handle WAL writes and rollbacks.
	 *
	 * Note we use them for syncronous packets handling as well
	 * thus when changing make sure that synchro handling won't
	 * be broken.
	 */
	struct trigger on_wal_write;
	trigger_create(&on_wal_write, applier_on_wal_write, applier, NULL);
	trigger_add(&replicaset.applier.on_wal_write, &on_wal_write);

	struct trigger on_rollback;
	trigger_create(&on_rollback, applier_on_rollback, applier, NULL);
	trigger_add(&replicaset.applier.on_rollback, &on_rollback);

	auto trigger_guard = make_scoped_guard([&] {
		trigger_clear(&on_wal_write);
		trigger_clear(&on_rollback);
	});

	/*
	 * Process a stream of rows from the binary log.
	 */
	while (true) {
		if (applier->state == APPLIER_FINAL_JOIN &&
		    instance_id != REPLICA_ID_NIL) {
			say_info("final data received");
			applier_set_state(applier, APPLIER_JOINED);
			applier_set_state(applier, APPLIER_READY);
			applier_set_state(applier, APPLIER_FOLLOW);
		}

		uint64_t cnt;
		/*
		 * Tarantool < 1.7.7 does not send periodic heartbeat
		 * messages so we can't assume that if we haven't heard
		 * from the master for quite a while the connection is
		 * broken - the master might just be idle.
		 */
		double timeout = applier->version_id < version_id(1, 7, 7) ?
				 TIMEOUT_INFINITY :
				 replication_disconnect_timeout();

		struct applier_tx *tx = applier_read_tx(applier, &cnt, timeout);

		/*
		 * In case of an heartbeat message wake a writer up
		 * and check applier state.
		 */
		struct xrow_header *first_row = &tx->rows[0];
		raft_process_heartbeat(box_raft(), applier->instance_id);
		if (first_row->lsn == 0) {
			if (unlikely(iproto_type_is_raft_request(
							first_row->type))) {
				if (applier_handle_raft(applier,
							first_row) != 0)
					diag_raise();
			}
			applier_signal_ack(applier);
		} else if (applier_apply_tx(applier, tx) != 0) {
			diag_raise();
		}

		/* Discard processed input. */
		ibuf->rpos = ibuf->xpos;
		/*
		 * Even though this is not necessary, defragment the buffer
		 * explicitly. Otherwise the defragmentation would be triggered
		 * by one of the row reads, resulting in moving a bigger memory
		 * chunk.
		 */
		applier_ibuf_defragment(applier);
		ibuf_reset(&applier->aux_buf);
	}
}

static inline void
applier_disconnect(struct applier *applier, enum applier_state state)
{
	applier_set_state(applier, state);
	if (applier->writer != NULL) {
		fiber_cancel(applier->writer);
		fiber_join(applier->writer);
		applier->writer = NULL;
	}

	coio_close_io(loop(), &applier->io);
	/* Clear all unparsed input. */
	ibuf_reinit(&applier->ibuf);
	ibuf_reinit(&applier->aux_buf);
	fiber_gc();
}

static int
applier_f(va_list ap)
{
	struct applier *applier = va_arg(ap, struct applier *);
	/*
	 * Set correct session type for use in on_replace()
	 * triggers.
	 */
	struct session *session = session_create_on_demand();
	if (session == NULL)
		return -1;
	session_set_type(session, SESSION_TYPE_APPLIER);

	/*
	 * The instance saves replication_anon value on bootstrap.
	 * If a freshly started instance sees it has received
	 * REPLICASET_UUID but hasn't yet registered, it must be an
	 * anonymous replica, hence the default value 'true'.
	 */
	bool was_anon = true;

	/* Re-connect loop */
	while (!fiber_is_cancelled()) {
		try {
			applier_connect(applier);
			if (tt_uuid_is_nil(&REPLICASET_UUID)) {
				/*
				 * Execute JOIN if this is a bootstrap.
				 * In case of anonymous replication, don't
				 * join but just fetch master's snapshot.
				 *
				 * The join will pause the applier
				 * until WAL is created.
				 */
				was_anon = replication_anon;
				if (replication_anon)
					applier_fetch_snapshot(applier);
				else
					applier_join(applier);
			}
			if (instance_id == REPLICA_ID_NIL &&
			    !replication_anon) {
				/*
				 * The instance transitioned from anonymous or
				 * is retrying final join.
				 */
				applier_register(applier, was_anon);
			}
			applier_subscribe(applier);
			/*
			 * subscribe() has an infinite loop which
			 * is stoppable only with fiber_cancel().
			 */
			unreachable();
			return 0;
		} catch (ClientError *e) {
			if (e->errcode() == ER_CONNECTION_TO_SELF &&
			    tt_uuid_is_equal(&applier->uuid, &INSTANCE_UUID)) {
				/* Connection to itself, stop applier */
				applier_disconnect(applier, APPLIER_OFF);
				return 0;
			} else if (e->errcode() == ER_LOADING) {
				/* Autobootstrap */
				applier_log_error(applier, e);
				applier_disconnect(applier, APPLIER_LOADING);
				goto reconnect;
			} else if (e->errcode() == ER_TOO_EARLY_SUBSCRIBE) {
				/*
				 * The instance is not anonymous, and is
				 * registered, but its ID is not delivered to
				 * all the nodes in the cluster yet, and some
				 * nodes may ask to retry connection later,
				 * until they receive _cluster record of this
				 * instance. From some third node, for example.
				 */
				applier_log_error(applier, e);
				applier_disconnect(applier, APPLIER_LOADING);
				goto reconnect;
			} else if (e->errcode() == ER_SYNC_QUORUM_TIMEOUT ||
				   e->errcode() == ER_SYNC_ROLLBACK) {
				/*
				 * Join failure due to synchronous
				 * transaction rollback.
				 */
				applier_log_error(applier, e);
				applier_disconnect(applier, APPLIER_LOADING);
				goto reconnect;
			} else if (e->errcode() == ER_CFG ||
				   e->errcode() == ER_ACCESS_DENIED ||
				   e->errcode() == ER_NO_SUCH_USER ||
				   e->errcode() == ER_PASSWORD_MISMATCH) {
				/* Invalid configuration */
				applier_log_error(applier, e);
				applier_disconnect(applier, APPLIER_LOADING);
				goto reconnect;
			} else if (e->errcode() == ER_SYSTEM) {
				/* System error from master instance. */
				applier_log_error(applier, e);
				applier_disconnect(applier, APPLIER_DISCONNECTED);
				goto reconnect;
			} else {
				/* Unrecoverable errors */
				applier_log_error(applier, e);
				applier_disconnect(applier, APPLIER_STOPPED);
				return -1;
			}
		} catch (XlogGapError *e) {
			/*
			 * Xlog gap error can't be a critical error. Because it
			 * is totally normal during bootstrap. Consider the
			 * case: node1 is a leader, it is booted with vclock
			 * {1: 3}. Node2 connects and fetches snapshot of node1,
			 * it also gets vclock {1: 3}. Then node1 writes
			 * something and its vclock becomes {1: 4}. Now node3
			 * boots from node1, and gets the same vclock. Vclocks
			 * now look like this:
			 *
			 * - node1: {1: 4}, leader, has {1: 3} snap.
			 * - node2: {1: 3}, booted from node1, has only snap.
			 * - node3: {1: 4}, booted from node1, has only snap.
			 *
			 * If the cluster is a fullmesh, node2 will send
			 * subscribe requests with vclock {1: 3}. If node3
			 * receives it, it will respond with xlog gap error,
			 * because it only has a snap with {1: 4}, nothing else.
			 * In that case node2 should retry connecting to node3,
			 * and in the meantime try to get newer changes from
			 * node1.
			 */
			applier_log_error(applier, e);
			applier_disconnect(applier, APPLIER_LOADING);
			goto reconnect;
		} catch (FiberIsCancelled *e) {
			if (!diag_is_empty(&applier->diag)) {
				diag_move(&applier->diag, &fiber()->diag);
				applier_disconnect(applier, APPLIER_STOPPED);
				break;
			}
			applier_disconnect(applier, APPLIER_OFF);
			break;
		} catch (SocketError *e) {
			applier_log_error(applier, e);
			applier_disconnect(applier, APPLIER_DISCONNECTED);
			goto reconnect;
		} catch (SystemError *e) {
			applier_log_error(applier, e);
			applier_disconnect(applier, APPLIER_DISCONNECTED);
			goto reconnect;
		} catch (Exception *e) {
			applier_log_error(applier, e);
			applier_disconnect(applier, APPLIER_STOPPED);
			return -1;
		}
		/* Put fiber_sleep() out of catch block.
		 *
		 * This is done to avoid the case when two or more
		 * fibers yield inside their try/catch blocks and
		 * throw an exception. Seems like the exception unwinder
		 * uses global state inside the catch block.
		 *
		 * This could lead to incorrect exception processing
		 * and crash the program.
		 *
		 * See: https://github.com/tarantool/tarantool/issues/136
		*/
reconnect:
		fiber_sleep(replication_reconnect_interval());
	}
	return 0;
}

void
applier_start(struct applier *applier)
{
	char name[FIBER_NAME_MAX];
	assert(applier->reader == NULL);

	int pos = snprintf(name, sizeof(name), "applier/");
	uri_format(name + pos, sizeof(name) - pos, &applier->uri, false);

	struct fiber *f = fiber_new_xc(name, applier_f);
	/**
	 * So that we can safely grab the status of the
	 * fiber any time we want.
	 */
	fiber_set_joinable(f, true);
	applier->reader = f;
	fiber_start(f, applier);
}

void
applier_stop(struct applier *applier)
{
	struct fiber *f = applier->reader;
	if (f == NULL)
		return;
	fiber_cancel(f);
	fiber_join(f);
	applier_set_state(applier, APPLIER_OFF);
	applier->reader = NULL;
}

struct applier *
applier_new(const char *uri)
{
	struct applier *applier = (struct applier *)
		calloc(1, sizeof(struct applier));
	if (applier == NULL) {
		diag_set(OutOfMemory, sizeof(*applier), "malloc",
			 "struct applier");
		return NULL;
	}
	coio_create(&applier->io, -1);
	ibuf_create(&applier->ibuf, &cord()->slabc, 1024);
	ibuf_create(&applier->aux_buf, &cord()->slabc, 1024);

	/* uri_parse() sets pointers to applier->source buffer */
	snprintf(applier->source, sizeof(applier->source), "%s", uri);
	int rc = uri_parse(&applier->uri, applier->source);
	/* URI checked by box_check_replication() */
	assert(rc == 0 && applier->uri.service != NULL);
	(void) rc;

	applier->last_row_time = ev_monotonic_now(loop());
	rlist_create(&applier->on_state);
	fiber_cond_create(&applier->resume_cond);
	fiber_cond_create(&applier->writer_cond);
	diag_create(&applier->diag);

	return applier;
}

void
applier_delete(struct applier *applier)
{
	assert(applier->reader == NULL && applier->writer == NULL);
	ibuf_destroy(&applier->ibuf);
	assert(applier->io.fd == -1);
	trigger_destroy(&applier->on_state);
	diag_destroy(&applier->diag);
	free(applier);
}

void
applier_resume(struct applier *applier)
{
	assert(!fiber_is_dead(applier->reader));
	applier->is_paused = false;
	fiber_cond_signal(&applier->resume_cond);
}

void
applier_pause(struct applier *applier)
{
	/* Sleep until applier_resume() wake us up */
	assert(fiber() == applier->reader);
	assert(!applier->is_paused);
	applier->is_paused = true;
	while (applier->is_paused && !fiber_is_cancelled())
		fiber_cond_wait(&applier->resume_cond);
}

struct applier_on_state {
	struct trigger base;
	struct applier *applier;
	enum applier_state desired_state;
	struct fiber_cond wakeup;
};

static int
applier_on_state_f(struct trigger *trigger, void *event)
{
	(void) event;
	struct applier_on_state *on_state =
		container_of(trigger, struct applier_on_state, base);

	struct applier *applier = on_state->applier;

	if (applier->state != APPLIER_OFF &&
	    applier->state != APPLIER_STOPPED &&
	    applier->state != on_state->desired_state)
		return 0;

	/* Wake up waiter */
	fiber_cond_signal(&on_state->wakeup);

	applier_pause(applier);
	return 0;
}

static inline void
applier_add_on_state(struct applier *applier,
		     struct applier_on_state *trigger,
		     enum applier_state desired_state)
{
	trigger_create(&trigger->base, applier_on_state_f, NULL, NULL);
	trigger->applier = applier;
	fiber_cond_create(&trigger->wakeup);
	trigger->desired_state = desired_state;
	trigger_add(&applier->on_state, &trigger->base);
}

static inline void
applier_clear_on_state(struct applier_on_state *trigger)
{
	fiber_cond_destroy(&trigger->wakeup);
	trigger_clear(&trigger->base);
}

static inline int
applier_wait_for_state(struct applier_on_state *trigger, double timeout)
{
	struct applier *applier = trigger->applier;
	double deadline = ev_monotonic_now(loop()) + timeout;
	while (applier->state != APPLIER_OFF &&
	       applier->state != APPLIER_STOPPED &&
	       applier->state != trigger->desired_state) {
		if (fiber_cond_wait_deadline(&trigger->wakeup, deadline) != 0)
			return -1; /* ER_TIMEOUT */
	}
	if (applier->state != trigger->desired_state) {
		assert(applier->state == APPLIER_OFF ||
		       applier->state == APPLIER_STOPPED);
		/* Re-throw the original error */
		assert(!diag_is_empty(&applier->reader->diag));
		diag_move(&applier->reader->diag, &fiber()->diag);
		return -1;
	}
	return 0;
}

void
applier_resume_to_state(struct applier *applier, enum applier_state state,
			double timeout)
{
	struct applier_on_state trigger;
	applier_add_on_state(applier, &trigger, state);
	applier_resume(applier);
	int rc = applier_wait_for_state(&trigger, timeout);
	applier_clear_on_state(&trigger);
	if (rc != 0)
		diag_raise();
	assert(applier->state == state);
}
