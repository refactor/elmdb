#pragma once
#include <erl_nif.h>
#include <erl_driver.h>
#include "liblmdb/lmdb.h"

#ifndef __UNUSED
#define __UNUSED(v) ((void)(v))
#endif

/* Atoms (initialized in on_load) */
static ERL_NIF_TERM ATOM_ERROR;
static ERL_NIF_TERM ATOM_OK;
static ERL_NIF_TERM ATOM_NOT_FOUND;
static ERL_NIF_TERM ATOM_DBI_NOT_FOUND;
static ERL_NIF_TERM ATOM_EXISTS;
static ERL_NIF_TERM ATOM_KEYEXIST;
static ERL_NIF_TERM ATOM_NOTFOUND;
static ERL_NIF_TERM ATOM_PAGE_NOTFOUND;
static ERL_NIF_TERM ATOM_CORRUPTED;
static ERL_NIF_TERM ATOM_PANIC;
static ERL_NIF_TERM ATOM_VERSION_MISMATCH;
static ERL_NIF_TERM ATOM_KEYEXIST;
static ERL_NIF_TERM ATOM_MAP_FULL;
static ERL_NIF_TERM ATOM_DBS_FULL;
static ERL_NIF_TERM ATOM_READERS_FULL;
static ERL_NIF_TERM ATOM_TLS_FULL;
static ERL_NIF_TERM ATOM_TXN_FULL;
static ERL_NIF_TERM ATOM_CURSOR_FULL;
static ERL_NIF_TERM ATOM_PAGE_FULL;
static ERL_NIF_TERM ATOM_MAP_RESIZED;
static ERL_NIF_TERM ATOM_INCOMPATIBLE;
static ERL_NIF_TERM ATOM_BAD_RSLOT;

static ERL_NIF_TERM ATOM_TXN_STARTED;
static ERL_NIF_TERM ATOM_TXN_NOT_STARTED;

/**
 * Convenience function to generate {error, {errno, Reason}}
 *
 * env    NIF environment
 * err    number of last error
 */
static inline ERL_NIF_TERM
__strerror_term(ErlNifEnv* env, int err)
{
    ERL_NIF_TERM term = 0;

    if (err < MDB_LAST_ERRCODE && err > MDB_KEYEXIST) {
    switch (err) {
    case MDB_KEYEXIST: /** key/data pair already exists */
        term = ATOM_KEYEXIST;
        break;
    case MDB_NOTFOUND: /** key/data pair not found (EOF) */
        term = ATOM_NOTFOUND;
        break;
    case MDB_PAGE_NOTFOUND: /** Requested page not found - this usually indicate
s corruption */
        term = ATOM_PAGE_NOTFOUND;
        break;
    case MDB_CORRUPTED: /** Located page was wrong type */
        term = ATOM_CORRUPTED;
        break;
    case MDB_PANIC  : /** Update of meta page failed, probably I/O error */
        term = ATOM_PANIC;
        break;
    case MDB_VERSION_MISMATCH: /** Environment version mismatch */
        term = ATOM_VERSION_MISMATCH;
        break;
    case MDB_INVALID: /** File is not a valid MDB file */
        term = ATOM_KEYEXIST;
        break;
    case MDB_MAP_FULL: /** Environment mapsize reached */
        term = ATOM_MAP_FULL;
        break;
    case MDB_DBS_FULL: /** Environment maxdbs reached */
        term = ATOM_DBS_FULL;
        break;
    case MDB_READERS_FULL: /** Environment maxreaders reached */
        term = ATOM_READERS_FULL;
        break;
    case MDB_TLS_FULL: /** Too many TLS keys in use - Windows only */
        term = ATOM_TLS_FULL;
        break;
    case MDB_TXN_FULL: /** Txn has too many dirty pages */
        term = ATOM_TXN_FULL;
        break;
    case MDB_CURSOR_FULL: /** Cursor stack too deep - internal error */
        term = ATOM_CURSOR_FULL;
        break;
    case MDB_PAGE_FULL: /** Page has not enough space - internal error */
        term = ATOM_PAGE_FULL;
        break;
    case MDB_MAP_RESIZED: /** Database contents grew beyond environment mapsize
*/
        term = ATOM_MAP_RESIZED;
        break;
    case MDB_INCOMPATIBLE: /** Database flags changed or would change */
        term = ATOM_INCOMPATIBLE;
        break;
    case MDB_BAD_RSLOT: /** Invalid reuse of reader locktable slot */
        term = ATOM_BAD_RSLOT;
        break;
    }
    } else {
    term = enif_make_atom(env, erl_errno_id(err));
    }

    /* We return the errno value as well as the message here because the error
       message provided by strerror() for differ across platforms and/or may be
       localized to any given language (i18n).  Use the errno atom rather than
       the message when matching in Erlang.  You've been warned. */
    return enif_make_tuple(env, 2, term,
            enif_make_string(env, mdb_strerror(err), ERL_NIF_LATIN1));
}
