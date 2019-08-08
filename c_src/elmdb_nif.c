#include <errno.h>

#include "erl_nif.h"
#include <erl_driver.h>

#include "common.h"
#include "liblmdb/lmdb.h"
#include "mylog.h"

#define CHECK(expr, label)                      \
    if (MDB_SUCCESS != (ret = (expr))) {                \
    ERR_LOG("CHECK(\"%s\") failed \"%s\" at %s:%d in %s()\n",   \
            #expr, mdb_strerror(ret), __FILE__, __LINE__, __func__);\
    err = __strerror_term(env,ret); \
    goto label;                         \
    }

#define FAIL_ERR(e, label)              \
    do {                        \
    err = enif_make_string(env, mdb_strerror(ret), ERL_NIF_LATIN1); \
    goto label;                 \
    } while(0)


static ErlNifResourceType *lmdbEnvResType;
static ErlNifResourceType *lmdbDbiResType;

typedef struct lmdb_env_s {
    MDB_env *env;
} lmdb_env_t;

typedef struct lmdb_dbi_s {
    char name[64];
    lmdb_env_t* lmdb_env;
    MDB_txn *txn;
    MDB_cursor *cursor;
    MDB_dbi dbi;
} lmdb_dbi_t;

static void lmdb_dtor(ErlNifEnv* __attribute__((unused)) env, void* obj) {
    INFO_LOG("destroy...... obj -> %p", obj);
    lmdb_env_t *lmdb = (lmdb_env_t*)obj;
    if (lmdb) {
        if (lmdb->env) {
            mdb_env_close(lmdb->env);
            lmdb->env = NULL;
        }
    }
}

static void lmdb_dbi_dtor(ErlNifEnv* env, void* obj) {
    __UNUSED(env);
    lmdb_dbi_t *f = (lmdb_dbi_t*)obj;
    if (f) {
        DBG("dbi: %u, name: %s", f->dbi, f->name);
        mdb_dbi_close(f->lmdb_env->env, f->dbi);
        enif_release_resource(f->lmdb_env);
    }
}
    
static int loads = 0;

static int load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
    __UNUSED(load_info);
    /* Initialize private data. */
    *priv_data = NULL;

    loads++;

    ATOM_ERROR = enif_make_atom(env, "error");
    ATOM_OK = enif_make_atom(env, "ok");
    ATOM_NOT_FOUND = enif_make_atom(env, "not_found");
    ATOM_EXISTS = enif_make_atom(env, "exists");

    ATOM_KEYEXIST = enif_make_atom(env, "key_exist");
    ATOM_NOTFOUND = enif_make_atom(env, "notfound");
    ATOM_CORRUPTED = enif_make_atom(env, "corrupted");
    ATOM_PANIC = enif_make_atom(env, "panic");
    ATOM_VERSION_MISMATCH = enif_make_atom(env, "version_mismatch");
    ATOM_MAP_FULL = enif_make_atom(env, "map_full");
    ATOM_DBS_FULL = enif_make_atom(env, "dbs_full");
    ATOM_READERS_FULL = enif_make_atom(env, "readers_full");
    ATOM_TLS_FULL = enif_make_atom(env, "tls_full");
    ATOM_TXN_FULL = enif_make_atom(env, "txn_full");
    ATOM_CURSOR_FULL = enif_make_atom(env, "cursor_full");
    ATOM_PAGE_FULL = enif_make_atom(env, "page_full");
    ATOM_MAP_RESIZED = enif_make_atom(env, "map_resized");
    ATOM_INCOMPATIBLE = enif_make_atom(env, "incompatible");
    ATOM_BAD_RSLOT = enif_make_atom(env, "bad_rslot");

    ATOM_TXN_STARTED = enif_make_atom(env, "txn_started");
    ATOM_TXN_NOT_STARTED = enif_make_atom(env, "txn_not_started");

    lmdbEnvResType = enif_open_resource_type(env, NULL, "lmdb_res", lmdb_dtor,
            ERL_NIF_RT_CREATE|ERL_NIF_RT_TAKEOVER, NULL);
    lmdbDbiResType = enif_open_resource_type(env, NULL, "lmdb_dbi_res", lmdb_dbi_dtor,
            ERL_NIF_RT_CREATE|ERL_NIF_RT_TAKEOVER, NULL);
    
    return 0;
}

static int upgrade(ErlNifEnv* env, void** priv_data, void** old_priv_data, ERL_NIF_TERM load_info) {
    __UNUSED(env);
    __UNUSED(load_info);
    /* Convert the private data to the new version. */
    *priv_data = *old_priv_data;

    loads++;

    return 0;
}

static void unload(ErlNifEnv* env, void* priv_data) {
    __UNUSED(env);
    __UNUSED(priv_data);
    if (loads == 1) {
        /* Destroy the private data. */
    }

    loads--;
}

static ERL_NIF_TERM elmdb_init(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    char dirname[128];
    if (!enif_get_string(env, argv[0], dirname, sizeof(dirname), ERL_NIF_LATIN1)) {
        return enif_make_badarg(env);
    }

    int ret = 0;
    ERL_NIF_TERM err;

    lmdb_env_t *handle = enif_alloc_resource(lmdbEnvResType, sizeof(*handle));
    if (handle == NULL) FAIL_ERR(ENOMEM, err3);

    CHECK(mdb_env_create(&(handle->env)), err2);
    CHECK(mdb_env_set_maxdbs(handle->env, 256), err2);
    CHECK(mdb_env_set_mapsize(handle->env, 10485760), err2);

    unsigned int envFlags = MDB_FIXEDMAP; 
    CHECK(mdb_env_open(handle->env, dirname, envFlags, 0664), err2);

    // Each transaction belongs to one thread.
    // The MDB_NOTLS flag changes this for read-only transactions
//    CHECK(mdb_env_set_flags(handle->env, MDB_NOTLS, 1), err2);

    ERL_NIF_TERM term = enif_make_resource(env, handle);
    enif_release_resource(handle);
    return term;

err2:
    mdb_env_close(handle->env);
err3:
    return err;
}

static ERL_NIF_TERM elmdb_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    lmdb_env_t *handle = NULL;
    if (!enif_get_resource(env, argv[0], lmdbEnvResType, (void**)&handle)) {
        return enif_make_badarg(env);
    }

    char dbname[128];
    if (!enif_get_string(env, argv[1], dbname, sizeof(dbname), ERL_NIF_LATIN1)) {
        return enif_make_badarg(env);
    }

    int ret;
    ERL_NIF_TERM err;

    MDB_dbi dbi;
    MDB_txn *txn = NULL;
    CHECK(mdb_txn_begin(handle->env, NULL, 0, &txn), err2);
    unsigned int dbFlags = MDB_CREATE;
    CHECK(mdb_dbi_open(txn, dbname, dbFlags, &dbi), err1);
    CHECK(mdb_txn_commit(txn), err1);
    lmdb_dbi_t *family = enif_alloc_resource(lmdbDbiResType, sizeof(*family));
    if (family == NULL) FAIL_ERR(ENOMEM, err2);
    *family = (lmdb_dbi_t) {
        .lmdb_env = handle,
        .dbi = dbi
    };
    ERL_NIF_TERM res = enif_make_resource(env, family);
    enif_release_resource(family);
    enif_keep_resource(handle);

    return res;

err1:
    mdb_txn_abort(txn);
err2:
    return err;
}

static ERL_NIF_TERM hello(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (enif_is_atom(env, argv[0])) {
        return enif_make_tuple2(env,
            enif_make_atom(env, "hello"),
            argv[0]);
    }

    return enif_make_tuple2(env,
        enif_make_atom(env, "error"),
        enif_make_atom(env, "badarg"));
}

static ErlNifFunc nif_funcs[] = {
    {"init", 1, elmdb_init},
    {"open", 2, elmdb_open},
    {"hello", 1, hello}
};

ERL_NIF_INIT(elmdb, nif_funcs, load, NULL, upgrade, unload)
