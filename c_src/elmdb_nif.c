#include <errno.h>
#include <string.h>

#include "erl_nif.h"
#include <erl_driver.h>

#include "khash.h"
#include "common.h"
#include "liblmdb/lmdb.h"
#include "mylog.h"

#define CHECK(expr, label)                      \
    if (MDB_SUCCESS != (ret = (expr))) {                \
    ERR_LOG("CHECK(\"%s\") failed \"%s(%d)\" at %s:%d in %s()\n",   \
            #expr, mdb_strerror(ret),ret, __FILE__, __LINE__, __func__);\
    err = __strerror_term(env,ret); \
    goto label;                         \
    }

#define FAIL_ERR(e, label)              \
    do {                        \
    err = enif_make_string(env, mdb_strerror(ret), ERL_NIF_LATIN1); \
    goto label;                 \
    } while(0)

KHASH_MAP_INIT_STR(layer, MDB_dbi)

static ErlNifResourceType *lmdbEnvResType;

typedef struct lmdb_env_s {
    MDB_env *env;
    khash_t(layer) *layers;
} lmdb_env_t;

static void lmdb_dtor(ErlNifEnv* __attribute__((unused)) env, void* obj) {
    INFO_LOG("destroy...... lmdb.env -> %p", obj);
    lmdb_env_t *lmdb = (lmdb_env_t*)obj;
    if (lmdb) {
        if (lmdb->env) {
            mdb_env_close(lmdb->env);
            lmdb->env = NULL;
        }
        if (lmdb->layers) {
            kh_destroy(layer, lmdb->layers);
            lmdb->layers = NULL;
        }
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
    ATOM_DBI_NOT_FOUND = enif_make_atom(env, "dbi_not_found");
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

    MDB_env *ctx;
    CHECK(mdb_env_create(&ctx), err2);
    CHECK(mdb_env_set_maxdbs(ctx, 256), err2);
    CHECK(mdb_env_set_mapsize(ctx, 10485760), err2);

    unsigned int envFlags = 0; 
    CHECK(mdb_env_open(ctx, dirname, envFlags, 0664), err2);

    // Each transaction belongs to one thread.
    // The MDB_NOTLS flag changes this for read-only transactions
//    CHECK(mdb_env_set_flags(handle->env, MDB_NOTLS, 1), err2);

    MDB_txn *txn = NULL;
    CHECK(mdb_txn_begin(ctx, NULL, MDB_RDONLY, &txn), err2);
    MDB_dbi dbi;
    CHECK(mdb_dbi_open(txn, NULL, 0, &dbi), err1);
    MDB_cursor *cursor;
    CHECK(mdb_cursor_open(txn, dbi, &cursor), err2);

    khash_t(layer) *layers = kh_init(layer);
    MDB_val key, val;
    while (mdb_cursor_get(cursor, &key, NULL, MDB_NEXT_NODUP) == 0) {
        if (memchr(key.mv_data, '\0', key.mv_size))
            continue;
        char *str = malloc(key.mv_size + 1);  
        memcpy(str, key.mv_data, key.mv_size);
        DBG("key: sz=%d, len=%d", key.mv_size, strlen(key.mv_data));
        str[key.mv_size] = '\0';
        MDB_dbi db2;
        if (mdb_dbi_open(txn, str, 0, &db2) == MDB_SUCCESS) {
            INFO_LOG("dbi --> %s => %d", str, db2);
            int absent = 0;
            khiter_t k = kh_put(layer, layers, str, &absent);
            if (absent) {
                kh_key(layers, k) = str;
            }
            else {
                free(str);
            }
            kh_value(layers, k) = 0; // TODO: some value for each layer
            mdb_dbi_close(ctx, db2);
        }
        else {
            WARN_LOG("not a dbi: %s", str);
        }
    }
    mdb_cursor_close(cursor);
    mdb_txn_abort(txn);

    lmdb_env_t *handle = enif_alloc_resource(lmdbEnvResType, sizeof(*handle));
    if (handle == NULL) FAIL_ERR(ENOMEM, err1);

    handle->env = ctx;
    handle->layers = layers;
    ERL_NIF_TERM term = enif_make_resource(env, handle);
    enif_release_resource(handle);
    return term;

err1:
    kh_destroy(layer, layers);
err2:
    mdb_env_close(ctx);
err3:
    return err;
}

static ERL_NIF_TERM elmdb_put(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    lmdb_env_t *handle = NULL;
    if (!enif_get_resource(env, argv[0], lmdbEnvResType, (void**)&handle)) {
        return enif_make_badarg(env);
    }
    
    const ERL_NIF_TERM* laykey = NULL;
    int arity = 0;
    if (!enif_get_tuple(env, argv[1], &arity, &laykey)) {
        return enif_make_badarg(env);
    }
    ErlNifBinary layBin;
    ErlNifBinary keyBin;
    if (arity != 2 ||
        !enif_inspect_iolist_as_binary(env, laykey[0], &layBin) ||
        !enif_inspect_binary(env, laykey[1], &keyBin)) {
        return enif_make_badarg(env);
    }

    int ret;
    ERL_NIF_TERM err;

    MDB_txn *txn = NULL;
    CHECK(mdb_txn_begin(handle->env, NULL, 0, &txn), err1);

    MDB_dbi dbi;
    char dbname[128] = {};
    memcpy(dbname, layBin.data, layBin.size);
    dbname[layBin.size] = '\0';
    if (kh_get(layer,handle->layers, dbname) == kh_end(handle->layers)) {
        DBG("the layer(%s) not found, create one", dbname);
        CHECK(mdb_dbi_open(txn, dbname, MDB_CREATE, &dbi), err1);
        int absent = 0;
        khiter_t k = kh_put(layer, handle->layers, dbname, &absent);
        if (absent) kh_key(handle->layers, k) = strndup(dbname, sizeof(dbname));
        kh_value(handle->layers, k) = dbi;
    }
    else {
        CHECK(mdb_dbi_open(txn, dbname, 0, &dbi), err1);
    }

    ErlNifBinary valTerm;
    if (!enif_inspect_binary(env, argv[2], &valTerm)) {
        return enif_make_badarg(env);
    }

    MDB_val key, val;
    key.mv_size = keyBin.size;
    key.mv_data = keyBin.data;
    val.mv_size = valTerm.size;
    val.mv_data = valTerm.data;

    mdb_put(txn, dbi, &key, &val, MDB_NOOVERWRITE);
    CHECK(mdb_txn_commit(txn), err2);
    return argv[0];

err1:
    mdb_txn_abort(txn);
err2:
    return err;
}

static ERL_NIF_TERM elmdb_get(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    lmdb_env_t *handle = NULL;
    if (!enif_get_resource(env, argv[0], lmdbEnvResType, (void**)&handle)) {
        return enif_make_badarg(env);
    }

    const ERL_NIF_TERM* laykey = NULL;
    int arity = 0;
    if (!enif_get_tuple(env, argv[1], &arity, &laykey)) {
        return enif_make_badarg(env);
    }
    ErlNifBinary layBin;
    ErlNifBinary keyBin;
    if (arity != 2 ||
        !enif_inspect_iolist_as_binary(env, laykey[0], &layBin) ||
        !enif_inspect_binary(env, laykey[1], &keyBin)) {
        return enif_make_badarg(env);
    }

    char dbname[128] = {};
    memcpy(dbname, layBin.data, layBin.size);
    dbname[layBin.size] = '\0';
    if (kh_get(layer,handle->layers, dbname) == kh_end(handle->layers)) {
        ERR_LOG("no layer found for %s", dbname);
        return enif_raise_exception(env, 
                enif_make_tuple2(env, ATOM_DBI_NOT_FOUND, laykey[0]));
    }

    int ret;
    ERL_NIF_TERM err;

    MDB_txn *txn = NULL;
    CHECK(mdb_txn_begin(handle->env, NULL, MDB_RDONLY, &txn), err1);
    MDB_dbi dbi;
    CHECK(mdb_dbi_open(txn, dbname, 0, &dbi), err1);
    
    MDB_val key, val;
    key.mv_size = keyBin.size;
    key.mv_data = keyBin.data;
    CHECK( mdb_get(txn, dbi, &key, &val), err1);
    mdb_txn_abort(txn);

    ERL_NIF_TERM res;
    unsigned char* bin = enif_make_new_binary(env, val.mv_size, &res);
    memcpy(bin, val.mv_data, val.mv_size);
    return res;

err1:
    mdb_txn_abort(txn);
    return err;
}

static ERL_NIF_TERM hello(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    lmdb_env_t *handle = NULL;
    if (!enif_get_resource(env, argv[0], lmdbEnvResType, (void**)&handle)) {
        return enif_make_badarg(env);
    }

    int ret;
    ERL_NIF_TERM err;

    MDB_txn *txn = NULL;
    CHECK(mdb_txn_begin(handle->env, NULL, MDB_RDONLY, &txn), err2);
    for (khiter_t k = kh_begin(handle->layers); k < kh_end(handle->layers); ++k) {
        if (kh_exist(handle->layers, k)) {
            const char* dbname = kh_key(handle->layers, k);
            INFO_LOG("dbi: [%s]  -> ", dbname);
            MDB_dbi dbi;
            CHECK(mdb_dbi_open(txn, dbname, 0, &dbi), err1);
            MDB_cursor* cursor = NULL;
            mdb_cursor_open(txn, dbi, &cursor);
            MDB_val key, value;
            int rc = 0;
            while ((rc = mdb_cursor_get(cursor, &key, &value, MDB_NEXT)) ==0) {
                DBG("key: %.*s => value: %.*s", (int)key.mv_size, (char *)key.mv_data, (int)value.mv_size, (char*)value.mv_data);
            }
            if (rc == MDB_NOTFOUND) {
                INFO_LOG("Gotcha you, last one");
            }
            else ERR_LOG("found one ret: %T", __strerror_term(env, rc));
            mdb_cursor_close(cursor);
            mdb_dbi_close(handle->env, dbi);
        }
    }
    mdb_txn_abort(txn);

    return enif_make_tuple2(env,
        enif_make_atom(env, "error"),
        enif_make_atom(env, "badarg"));

err1:
    mdb_txn_abort(txn);
err2:
    return err;
}

static ErlNifFunc nif_funcs[] = {
    {"init", 1, elmdb_init},
    {"put",  3, elmdb_put},
    {"get",  2, elmdb_get},
    {"hello", 1, hello}
};

ERL_NIF_INIT(elmdb, nif_funcs, load, NULL, upgrade, unload)
