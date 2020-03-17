#include <errno.h>
#include <string.h>
#include <stdbool.h>

#include "erl_nif.h"
#include <erl_driver.h>

#include "khash.h"
#include "common.h"
#include "liblmdb/lmdb.h"
#include "mylog.h"

#define SUBDB_NAME_SZ 64

#define CHECK(expr, label)                                              \
    if (MDB_SUCCESS != (ret = (expr))) {                                \
    ERR_LOG("CHECK(\"%s\") failed \"%s(%d)\" at %s:%d in %s()\n",       \
            #expr, mdb_strerror(ret),ret, __FILE__, __LINE__, __func__);\
    err = enif_raise_exception(env, __strerror_term(env,ret));          \
    goto label;                                                         \
    }

#define FAIL_ERR(e, label)                                          \
    do {                                                            \
    err = enif_make_string(env, mdb_strerror(ret), ERL_NIF_LATIN1); \
    goto label;                                                     \
    } while(0)

KHASH_MAP_INIT_STR(layer, unsigned int)

static ErlNifResourceType *lmdbEnvResType;
static ErlNifResourceType *lmdbCursorResType;

typedef struct lmdb_env_s {
    MDB_env *env;

    ErlNifRWLock* layers_rwlock; // used to protect khash, not for lmdb
    khash_t(layer) *layers;
} lmdb_env_t;

static void lmdb_close(lmdb_env_t* lmdb) {
    if (lmdb) {
        DBG("close lmdb...");
        if (lmdb->layers) {
            DBG("\tfree dbnames & destroy kh");
            const char* dbname = NULL;
            MDB_dbi dbi;
            enif_rwlock_rwlock(lmdb->layers_rwlock);
            kh_foreach(lmdb->layers, dbname, dbi, {
                DBG("\tfree dbi: %s", dbname);    
                //mdb_dbi_close(lmdb->env, dbi);    
                free((void*)dbname);
            });
            kh_destroy(layer, lmdb->layers);
            enif_rwlock_rwunlock(lmdb->layers_rwlock);
            lmdb->layers = NULL;
        }
        if (lmdb->env) {
            DBG("\tclose env!");
            mdb_env_close(lmdb->env);
            lmdb->env = NULL;
        }

        if (lmdb->layers_rwlock) {
            enif_rwlock_destroy(lmdb->layers_rwlock);
            lmdb->layers_rwlock = NULL;
        }
    }
}

static void lmdb_dtor(ErlNifEnv* env, void* obj) {
    __UNUSED(env);
    INFO_LOG("destroy...... lmdb.env -> %p", obj);
    lmdb_env_t *lmdb = (lmdb_env_t*)obj;
    lmdb_close(lmdb);
}

typedef struct lmdb_cursor_s {
    MDB_cursor *cur;
    MDB_cursor_op op;
    MDB_txn *txn;
    MDB_dbi dbi;
    lmdb_env_t* lmdb;
} lmdb_cursor_t;

static void lmdb_cursor_close(lmdb_cursor_t* cursor) {
    if (cursor && cursor->cur) {
        DBG("close cursor...");
        mdb_cursor_close(cursor->cur);
        mdb_txn_commit(cursor->txn);
        mdb_dbi_close(cursor->lmdb->env, cursor->dbi);

        enif_release_resource(cursor->lmdb);
        cursor->cur = NULL;
    }
}
static void lmdb_cursor_dtor(ErlNifEnv* env, void* obj) {
    __UNUSED(env);
    INFO_LOG("destroy...... lmdb.env -> %p", obj);
    lmdb_cursor_t *cursor = (lmdb_cursor_t*)obj;
    lmdb_cursor_close(cursor);
}

static ERL_NIF_TERM elmdb_init(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    __UNUSED(argc);
    char dirname[128] = {0};
    if (!enif_get_string(env, argv[0], dirname, sizeof(dirname), ERL_NIF_LATIN1)) {
        return enif_make_badarg(env);
    }

    int ret = 0;
    ERL_NIF_TERM err;

    MDB_env *ctx;
    CHECK(mdb_env_create(&ctx), err2);
    CHECK(mdb_env_set_maxdbs(ctx, 256 - 2), err2);
    CHECK(mdb_env_set_mapsize(ctx, 10485760), err2);

    unsigned int envFlags = 0 | MDB_NOTLS; 
    CHECK(mdb_env_open(ctx, dirname, envFlags, 0664), err2);

    // Each transaction belongs to one thread.
    // The MDB_NOTLS flag changes this for read-only transactions
//    CHECK(mdb_env_set_flags(ctx, MDB_NOTLS, 1), err2);

    MDB_txn *rotxn = NULL;  // READONLY txn
    CHECK(mdb_txn_begin(ctx, NULL, MDB_RDONLY, &rotxn), err2);
    MDB_dbi dbi;
    CHECK(mdb_dbi_open(rotxn, NULL, 0, &dbi), err2);
    MDB_cursor *cursor;
    CHECK(mdb_cursor_open(rotxn, dbi, &cursor), err2);
    khash_t(layer) *layers = kh_init(layer);
    MDB_val key;
    while (mdb_cursor_get(cursor, &key, NULL, MDB_NEXT_NODUP) == 0) {
        if (memchr(key.mv_data, '\0', key.mv_size))
            continue;

        char *dbname = calloc(key.mv_size + 1, 1);  
        memcpy(dbname, key.mv_data, key.mv_size);
        MDB_dbi subdb;
        if (mdb_dbi_open(rotxn, dbname, 0, &subdb) == MDB_SUCCESS) {
            int absent = 0;
            khiter_t k = kh_put(layer, layers, dbname, &absent);
            if (absent) {
                kh_key(layers, k) = dbname;
            }
            else {
                free(dbname);
            }
            unsigned int dbiflags = 0;
            CHECK(mdb_dbi_flags(rotxn, subdb, &dbiflags), err1);
            DBG("subdb -> name=%s, dbi=%d, dbiflags=%u", dbname, subdb, dbiflags);
            kh_value(layers, k) = dbiflags;
            mdb_dbi_close(ctx, subdb);
        }
        else {
            WARN_LOG("not a dbi: %s", dbname);
        }
    }
    mdb_cursor_close(cursor);

    lmdb_env_t *handle = enif_alloc_resource(lmdbEnvResType, sizeof(*handle));
    if (handle == NULL) FAIL_ERR(ENOMEM, err1);
    mdb_txn_commit(rotxn);

    handle->env = ctx;
    handle->layers = layers;
    handle->layers_rwlock = enif_rwlock_create("khash-lock");
    ERL_NIF_TERM term = enif_make_resource(env, handle);
    enif_release_resource(handle);
    return term;

err1:
    mdb_txn_abort(rotxn);
    kh_destroy(layer, layers);
err2:
    mdb_env_close(ctx);
    return err;
}

static ERL_NIF_TERM elmdb_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    __UNUSED(argc);
    lmdb_env_t *handle = NULL;
    if (!enif_get_resource(env, argv[0], lmdbEnvResType, (void**)&handle)) {
        return enif_make_badarg(env);
    }
    
    lmdb_close(handle);
    return ATOM_OK;
}

#define CHECKOUT_ARG_FOR_DB(handle) \
    if (!enif_get_resource(env, argv[0], lmdbEnvResType, (void**)&handle)) {\
        return enif_make_badarg(env);\
    }\
    if (handle->env == NULL) {\
        return enif_raise_exception(env, enif_make_string(env, "closed lmdb", ERL_NIF_LATIN1));\
    }

static ERL_NIF_TERM elmdb_path(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    __UNUSED(argc);
    lmdb_env_t *handle = NULL;
    CHECKOUT_ARG_FOR_DB(handle);
    const char* path;
    mdb_env_get_path(handle->env, &path);
    DBG("path: %s",path);
    return enif_make_string(env, path, ERL_NIF_LATIN1);
}

static ERL_NIF_TERM elmdb_drop(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    __UNUSED(argc);
    lmdb_env_t *handle = NULL;
    CHECKOUT_ARG_FOR_DB(handle);
    
    ErlNifBinary layBin;
    if (!enif_inspect_iolist_as_binary(env, argv[1], &layBin)) {
        return enif_make_badarg(env);
    }
    char dbname[SUBDB_NAME_SZ] = {0};
    memcpy(dbname, layBin.data, layBin.size);

    enif_rwlock_rlock(handle->layers_rwlock);
    bool exist = (kh_get(layer,handle->layers, dbname) != kh_end(handle->layers));
    enif_rwlock_runlock(handle->layers_rwlock);
    if (!exist) {
        DBG("layer(sub-db: %s) NOT exist", dbname);
        return enif_raise_exception(env, 
                enif_make_tuple2(env, ATOM_DBI_NOT_FOUND, argv[1]));
    }

    int ret;
    ERL_NIF_TERM err;

    MDB_txn *txn = NULL;
    CHECK(mdb_txn_begin(handle->env, NULL, 0, &txn), err2);
    MDB_dbi dbi;
    CHECK(mdb_dbi_open(txn, dbname, 0, &dbi), err1);
    CHECK(mdb_drop(txn, dbi, 1), err1);
    CHECK(mdb_txn_commit(txn), err1);

    enif_rwlock_rwlock(handle->layers_rwlock);
    khiter_t k = kh_get(layer,handle->layers, dbname);
    kh_del(layer, handle->layers, k);
    enif_rwlock_rwunlock(handle->layers_rwlock);

    return argv[0];

err1:
    mdb_txn_abort(txn);
err2:
    return enif_raise_exception(env, err);
}

static ERL_NIF_TERM elmdb_count(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    __UNUSED(argc);
    lmdb_env_t *handle = NULL;
    CHECKOUT_ARG_FOR_DB(handle);
    
    ErlNifBinary layBin;
    if (!enif_inspect_iolist_as_binary(env, argv[1], &layBin)) {
        return enif_make_badarg(env);
    }
    char dbname[SUBDB_NAME_SZ] = {0};
    memcpy(dbname, layBin.data, layBin.size);

    enif_rwlock_rlock(handle->layers_rwlock);
    bool exist = (kh_get(layer,handle->layers, dbname) != kh_end(handle->layers));
    enif_rwlock_runlock(handle->layers_rwlock);
    if (!exist) {
        ERR_LOG("layer(sub-db: %s) NOT exist", dbname);
        return enif_make_int(env, 0);
    }

    int ret;
    ERL_NIF_TERM err;

    MDB_txn *txn = NULL;
    CHECK(mdb_txn_begin(handle->env, NULL, MDB_RDONLY, &txn), err2);
    MDB_dbi dbi;
    CHECK(mdb_dbi_open(txn, dbname, 0, &dbi), err1);
    MDB_stat mst;
    CHECK(mdb_stat(txn, dbi, &mst), err1);
    mdb_txn_abort(txn);

    return enif_make_int(env, mst.ms_entries);

err1:
    mdb_txn_abort(txn);
err2:
    return enif_raise_exception(env, err);
}

typedef struct my_key_s {
    MDB_val key;
    unsigned int type;
    union {
        ErlNifBinary keyBin;
        int64_t keyInt;
    };
} my_key_t;

static bool get_mykey_from(ErlNifEnv *env, const ERL_NIF_TERM kt, my_key_t *mykey) {
    switch (enif_term_type(env, kt)) {
        case ERL_NIF_TERM_TYPE_BITSTRING:
            if (!enif_inspect_binary(env, kt, &mykey->keyBin)) {
                return false;
            }
            mykey->key.mv_size = mykey->keyBin.size;
            mykey->key.mv_data = mykey->keyBin.data;
            return true;
        case ERL_NIF_TERM_TYPE_INTEGER:
            if (!enif_get_int64(env, kt, (ErlNifSInt64*)&mykey->keyInt)) {
                return false;
            }
            mykey->key.mv_size = sizeof(ErlNifSInt64);
            mykey->key.mv_data = &mykey->keyInt;
            mykey->type = MDB_INTEGERKEY;
            return true;
        default:
            ERR_LOG("unknow key type, only support int & string");
            return false;
    }

}

static ERL_NIF_TERM elmdb_put(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    __UNUSED(argc);
    lmdb_env_t *handle = NULL;
    CHECKOUT_ARG_FOR_DB(handle);

    const ERL_NIF_TERM* laykey = NULL;
    int arity = 0;
    if (!enif_get_tuple(env, argv[1], &arity, &laykey)) {
        return enif_make_badarg(env);
    }
    ErlNifBinary layBin;
    if (arity != 2 ||
        !enif_inspect_iolist_as_binary(env, laykey[0], &layBin)) {
        return enif_make_badarg(env);
    }

    my_key_t mykey = { };
    if (!get_mykey_from(env, laykey[1], &mykey)) {
        return enif_make_badarg(env);
    }

    ErlNifBinary valTerm;
    if (!enif_inspect_binary(env, argv[2], &valTerm)) {
        return enif_make_badarg(env);
    }

    int ret;
    ERL_NIF_TERM err;

    MDB_txn *txn = NULL;
    CHECK(mdb_txn_begin(handle->env, NULL, 0, &txn), err2);

    MDB_dbi dbi;
    char dbname[SUBDB_NAME_SZ] = {0};
    memcpy(dbname, layBin.data, layBin.size);

    unsigned int dbiFlags = 0;
    enif_rwlock_rwlock(handle->layers_rwlock);
    khiter_t it;
    if ((it=kh_get(layer,handle->layers, dbname)) == kh_end(handle->layers)) {
        DBG("the layer(%s) not found, create it.", dbname);
        dbiFlags |= mykey.type;
        int absent = 0;
        khiter_t k = kh_put(layer, handle->layers, dbname, &absent);
        if (absent) kh_key(handle->layers, k) = strndup(dbname, sizeof(dbname));
        kh_value(handle->layers, k) = dbiFlags;
        dbiFlags |= MDB_CREATE;
    }
    else {
        unsigned int flags = kh_value(handle->layers, it);
        DBG("dbiFlags: %u...for %s", flags, dbname);
        dbiFlags = flags;
        if ((mykey.type & MDB_INTEGERKEY) != (flags & MDB_INTEGERKEY)) {
            ERR_LOG("dbi_flags changed, DO NOT do this");
            err = enif_raise_exception(env, enif_make_string(env, "key type changed", ERL_NIF_LATIN1));
            goto err1;
        }
    }
    // must not be called from multiple concurrent transactions in the same process
    CHECK(mdb_dbi_open(txn, dbname, dbiFlags, &dbi), err1);
    enif_rwlock_rwunlock(handle->layers_rwlock);

    //mdb_dbi_flags(txn, dbi, &dbiFlags);

    MDB_val val;
    val.mv_size = valTerm.size;
    val.mv_data = valTerm.data;

    CHECK(mdb_put(txn, dbi, &mykey.key, &val, 0), err2);
    CHECK(mdb_txn_commit(txn), err2);
    return argv[0];

err1:
    enif_rwlock_rwunlock(handle->layers_rwlock);
err2:
    mdb_txn_abort(txn);
    return err;
}

static ERL_NIF_TERM elmdb_get(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    __UNUSED(argc);
    lmdb_env_t *handle = NULL;
    CHECKOUT_ARG_FOR_DB(handle);

    const ERL_NIF_TERM* laykey = NULL;
    int arity = 0;
    if (!enif_get_tuple(env, argv[1], &arity, &laykey)) {
        return enif_make_badarg(env);
    }
    ErlNifBinary layBin;
    if (arity != 2 ||
        !enif_inspect_iolist_as_binary(env, laykey[0], &layBin)) {
        return enif_make_badarg(env);
    }

    my_key_t mykey = { };
    if (!get_mykey_from(env, laykey[1], &mykey)) {
        return enif_make_badarg(env);
    }

    char dbname[SUBDB_NAME_SZ] = {0};
    memcpy(dbname, layBin.data, layBin.size);
    enif_rwlock_rlock(handle->layers_rwlock);
    bool exist = (kh_get(layer,handle->layers, dbname) != kh_end(handle->layers));
    enif_rwlock_runlock(handle->layers_rwlock);
    if (!exist) {
        ERR_LOG("no layer created for %s", dbname);
        return enif_raise_exception(env, 
                enif_make_tuple2(env, ATOM_DBI_NOT_FOUND, laykey[0]));
    }

    int ret;
    ERL_NIF_TERM err;

    MDB_txn *txn = NULL;
    CHECK(mdb_txn_begin(handle->env, NULL, MDB_RDONLY, &txn), err2);
    MDB_dbi dbi;
    CHECK(mdb_dbi_open(txn, dbname, 0, &dbi), err1);
    
    unsigned int ff = 0;
    CHECK(mdb_dbi_flags(txn, dbi, &ff), err1);

    MDB_val val;
    CHECK( mdb_get(txn, dbi, &mykey.key, &val), err1);
    mdb_txn_abort(txn);

    ERL_NIF_TERM res;
    unsigned char* bin = enif_make_new_binary(env, val.mv_size, &res);
    memcpy(bin, val.mv_data, val.mv_size);
    return res;

err1:
    mdb_txn_abort(txn);
err2:
    return err;
}

static ERL_NIF_TERM min_max(ErlNifEnv* env, const ERL_NIF_TERM argv[], MDB_cursor_op op) {
    lmdb_env_t *handle = NULL;
    CHECKOUT_ARG_FOR_DB(handle);

    ErlNifBinary layBin;
    if (!enif_inspect_iolist_as_binary(env, argv[1], &layBin)) {
        return enif_make_badarg(env);
    }

    char dbname[SUBDB_NAME_SZ] = {0};
    memcpy(dbname, layBin.data, layBin.size);
    enif_rwlock_rlock(handle->layers_rwlock);
    bool exist = (kh_get(layer,handle->layers, dbname) != kh_end(handle->layers));
    enif_rwlock_runlock(handle->layers_rwlock);
    if (!exist) {
        ERR_LOG("no layer created for %s", dbname);
        return enif_raise_exception(env, 
                enif_make_tuple2(env, ATOM_DBI_NOT_FOUND, argv[1]));
    }

    int ret;
    ERL_NIF_TERM err;

    MDB_txn *txn = NULL;
    CHECK(mdb_txn_begin(handle->env, NULL, 0, &txn), err4);
    MDB_dbi dbi;
    CHECK(mdb_dbi_open(txn, dbname, 0, &dbi), err3);
    DBG("open dbi: %d", dbi);
    MDB_cursor *cursor;
    CHECK(mdb_cursor_open(txn, dbi, &cursor), err2);

    MDB_val key, val;
    CHECK(mdb_cursor_get(cursor, &key, &val, op), err1);
    mdb_txn_abort(txn);
    
    ERL_NIF_TERM res;
    enif_rwlock_rlock(handle->layers_rwlock);
    khiter_t it;
    if ((it=kh_get(layer,handle->layers, dbname)) != kh_end(handle->layers)) {
        unsigned int dbflag = kh_value(handle->layers, it);
        if (dbflag & MDB_INTEGERKEY) {
            res = enif_make_int64(env, *((ErlNifSInt64*)key.mv_data));
        }
        else {
            unsigned char* ptr = enif_make_new_binary(env, key.mv_size, &res);
            memcpy(ptr, key.mv_data, key.mv_size);
        }
    }
    else {
        res = enif_raise_exception(env, 
                enif_make_tuple2(env, ATOM_DBI_NOT_FOUND, argv[1]) );
    }
    enif_rwlock_runlock(handle->layers_rwlock);
    
    return res;
err1: 
    mdb_cursor_close(cursor);
err2:
    mdb_dbi_close(handle->env, dbi);
err3:
    mdb_txn_abort(txn);
err4:
    return err;
}

static ERL_NIF_TERM elmdb_min(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    __UNUSED(argc);
    return min_max(env, argv, MDB_FIRST);
}

static ERL_NIF_TERM elmdb_max(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    __UNUSED(argc);
    return min_max(env, argv, MDB_LAST);
}

static ERL_NIF_TERM elmdb_del(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    __UNUSED(argc);
    lmdb_env_t *handle = NULL;
    CHECKOUT_ARG_FOR_DB(handle);

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

    char dbname[SUBDB_NAME_SZ] = {0};
    memcpy(dbname, layBin.data, layBin.size);
    enif_rwlock_rlock(handle->layers_rwlock);
    bool exist = (kh_get(layer,handle->layers, dbname) != kh_end(handle->layers));
    enif_rwlock_runlock(handle->layers_rwlock);
    if (!exist) {
        ERR_LOG("no layer created for %s", dbname);
        return argv[0];
    }

    int ret;
    ERL_NIF_TERM err;

    MDB_txn *txn = NULL;
    CHECK(mdb_txn_begin(handle->env, NULL, 0, &txn), err2);
    MDB_dbi dbi;
    CHECK(mdb_dbi_open(txn, dbname, 0, &dbi), err1);
    DBG("open dbi: %d", dbi);
    
    MDB_val key;
    key.mv_size = keyBin.size;
    key.mv_data = keyBin.data;
    CHECK( mdb_del(txn, dbi, &key, NULL), err1);
    mdb_txn_commit(txn);
    return argv[0];

err1:
    mdb_txn_abort(txn);
err2:
    return err;
}

static ERL_NIF_TERM elmdb_ls(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    __UNUSED(argc);
    lmdb_env_t *handle = NULL;
    CHECKOUT_ARG_FOR_DB(handle);

    ERL_NIF_TERM list = enif_make_list(env, 0);
    const char* dbname = NULL;
    MDB_dbi dbi;
    enif_rwlock_rlock(handle->layers_rwlock);
    kh_foreach(handle->layers, dbname, dbi, {
        ERL_NIF_TERM hd = enif_make_string(env, dbname, ERL_NIF_LATIN1);    
        list = enif_make_list_cell(env, hd, list);    
    });
    enif_rwlock_runlock(handle->layers_rwlock);
    return list;
}

static ERL_NIF_TERM elmdb_range(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    lmdb_env_t *handle = NULL;
    CHECKOUT_ARG_FOR_DB(handle);

    ErlNifBinary layBin;
    if (!enif_inspect_iolist_as_binary(env, argv[1], &layBin)) {
        return enif_make_badarg(env);
    }
    char dbname[SUBDB_NAME_SZ] = {0};
    memcpy(dbname, layBin.data, layBin.size);

    enif_rwlock_rlock(handle->layers_rwlock);
    bool exist = (kh_get(layer,handle->layers, dbname) != kh_end(handle->layers));
    enif_rwlock_runlock(handle->layers_rwlock);
    if (!exist) {
        ERR_LOG("no layer(sub-db) created for %s", dbname);
        return enif_raise_exception(env, 
                enif_make_tuple2(env, ATOM_DBI_NOT_FOUND, argv[1]));
    }

    int ret;
    ERL_NIF_TERM err;

    MDB_txn *txn = NULL;
    CHECK(mdb_txn_begin(handle->env, NULL, MDB_RDONLY, &txn), err2);
    MDB_dbi dbi;
    CHECK(mdb_dbi_open(txn, dbname, 0, &dbi), err2);
    DBG("open sub-db: %d for %s", dbi, dbname);
    unsigned int dbflag = 0;
    CHECK(mdb_dbi_flags(txn, dbi, &dbflag), err1);
    MDB_cursor *cur;
    CHECK(mdb_cursor_open(txn, dbi, &cur), err1);

    ERL_NIF_TERM map = enif_make_new_map(env);
    MDB_val val;

    my_key_t mykey = { };
    if (!get_mykey_from(env, argv[2], &mykey)) {
        err = enif_raise_exception(env, enif_make_string(env, "cannot extract key", ERL_NIF_LATIN1));
        goto err1;
    }
    MDB_val iterkey;
    iterkey.mv_data = mykey.key.mv_data;
    iterkey.mv_size = mykey.key.mv_size;

    my_key_t endkey = { };
    if (argc == 4) {
        if (!get_mykey_from(env, argv[3], &endkey)) {
            err = enif_raise_exception(env, enif_make_string(env, "cannot extract key", ERL_NIF_LATIN1));
            goto err1;
        }
    }
    else {
        CHECK(mdb_cursor_get(cur, &endkey.key, NULL, MDB_LAST), err1);
    }
    MDB_cursor_op op = MDB_SET_RANGE;
    while ((ret = mdb_cursor_get(cur, &iterkey, &val, op)) != MDB_NOTFOUND) {
        if (mdb_cmp(txn, dbi, &iterkey, &endkey.key) > 0)
            break;
        ERL_NIF_TERM keyTerm;
        if (dbflag & MDB_INTEGERKEY) {
            keyTerm = enif_make_int64(env, *((ErlNifSInt64*)iterkey.mv_data));
        }
        else {
            unsigned char* ptr = enif_make_new_binary(env, iterkey.mv_size, &keyTerm);
            memcpy(ptr, iterkey.mv_data, iterkey.mv_size);
        }
        ERL_NIF_TERM valTerm;
        unsigned char* ptr = enif_make_new_binary(env, val.mv_size, &valTerm);
        memcpy(ptr, val.mv_data, val.mv_size);
        enif_make_map_put(env, map, keyTerm, valTerm, &map);
        op = MDB_NEXT;
    }
    mdb_cursor_close(cur);
    mdb_dbi_close(handle->env, dbi);
    mdb_txn_abort(txn);
    return map;

err1:
    mdb_dbi_close(handle->env, dbi);
err2:
    mdb_txn_abort(txn);
    return enif_raise_exception(env, err);
}

static ERL_NIF_TERM elmdb_to_map(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    __UNUSED(argc);
    lmdb_env_t *handle = NULL;
    if (!enif_get_resource(env, argv[0], lmdbEnvResType, (void**)&handle)) {
        return enif_make_badarg(env);
    }
    if (handle->env == NULL) return enif_raise_exception(env, enif_make_string(env, "closed lmdb", ERL_NIF_LATIN1));

    ErlNifBinary layBin;
    if (!enif_inspect_iolist_as_binary(env, argv[1], &layBin)) {
        return enif_make_badarg(env);
    }
    char dbname[SUBDB_NAME_SZ] = {0};
    memcpy(dbname, layBin.data, layBin.size);

    enif_rwlock_rlock(handle->layers_rwlock);
    bool exist = (kh_get(layer,handle->layers, dbname) != kh_end(handle->layers));
    enif_rwlock_runlock(handle->layers_rwlock);
    if (!exist) {
        ERR_LOG("no layer(sub-db) created for %s", dbname);
        return enif_raise_exception(env, 
                enif_make_tuple2(env, ATOM_DBI_NOT_FOUND, argv[1]));
    }

    int ret;
    ERL_NIF_TERM err;

    MDB_txn *txn = NULL;
    CHECK(mdb_txn_begin(handle->env, NULL, MDB_RDONLY, &txn), err2);
    MDB_dbi dbi;
    CHECK(mdb_dbi_open(txn, dbname, 0, &dbi), err2);
    DBG("open sub-db: %d for %s", dbi, dbname);
    unsigned int dbflag = 0;
    CHECK(mdb_dbi_flags(txn, dbi, &dbflag), err1);
    MDB_cursor *cur;
    CHECK(mdb_cursor_open(txn, dbi, &cur), err1);

    ERL_NIF_TERM map = enif_make_new_map(env);
    MDB_val key, val;
    MDB_cursor_op op = MDB_FIRST;
    while ((ret = mdb_cursor_get(cur, &key, &val, op)) != MDB_NOTFOUND) {
        ERL_NIF_TERM keyTerm;
        if (dbflag & MDB_INTEGERKEY) {
            keyTerm = enif_make_int64(env, *((ErlNifSInt64*)key.mv_data));
        }
        else {
            unsigned char* ptr = enif_make_new_binary(env, key.mv_size, &keyTerm);
            memcpy(ptr, key.mv_data, key.mv_size);
        }
        ERL_NIF_TERM valTerm;
        unsigned char* ptr = enif_make_new_binary(env, val.mv_size, &valTerm);
        memcpy(ptr, val.mv_data, val.mv_size);
        enif_make_map_put(env, map, keyTerm, valTerm, &map);
        op = MDB_NEXT;
    }
    mdb_cursor_close(cur);
    mdb_dbi_close(handle->env, dbi);
    mdb_txn_abort(txn);
    return map;

err1:
    mdb_dbi_close(handle->env, dbi);
err2:
    mdb_txn_abort(txn);
    return enif_raise_exception(env, err);
}

static ERL_NIF_TERM elmdb_next(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    __UNUSED(argc);
    lmdb_cursor_t *cursor = NULL;
    if (!enif_get_resource(env, argv[0], lmdbCursorResType, (void**)&cursor)) {
        return enif_make_badarg(env);
    }
    if (cursor->cur == NULL) return enif_raise_exception(env, enif_make_string(env, "closed cursor", ERL_NIF_LATIN1));

    unsigned int dbflag = 0;
    int ret = 0;
    ERL_NIF_TERM err;
    CHECK(mdb_dbi_flags(cursor->txn, cursor->dbi, &dbflag), err1);
    MDB_val key, val;
    if ((ret = mdb_cursor_get(cursor->cur, &key, &val, cursor->op)) != MDB_NOTFOUND) {
        ERL_NIF_TERM keyTerm;
        if (dbflag & MDB_INTEGERKEY) {
            keyTerm = enif_make_int64(env, *((ErlNifSInt64*)key.mv_data));
        }
        else {
            unsigned char* ptr = enif_make_new_binary(env, key.mv_size, &keyTerm);
            memcpy(ptr, key.mv_data, key.mv_size);
        }
        ERL_NIF_TERM valTerm;
        unsigned char* ptr = enif_make_new_binary(env, val.mv_size, &valTerm);
        memcpy(ptr, val.mv_data, val.mv_size);

        cursor->op = MDB_NEXT;
        return enif_make_tuple2(env, keyTerm, valTerm);
    }

    return enif_make_atom(env, "end_of_table");
err1:
    return enif_raise_exception(env, err);
}

static ERL_NIF_TERM elmdb_iter(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    __UNUSED(argc);
    lmdb_env_t *handle = NULL;
    if (!enif_get_resource(env, argv[0], lmdbEnvResType, (void**)&handle)) {
        return enif_make_badarg(env);
    }
    if (handle->env == NULL) return enif_raise_exception(env, enif_make_string(env, "closed lmdb", ERL_NIF_LATIN1));

    ErlNifBinary layBin;
    if (!enif_inspect_iolist_as_binary(env, argv[1], &layBin)) {
        return enif_make_badarg(env);
    }
    char dbname[SUBDB_NAME_SZ] = {0};
    memcpy(dbname, layBin.data, layBin.size);

    enif_rwlock_rlock(handle->layers_rwlock);
    bool exist = (kh_get(layer,handle->layers, dbname) != kh_end(handle->layers));
    enif_rwlock_runlock(handle->layers_rwlock);
    if (!exist) {
        ERR_LOG("no layer(sub-db) created for %s", dbname);
        return enif_raise_exception(env, 
                enif_make_tuple2(env, ATOM_DBI_NOT_FOUND, argv[1]));
    }

    int ret;
    ERL_NIF_TERM err;

    MDB_txn *txn = NULL;
    CHECK(mdb_txn_begin(handle->env, NULL, MDB_RDONLY, &txn), err2);
    MDB_dbi dbi;
    CHECK(mdb_dbi_open(txn, dbname, 0, &dbi), err2);
    DBG("open sub-db: %d for %s", dbi, dbname);
    unsigned int dbflag = 0;
    CHECK(mdb_dbi_flags(txn, dbi, &dbflag), err1);
    MDB_cursor *cur;
    CHECK(mdb_cursor_open(txn, dbi, &cur), err1);

    lmdb_cursor_t* cursor = enif_alloc_resource(lmdbCursorResType, sizeof(*cursor));
    *cursor = (lmdb_cursor_t) {
        .cur = cur,
        .op = MDB_FIRST,
        .txn = txn,
        .dbi = dbi,
        .lmdb = handle,
    };
    enif_keep_resource(handle);

    ERL_NIF_TERM term = enif_make_resource(env, cursor);
    enif_release_resource(cursor);
    return term;

err1:
    mdb_dbi_close(handle->env, dbi);
err2:
    mdb_txn_abort(txn);
    return enif_raise_exception(env, err);
}

static ERL_NIF_TERM hello(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    __UNUSED(argc);
    __UNUSED(argv);
    __UNUSED(env);
    return ATOM_OK;
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

    lmdbEnvResType = enif_open_resource_type(env, NULL, "lmdb_env_res", lmdb_dtor,
            ERL_NIF_RT_CREATE|ERL_NIF_RT_TAKEOVER, NULL);
    lmdbCursorResType = enif_open_resource_type(env, NULL, "lmdb_cursor_res", lmdb_cursor_dtor,
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

static ErlNifFunc nif_funcs[] = {
    {"init",        1, elmdb_init,      ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"close",       1, elmdb_close,     ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"drop",        2, elmdb_drop,      ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"db_path",     1, elmdb_path,      0},
    {"count",       2, elmdb_count,     0},
    {"put",         3, elmdb_put,       0},
    {"get",         2, elmdb_get,       0},
    {"del",         2, elmdb_del,       0},
    {"minkey",      2, elmdb_min,       0},
    {"maxkey",      2, elmdb_max,       0},
    {"ls",          1, elmdb_ls,        0},
    {"range",       3, elmdb_range,     ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"range",       4, elmdb_range,     ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"to_map",      2, elmdb_to_map,    ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"iter",        2, elmdb_iter,    ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"next",        1, elmdb_next,    ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"hello",       1, hello,           0}
};

ERL_NIF_INIT(elmdb, nif_funcs, load, NULL, upgrade, unload)
