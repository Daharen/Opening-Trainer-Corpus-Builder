/*
 * SQLite runtime shim for OTCB.
 *
 * The corpus builder vendors sqlite3.h in-repo and resolves the SQLite C API
 * at runtime so ordinary builds do not require a machine-installed development
 * package. This keeps the default path self-contained at configure time while
 * allowing explicit external linking when requested.
 */

#include "sqlite3.h"

#include <stddef.h>

#if defined(_WIN32)
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#else
#include <dlfcn.h>
#endif

typedef struct otcb_sqlite_api {
    int (*open_fn)(const char *, sqlite3 **);
    int (*close_fn)(sqlite3 *);
    int (*prepare_v2_fn)(sqlite3 *, const char *, int, sqlite3_stmt **, const char **);
    int (*finalize_fn)(sqlite3_stmt *);
    int (*exec_fn)(sqlite3 *, const char *, int (*)(void *, int, char **, char **), void *, char **);
    void (*free_fn)(void *);
    int (*bind_text_fn)(sqlite3_stmt *, int, const char *, int, void (*)(void *));
    int (*bind_int_fn)(sqlite3_stmt *, int, int);
    int (*bind_double_fn)(sqlite3_stmt *, int, double);
    int (*bind_null_fn)(sqlite3_stmt *, int);
    int (*clear_bindings_fn)(sqlite3_stmt *);
    int (*reset_fn)(sqlite3_stmt *);
    int (*step_fn)(sqlite3_stmt *);
    int (*column_int_fn)(sqlite3_stmt *, int);
    sqlite3_int64 (*last_insert_rowid_fn)(sqlite3 *);
    const char *(*errmsg_fn)(sqlite3 *);
} otcb_sqlite_api;

static otcb_sqlite_api g_api;
static int g_api_loaded = 0;
static const char g_sqlite_error_message[] = "SQLite runtime library is unavailable";

static int otcb_load_sqlite_api(void) {
    if (g_api_loaded) {
        return 1;
    }

#if defined(_WIN32)
    HMODULE module = LoadLibraryA("sqlite3.dll");
    if (module == NULL) {
        module = LoadLibraryA("winsqlite3.dll");
    }
    if (module == NULL) {
        return 0;
    }
#define OTCB_SQLITE_SYM(field, symbol) g_api.field##_fn = (void *)GetProcAddress(module, symbol)
#else
    void *module = dlopen("libsqlite3.so.0", RTLD_NOW | RTLD_LOCAL);
    if (module == NULL) {
        module = dlopen("libsqlite3.so", RTLD_NOW | RTLD_LOCAL);
    }
    if (module == NULL) {
        return 0;
    }
#define OTCB_SQLITE_SYM(field, symbol) g_api.field##_fn = dlsym(module, symbol)
#endif

    OTCB_SQLITE_SYM(open, "sqlite3_open");
    OTCB_SQLITE_SYM(close, "sqlite3_close");
    OTCB_SQLITE_SYM(prepare_v2, "sqlite3_prepare_v2");
    OTCB_SQLITE_SYM(finalize, "sqlite3_finalize");
    OTCB_SQLITE_SYM(exec, "sqlite3_exec");
    OTCB_SQLITE_SYM(free, "sqlite3_free");
    OTCB_SQLITE_SYM(bind_text, "sqlite3_bind_text");
    OTCB_SQLITE_SYM(bind_int, "sqlite3_bind_int");
    OTCB_SQLITE_SYM(bind_double, "sqlite3_bind_double");
    OTCB_SQLITE_SYM(bind_null, "sqlite3_bind_null");
    OTCB_SQLITE_SYM(clear_bindings, "sqlite3_clear_bindings");
    OTCB_SQLITE_SYM(reset, "sqlite3_reset");
    OTCB_SQLITE_SYM(step, "sqlite3_step");
    OTCB_SQLITE_SYM(column_int, "sqlite3_column_int");
    OTCB_SQLITE_SYM(last_insert_rowid, "sqlite3_last_insert_rowid");
    OTCB_SQLITE_SYM(errmsg, "sqlite3_errmsg");

#undef OTCB_SQLITE_SYM

    if (g_api.open_fn == NULL || g_api.close_fn == NULL || g_api.prepare_v2_fn == NULL ||
        g_api.finalize_fn == NULL || g_api.exec_fn == NULL || g_api.free_fn == NULL ||
        g_api.bind_text_fn == NULL || g_api.bind_int_fn == NULL || g_api.bind_double_fn == NULL ||
        g_api.bind_null_fn == NULL || g_api.clear_bindings_fn == NULL || g_api.reset_fn == NULL ||
        g_api.step_fn == NULL || g_api.column_int_fn == NULL || g_api.last_insert_rowid_fn == NULL ||
        g_api.errmsg_fn == NULL) {
        return 0;
    }

    g_api_loaded = 1;
    return 1;
}

int sqlite3_open(const char *filename, sqlite3 **ppDb) {
    if (!otcb_load_sqlite_api()) {
        if (ppDb != NULL) {
            *ppDb = NULL;
        }
        return SQLITE_CANTOPEN;
    }
    return g_api.open_fn(filename, ppDb);
}

int sqlite3_close(sqlite3 *db) {
    if (!otcb_load_sqlite_api()) {
        return SQLITE_MISUSE;
    }
    return g_api.close_fn(db);
}

int sqlite3_prepare_v2(sqlite3 *db, const char *zSql, int nByte, sqlite3_stmt **ppStmt, const char **pzTail) {
    if (!otcb_load_sqlite_api()) {
        if (ppStmt != NULL) {
            *ppStmt = NULL;
        }
        if (pzTail != NULL) {
            *pzTail = zSql;
        }
        return SQLITE_MISUSE;
    }
    return g_api.prepare_v2_fn(db, zSql, nByte, ppStmt, pzTail);
}

int sqlite3_finalize(sqlite3_stmt *pStmt) {
    if (!otcb_load_sqlite_api()) {
        return SQLITE_MISUSE;
    }
    return g_api.finalize_fn(pStmt);
}

int sqlite3_exec(
    sqlite3 *db,
    const char *sql,
    int (*callback)(void *, int, char **, char **),
    void *arg,
    char **errmsg) {
    if (!otcb_load_sqlite_api()) {
        if (errmsg != NULL) {
            *errmsg = NULL;
        }
        return SQLITE_MISUSE;
    }
    return g_api.exec_fn(db, sql, callback, arg, errmsg);
}

void sqlite3_free(void *p) {
    if (!otcb_load_sqlite_api()) {
        return;
    }
    g_api.free_fn(p);
}

int sqlite3_bind_text(sqlite3_stmt *stmt, int idx, const char *value, int n, void (*dtor)(void *)) {
    if (!otcb_load_sqlite_api()) {
        return SQLITE_MISUSE;
    }
    return g_api.bind_text_fn(stmt, idx, value, n, dtor);
}

int sqlite3_bind_int(sqlite3_stmt *stmt, int idx, int value) {
    if (!otcb_load_sqlite_api()) {
        return SQLITE_MISUSE;
    }
    return g_api.bind_int_fn(stmt, idx, value);
}

int sqlite3_bind_double(sqlite3_stmt *stmt, int idx, double value) {
    if (!otcb_load_sqlite_api()) {
        return SQLITE_MISUSE;
    }
    return g_api.bind_double_fn(stmt, idx, value);
}

int sqlite3_bind_null(sqlite3_stmt *stmt, int idx) {
    if (!otcb_load_sqlite_api()) {
        return SQLITE_MISUSE;
    }
    return g_api.bind_null_fn(stmt, idx);
}

int sqlite3_clear_bindings(sqlite3_stmt *stmt) {
    if (!otcb_load_sqlite_api()) {
        return SQLITE_MISUSE;
    }
    return g_api.clear_bindings_fn(stmt);
}

int sqlite3_reset(sqlite3_stmt *stmt) {
    if (!otcb_load_sqlite_api()) {
        return SQLITE_MISUSE;
    }
    return g_api.reset_fn(stmt);
}

int sqlite3_step(sqlite3_stmt *stmt) {
    if (!otcb_load_sqlite_api()) {
        return SQLITE_MISUSE;
    }
    return g_api.step_fn(stmt);
}

int sqlite3_column_int(sqlite3_stmt *stmt, int iCol) {
    if (!otcb_load_sqlite_api()) {
        return 0;
    }
    return g_api.column_int_fn(stmt, iCol);
}

sqlite3_int64 sqlite3_last_insert_rowid(sqlite3 *db) {
    if (!otcb_load_sqlite_api()) {
        return 0;
    }
    return g_api.last_insert_rowid_fn(db);
}

const char *sqlite3_errmsg(sqlite3 *db) {
    if (!otcb_load_sqlite_api()) {
        (void)db;
        return g_sqlite_error_message;
    }
    return g_api.errmsg_fn(db);
}
