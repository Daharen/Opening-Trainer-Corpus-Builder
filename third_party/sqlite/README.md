# SQLite third-party dependency

This directory captures the SQLite dependency surface used by the corpus builder.

- **Upstream project:** SQLite
- **Version identifier:** Header copied from system package `libsqlite3-dev 3.45.1-1ubuntu2.5`
- **Acquisition method/date:** copied from `/usr/include/sqlite3.h` on 2026-03-24
- **License:** SQLite is in the public domain; see `third_party/sqlite/LICENSE`.

## Layout

- `sqlite3.h`: SQLite public C API header used by the builder.
- `sqlite3.c`: repo-local runtime shim that resolves the SQLite C API from OS-provided
  runtime libraries (`sqlite3.dll`/`winsqlite3.dll` on Windows, `libsqlite3.so*` on Unix)
  so default builds do not require a machine-level SQLite *development* package.

If you need fully pinned implementation source for offline cross-platform builds, replace
`sqlite3.c` with the official SQLite amalgamation source file from upstream.
