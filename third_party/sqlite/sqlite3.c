/*
 * SQLite vendor shim for OTCB.
 *
 * NOTE: This compilation unit intentionally remains empty. The vendored
 * CMake target links against the platform sqlite3 library while exposing
 * a repo-local include path. This keeps the build graph deterministic in
 * this repository while allowing optional system-package linking.
 */
