#ifndef DUCKDB_CURSOR_H
#define DUCKDB_CURSOR_H
#include "Python.h"
#include "connection.h"
#include "module.h"

typedef struct {
	PyObject_HEAD duckdb_Connection *connection;

	uint64_t rowcount;
	uint64_t offset;

	int closed;
	int reset;
	int initialized;
	duckdb_result result;
} duckdb_Cursor;

extern PyTypeObject duckdb_CursorType;

PyObject *duckdb_cursor_execute(duckdb_Cursor *self, PyObject *args);
PyObject *duckdb_cursor_getiter(duckdb_Cursor *self);
PyObject *duckdb_cursor_iternext(duckdb_Cursor *self);
// PyObject *duckdb_cursor_fetchone(duckdb_Cursor *self, PyObject *args);
// PyObject *duckdb_cursor_fetchmany(duckdb_Cursor *self, PyObject *args, PyObject *kwargs);
// PyObject *duckdb_cursor_fetchall(duckdb_Cursor *self, PyObject *args);
PyObject *duckdb_cursor_close(duckdb_Cursor *self, PyObject *args);

int duckdb_cursor_setup_types(void);

#define UNKNOWN (-1)
#endif
