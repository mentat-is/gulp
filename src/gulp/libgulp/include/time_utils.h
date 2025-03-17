#ifndef __MUTY_TIME_H__
#define __MUTY_TIME_H__
#include <stdio.h>
#include <Python.h>

#ifdef __cplusplus
extern "C" {
#endif
	PyObject *c_ensure_iso8601(PyObject *self, PyObject *args);
	PyObject* c_string_to_nanos_from_unix_epoch(PyObject* self, PyObject* args);
	PyObject* c_number_to_nanos_from_unix_epoch(PyObject* self, PyObject* args);
#ifdef __cplusplus
}
#endif
#endif