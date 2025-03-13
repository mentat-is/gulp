#ifndef __MUTY_TIME_H__
#define __MUTY_TIME_H__
#include <stdio.h>
#include <Python.h>

#ifdef __cplusplus
extern "C" {
#endif
	PyObject *c_ensure_iso8601(PyObject *self, PyObject *args, PyObject *kwargs);

#ifdef __cplusplus
}
#endif
#endif