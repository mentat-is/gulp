/**
 * libgulp.cpp - gulp c extension implementation
 *
 * this file contains the implementation of the c extension module for gulp.
 *
 * use like:
 *
 * import gulp.libgulp
 * print(gulp.libgulp.fast_add(2,3))
 *
 * build together with package install like i.e.
 *
 * pip3 install -e .
 *
 * either build during dev using the makefile
 *
 * cd src/gulp/libgulp
 * make
 */

#define PY_SSIZE_T_CLEAN
#include "include/libgulp.h"

#include <Python.h>
#include <ctype.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "include/mapping.h"
#include "include/string_utils.h"
#include "include/time_utils.h"

/* method definitions */
static PyMethodDef ModuleMethods[] = {
    {"c_number_to_nanos_from_unix_epoch",  (PyCFunction)c_number_to_nanos_from_unix_epoch,
     METH_VARARGS|METH_KEYWORDS, "C variant of muty.time.number_to_nanos_from_unix_epoch"},
    {"c_string_to_nanos_from_unix_epoch",  (PyCFunction)c_string_to_nanos_from_unix_epoch,
     METH_VARARGS|METH_KEYWORDS, "C variant of muty.time.c_string_to_nanos_from_unix_epoch"},
    {"c_ensure_iso8601",  (PyCFunction)c_ensure_iso8601, METH_VARARGS | METH_KEYWORDS,
     "convert various time formats to iso8601"},
    {"c_is_valid_ip",  (PyCFunction)c_is_valid_ip, METH_VARARGS,
     "C variant of muty.string.is_valid_ip"},
    {NULL, NULL, 0, NULL} /* sentinel */
};

/* module definition */
static struct PyModuleDef module_def = {
    PyModuleDef_HEAD_INIT, "gulp.libgulp", /* module name */
    "c extension for gulp",                /* module docstring */
    -1,                                    /* size of per-interpreter state */
    ModuleMethods};

/**
 * module initialization function
 *
 * Returns:
 *     PyObject*: the module object
 *
 * Throws:
 *     PyError: if module creation fails
 */
PyMODINIT_FUNC PyInit_libgulp(void) { return PyModule_Create(&module_def); }