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
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <errno.h>
#include <ctype.h>
#include <Python.h>
#include "include/libgulp.h"
#include "include/time_utils.h"
#include "include/string_utils.h"
#include "include/mapping.h"

/* method definitions */
static PyMethodDef ModuleMethods[] = {
    {"c_type_checks", c_type_checks, METH_VARARGS, "convert value based on index mapping type"},
    {"c_ensure_iso8601", c_ensure_iso8601, METH_VARARGS, "convert various time formats to iso8601"},
    {NULL, NULL, 0, NULL} /* sentinel */
};

/* module definition */
static struct PyModuleDef module_def = {
    PyModuleDef_HEAD_INIT,
    "gulp.libgulp",         /* module name */
    "c extension for gulp", /* module docstring */
    -1,                     /* size of per-interpreter state */
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
PyMODINIT_FUNC PyInit_libgulp(void)
{
    return PyModule_Create(&module_def);
}