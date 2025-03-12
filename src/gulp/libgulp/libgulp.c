/**
 * module.c - gulp c extension implementation
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
#include <Python.h>
#include "include/libgulp.h"

/**
 * example method that adds two numbers
 *
 * Args:
 *     self: module object
 *     args: positional arguments (expects two integers)
 *
 * Returns:
 *     PyObject*: python integer with the sum
 */
static PyObject *fast_add(PyObject *self, PyObject *args)
{
    int a, b;

    /* parse arguments */
    if (!PyArg_ParseTuple(args, "ii", &a, &b))
    {
        return NULL; /* raises appropriate exception */
    }

    return PyLong_FromLong(a + b);
}

/* method definitions */
static PyMethodDef ModuleMethods[] = {
    {"fast_add", fast_add, METH_VARARGS, "add two integers quickly"},
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