#ifndef __STRING_UTILS_H__
#define __STRING_UTILS_H__
#include <Python.h>

#ifdef __cplusplus
extern "C" {
#endif

int is_numeric(const char *str);
long hex_to_int(const char *hex_str);
int is_valid_ip(const char *str);
PyObject *c_is_valid_ip(PyObject *self, PyObject *args);

#ifdef __cplusplus
}
#endif
#endif