#include <Python.h>
#include "include/mapping.h"
#include "include/string_utils.h"
#include "include/time_utils.h"

/**
 * convert type based on index mapping
 *
 * Args:
 *     key: field key
 *     value: field value
 *     index_type: type from index mapping
 *
 * Returns:
 *     PyObject*: converted value as Python object
 */
PyObject *convert_type(const char *key, PyObject *value, const char *index_type)
{
    if (!index_type)
    {
        Py_INCREF(value); // return original value
        return value;
    }

    // long type conversion
    if (strcmp(index_type, "long") == 0)
    {
        if (PyUnicode_Check(value))
        {
            const char *str_value = PyUnicode_AsUTF8(value);

            // handle hex strings
            if (strncmp(str_value, "0x", 2) == 0 || strncmp(str_value, "0X", 2) == 0)
            {
                long int_value = hex_to_int(str_value);
                if (int_value >= 0)
                {
                    return PyLong_FromLong(int_value);
                }
            }

            // try to convert string to integer
            if (is_numeric(str_value))
            {
                char *endptr;
                long int_value = strtol(str_value, &endptr, 10);
                if (*endptr == '\0')
                {
                    return PyLong_FromLong(int_value);
                }
            }
        }

        // either return as is
        Py_INCREF(value);
        return value;
    }

    // float/double type conversion
    else if (strcmp(index_type, "float") == 0 || strcmp(index_type, "double") == 0)
    {
        if (PyUnicode_Check(value))
        {
            const char *str_value = PyUnicode_AsUTF8(value);
            char *endptr;
            double float_value = strtod(str_value, &endptr);
            if (*endptr == '\0')
            {
                return PyFloat_FromDouble(float_value);
            }
        }

        // either return as is
        Py_INCREF(value);
        return value;
    }

    // date type conversion for hex strings
    else if (strcmp(index_type, "date") == 0 && PyUnicode_Check(value))
    {
        const char *str_value = PyUnicode_AsUTF8(value);
        if (!str_value)
        {
            Py_RETURN_NONE;
        }

        if (strncmp(str_value, "0x", 2) == 0 || strncmp(str_value, "0X", 2) == 0)
        {
            long int_value = hex_to_int(str_value);
            if (int_value >= 0)
            {
                // it's a valid date, return the same value
                Py_INCREF(value);
                return value;
            }
        }
    }

    // keyword/text type conversion
    else if (strcmp(index_type, "keyword") == 0 || strcmp(index_type, "text") == 0)
    {
        if (PyLong_Check(value))
        {
            long int_value = PyLong_AsLong(value);
            return PyUnicode_FromFormat("%ld", int_value);
        }
        else if (PyFloat_Check(value))
        {
            double float_value = PyFloat_AsDouble(value);
            return PyUnicode_FromFormat("%g", float_value);
        }

        // either return as is
        Py_INCREF(value);
        return value;
    }

    // ip type validation
    else if (strcmp(index_type, "ip") == 0 && PyUnicode_Check(value))
    {
        const char *str_value = PyUnicode_AsUTF8(value);
        if (!is_valid_ip(str_value))
        {
            Py_RETURN_NONE;
        }
        if (strcmp(str_value, "local") == 0 || strcmp(str_value, "LOCAL") == 0)
        {
            return PyUnicode_FromString("127.0.0.1");
        }

        //
        Py_RETURN_NONE;
    }

    // if no conversion applied, return original value
    Py_INCREF(value);
    return value;
}

/**
 * c implementation of type_checks function that converts values according to opensearch mapping types
 *
 * Args:
 *     self: module object
 *     args: positional arguments (key, value, index_type)
 *
 * Returns:
 *     PyObject*: tuple containing (key, converted_value)
 */
PyObject *c_type_checks(PyObject *self, PyObject *args)
{
    PyObject *key_obj;
    PyObject *value_obj;
    PyObject *index_type_obj = Py_None;

    /* parse arguments */
    if (!PyArg_ParseTuple(args, "OO|O", &key_obj, &value_obj, &index_type_obj))
    {
        Py_RETURN_NONE;
    }

    /* ensure key is a string */
    if (!PyUnicode_Check(key_obj))
    {
        PyErr_SetString(PyExc_TypeError, "key must be a string");
        Py_RETURN_NONE;
    }

    /* handle None values */
    if (value_obj == Py_None)
    {
        /* return (key, None) */
        return PyTuple_Pack(2, key_obj, Py_None);
    }

    const char *key = PyUnicode_AsUTF8(key_obj);
    const char *index_type = NULL;

    /* extract index_type string if provided */
    if (index_type_obj != Py_None && PyUnicode_Check(index_type_obj))
    {
        index_type = PyUnicode_AsUTF8(index_type_obj);
    }

    /* perform type conversion */
    PyObject *converted_value = convert_type(key, value_obj, index_type);

    /* return (key, converted_value) */
    return PyTuple_Pack(2, key_obj, converted_value);
}
