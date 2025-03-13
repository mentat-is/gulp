#include <time.h>
#include <errno.h>
#include <ctype.h>
#include <stdlib.h>
#include <stdint.h>
#include <Python.h>
#include "../include/string_utils.h"
#include "../include/time_utils.h"

/**
 * determine number of digits in a integer
 *
 * Args:
 *     num: integer to check
 *
 * Returns:
 *     int: number of digits
 */
inline int num_digits(int64_t num) {
    if (num == 0) {
        return 1;
    }

    int digits = 0;
    if (num < 0) {
        num = -num;
        digits++; // for the minus sign
    }

    while (num > 0) {
        num /= 10;
        digits++;
    }

    return digits;
}

/**
 * convert numeric value to iso8601 string
 *
 * Args:
 *     numeric: numeric value representing timestamp
 *
 * Returns:
 *     const char*: iso8601 formatted string (must be freed by caller)
 */
char *number_to_iso8601(int64_t numeric) {
    time_t seconds;
    int64_t nanoseconds = 0;
    char *result = NULL;
    struct tm *tm_info;

    // Determine timestamp format based on value length
    int digits = num_digits(numeric);

    if (numeric > 1000000000000000000LL) {
        // nanoseconds from epoch
        seconds = numeric / 1000000000LL;
        nanoseconds = numeric % 1000000000LL;
    } else if (numeric > 1000000000000LL) {
        // milliseconds from epoch
        seconds = numeric / 1000;
    } else {
        // seconds from epoch
        seconds = numeric;
    }

    // Convert to tm struct
    tm_info = gmtime(&seconds);
    if (!tm_info) {
        return NULL;
    }

    // Format the ISO string (YYYY-MM-DDThh:mm:ss.sssZ)
    result = (char *)malloc(32);
    if (!result) {
        return NULL;
    }

    if (nanoseconds > 0) {
        // Include milliseconds
        int milliseconds = nanoseconds / 1000000;
        strftime(result, 32, "%Y-%m-%dT%H:%M:%S", tm_info);
        sprintf(result + strlen(result), ".%03dZ", milliseconds);
    } else {
        // Just seconds
        strftime(result, 32, "%Y-%m-%dT%H:%M:%SZ", tm_info);
    }

    return result;
}

/**
 * parse iso8601 date string
 *
 * Args:
 *     date_str: date string to parse
 *
 * Returns:
 *     time_t: unix timestamp, -1 if parsing fails
 */
time_t parse_iso8601(const char *date_str)
{
    struct tm tm_time = {0};
    char *ret;

    // Parse ISO 8601 format: YYYY-MM-DDThh:mm:ss.sssZ or YYYY-MM-DDThh:mm:ssZ
    ret = strptime(date_str, "%Y-%m-%dT%H:%M:%S", &tm_time);

    // Check if parsing was successful
    if (ret == NULL) {
        return -1;
    }

    // Handle milliseconds part if present
    if (*ret == '.') {
        // Skip decimal point and milliseconds
        while (*ret && *ret != 'Z' && *ret != '+' && *ret != '-') {
            ret++;
        }
    }

    // Handle timezone if present
    if (*ret == 'Z') {
        // UTC timezone, no adjustment needed
    }
    else if (*ret == '+' || *ret == '-') {
        int sign = (*ret == '+') ? 1 : -1;
        ret++;

        // Parse hours and minutes
        int hours = 0, minutes = 0;
        if (sscanf(ret, "%02d:%02d", &hours, &minutes) == 2) {
            // Adjust time by timezone offset
            tm_time.tm_hour -= sign * hours;
            tm_time.tm_min -= sign * minutes;
        }
    }

    // Convert to time_t (seconds since epoch)
    tm_time.tm_isdst = -1; // Let the system determine DST
    return mktime(&tm_time);
}

/**
 * c implementation of ensure_iso8601 to convert various time formats to iso8601
 *
 * Args:
 *     self: module object
 *     args: positional arguments (time_str, dayfirst, yearfirst, fuzzy)
 *     kwargs: keyword arguments
 *
 * Returns:
 *     PyObject*: iso8601 formatted string
 *
 * Throws:
 *     ValueError: if time_str has invalid format
 *     TypeError: if time_str is not a string or integer
 */
PyObject *c_ensure_iso8601(PyObject *self, PyObject *args, PyObject *kwargs) {
    PyObject *time_str_obj;
    PyObject *dayfirst_obj = Py_None;
    PyObject *yearfirst_obj = Py_None;
    PyObject *fuzzy_obj = Py_None;

    static char *kwlist[] = {"time_str", "dayfirst", "yearfirst", "fuzzy", NULL};

    /* parse arguments and keyword arguments */
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|OOO", kwlist,
                                     &time_str_obj, &dayfirst_obj, &yearfirst_obj, &fuzzy_obj)) {
        Py_RETURN_NONE;
    }

    /* handle numeric timestamp as int */
    if (PyLong_Check(time_str_obj)) {
        /* handle numeric timestamp */
        int64_t numeric = PyLong_AsLongLong(time_str_obj);
        char *iso_str = number_to_iso8601(numeric);
        if (!iso_str)
        {
            PyErr_SetString(PyExc_ValueError, "invalid time format");
            Py_RETURN_NONE;
        }
        PyObject *result = PyUnicode_FromString(iso_str);
        free(iso_str);
        return result;
    } /* handle string input:*/ else if (PyUnicode_Check(time_str_obj)) {
        const char *time_str = PyUnicode_AsUTF8(time_str_obj);

        /* check if string is numeric */
        if (is_numeric(time_str)) {
            int64_t numeric = atoll(time_str);
            char *iso_str = number_to_iso8601(numeric);
            if (!iso_str) {
                PyErr_SetString(PyExc_ValueError, "invalid time format");
                Py_RETURN_NONE;
            }
            PyObject *result = PyUnicode_FromString(iso_str);
            free(iso_str);
            return result;
        }

        /* try to parse as ISO8601 */
        time_t timestamp = parse_iso8601(time_str);
        if (timestamp != -1) {
            /* already ISO8601 format, return as is */
            return PyUnicode_FromString(time_str);
        }

        /* if we get here, the format is not recognized */
        PyErr_Format(PyExc_ValueError, "invalid time format: %s", time_str);
        Py_RETURN_NONE;
    } else {
        PyErr_SetString(PyExc_TypeError, "time_str must be a string or integer");
        Py_RETURN_NONE;
    }
}