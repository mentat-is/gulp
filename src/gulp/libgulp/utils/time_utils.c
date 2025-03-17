#include "../include/time_utils.h"

#include <Python.h>
#include <ctype.h>
#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "../include/string_utils.h"

// constants for time conversions
#define SECONDS_TO_NANOSECONDS 1000000000LL
#define MILLISECONDS_TO_NANOSECONDS 1000000LL
#define MICROSECONDS_TO_NANOSECONDS 1000LL

// chrome epoch starts at 1601-01-01, unix epoch at 1970-01-01
#define CHROME_TO_UNIX_EPOCH_SECONDS 11644473600LL

// maximum length of iso8601 string
#define ISO8601_MAX_LEN 64

/**
 * formats a time_t value as iso8601 string with utc timezone
 *
 * Args:
 *     timestamp (time_t): time in seconds since unix epoch
 *     fraction_ns (int): fractional part in nanoseconds (0-999999999)
 *     buffer (char*): buffer to store the result
 *     buffer_size (size_t): size of buffer
 *
 * Returns:
 *     bool: true if formatting succeeded
 */
bool format_iso8601(time_t timestamp, int fraction_ns, char* buffer,
                    size_t buffer_size) {
  struct tm tm_info;

#ifdef _WIN32
  gmtime_s(&tm_info, &timestamp);
#else
  gmtime_r(&timestamp, &tm_info);
#endif

  // format the base time (YYYY-MM-DDThh:mm:ss)
  size_t len = strftime(buffer, buffer_size, "%Y-%m-%dT%H:%M:%S", &tm_info);
  if (len == 0) return false;

  // add fractional part if needed (with appropriate precision)
  if (fraction_ns > 0) {
    // count significant digits by removing trailing zeros
    int fraction_digits = 9;
    int temp = fraction_ns;
    while (temp % 10 == 0 && temp > 0 && fraction_digits > 0) {
      temp /= 10;
      fraction_digits--;
    }

    // format fraction with appropriate precision
    if (fraction_digits > 0) {
      char format[20];
      snprintf(format, sizeof(format), ".%%0%dd", fraction_digits);

      // calculate fraction with appropriate scale
      int divisor = 1;
      for (int i = 0; i < (9 - fraction_digits); i++) {
        divisor *= 10;
      }
      int fraction_scaled = fraction_ns / divisor;

      // append fraction to buffer
      len += snprintf(buffer + len, buffer_size - len, format, fraction_scaled);
    }
  }

  // add Z for UTC timezone
  if (len < buffer_size - 1) {
    buffer[len] = 'Z';
    buffer[len + 1] = '\0';
    return true;
  }

  return false;
}

/**
 * checks if an iso8601 string has utc timezone
 *
 * Args:
 *     iso_str (const char*): the iso8601 string to check
 *
 * Returns:
 *     bool: true if string has utc timezone (Z or +00:00)
 */
bool has_utc_timezone(const char* iso_str) {
  if (!iso_str) return false;

  size_t len = strlen(iso_str);
  if (len == 0) return false;

  // check for Z at the end
  if (iso_str[len - 1] == 'Z' || iso_str[len - 1] == 'z') {
    return true;
  }

  // check for +00:00 or -00:00
  if (len >= 6) {
    const char* end = iso_str + len - 6;
    if ((end[0] == '+' || end[0] == '-') && end[1] == '0' && end[2] == '0' &&
        end[3] == ':' && end[4] == '0' && end[5] == '0') {
      return true;
    }
  }

  return false;
}

/**
 * converts a tm struct to seconds since unix epoch (utc)
 *
 * Args:
 *     tm_info (struct tm*): pointer to the time structure
 *
 * Returns:
 *     time_t: seconds since unix epoch, or -1 on error
 */
time_t tm_to_utc_time(struct tm* tm_info) {
#ifdef _WIN32
  // windows doesn't have timegm, use mktime and adjust for local timezone
  time_t local_time = mktime(tm_info);
  if (local_time == -1) return -1;

  // get the timezone offset
  time_t gmt_time;
  struct tm gmt_tm;
  gmtime_s(&gmt_tm, &local_time);
  gmt_tm.tm_isdst = 0;
  gmt_time = mktime(&gmt_tm);

  // return utc time
  return local_time + (local_time - gmt_time);
#else
  // use timegm on posix systems
  return timegm(tm_info);
#endif
}

/**
 * checks if a string is in iso8601 format
 *
 * Args:
 *     str (const char*): string to check
 *
 * Returns:
 *     bool: true if string is in iso8601 format
 */
bool is_iso8601(const char* str) {
  // basic validation - check for minimum length and YYYY-MM-DD pattern
  if (!str || strlen(str) < 10) {
    return false;
  }

  // check for yyyy-mm-dd pattern
  if (isdigit(str[0]) && isdigit(str[1]) && isdigit(str[2]) &&
      isdigit(str[3]) && str[4] == '-' && isdigit(str[5]) && isdigit(str[6]) &&
      str[7] == '-' && isdigit(str[8]) && isdigit(str[9])) {
    // check for T separator (required in ISO8601)
    const char* t_pos = strchr(str, 'T');
    if (t_pos || strchr(str, 't')) {
      return true;
    }
  }
  return false;
}

/**
 * enforces utc timezone on an iso8601 string
 *
 * Args:
 *     iso_str (const char*): the input iso8601 string
 *     buffer (char*): buffer to store the result
 *     buffer_size (size_t): size of buffer
 *
 * Returns:
 *     bool: true if conversion succeeded
 */
bool enforce_utc_timezone(const char* iso_str, char* buffer,
                          size_t buffer_size) {
  // validate inputs
  if (!iso_str || !buffer || buffer_size < 20) {
    return false;
  }

  // check input length to prevent buffer overflow
  size_t iso_len = strlen(iso_str);
  if (iso_len >= buffer_size) {
    // input too long for buffer
    return false;
  }

  // if already has utc timezone, just normalize it
  if (has_utc_timezone(iso_str)) {
    // copy the base part of the string (without timezone)
    size_t base_len = iso_len;

    // determine where timezone specifier starts
    if (iso_str[iso_len - 1] == 'Z' || iso_str[iso_len - 1] == 'z') {
      // format with 'Z' at the end
      base_len = iso_len - 1;
    } else if (iso_len >= 6 &&
               (iso_str[iso_len - 6] == '+' || iso_str[iso_len - 6] == '-') &&
               iso_str[iso_len - 5] == '0' && iso_str[iso_len - 4] == '0' &&
               iso_str[iso_len - 3] == ':' && iso_str[iso_len - 2] == '0' &&
               iso_str[iso_len - 1] == '0') {
      // format with +00:00 or -00:00
      base_len = iso_len - 6;
    }

    // copy base part
    if (base_len >= buffer_size - 2) {
      // not enough space for base + 'Z' + null terminator
      return false;
    }

    // copy base part and add 'Z'
    strncpy(buffer, iso_str, base_len);
    buffer[base_len] = 'Z';
    buffer[base_len + 1] = '\0';
    // printf("has_utc_timezone, return true: %s", buffer);
    return true;
  }

  // no utc timezone, need to parse and convert
  struct tm tm_info = {0};
  char* parse_success = NULL;

  // printf("has_utc_timezone, before strptime: %s\n", iso_str);

  // try all supported date formats with timezone information
  const char* formats[] = {
      "%Y-%m-%dT%H:%M:%S",         // iso 8601: "2025-03-13t15:30:45"
      "%Y-%m-%dT%H:%M:%SZ",        // iso 8601 with Z: "2025-03-13t15:30:45z"
      "%Y-%m-%dT%H:%M:%S%z",       // rfc 3339: "2025-03-13t15:30:45+00:00"
      "%a, %d %b %Y %H:%M:%S %z",  // rfc 2822: "thu, 13 mar 2025 15:30:45
                                   // +0000"
      "%Y-%m-%d %H:%M:%S",         // common format: "2025-03-13 15:30:45"
      "%Y-%m-%d",                  // iso date only: "2025-03-13"
  };

  int num_formats = sizeof(formats) / sizeof(formats[0]);

  // try each format to parse the date string
  for (int i = 0; i < num_formats; i++) {
    memset(&tm_info, 0, sizeof(struct tm));
    parse_success = strptime(iso_str, formats[i], &tm_info);

    if (parse_success) {
      // printf("format found (%s)\n", formats[i]);

      // convert to utc time
      time_t timestamp = tm_to_utc_time(&tm_info);
      if (timestamp != -1) {
        // format as iso8601 with utc timezone
        if (format_iso8601(timestamp, 0, buffer, buffer_size)) {
          return true;
        }
      }
    }
  }

  // printf("has_utc_timezone, return false");
  return false;
}

// Helper function to convert timezone offset string to seconds
long parse_timezone_offset(const char* offset_str) {
  int hours, minutes;
  char sign;

  if (sscanf(offset_str, "%c%2d:%2d", &sign, &hours, &minutes) == 3) {
    long total_seconds = hours * 3600 + minutes * 60;
    return (sign == '-') ? -total_seconds : total_seconds;
  } else {
    return 0;  // Invalid offset format (Should be an error in strict parser)
  }
}

/**
 * converts an iso8601 string to nanoseconds since unix epoch
 *
 * Args:
 *     iso_str (const char*): the iso8601 string to convert
 *
 * Returns:
 *     int64_t: nanoseconds since unix epoch, or -1 on failure
 *
 * Throws:
 *     does not throw exceptions, returns -1 on failure
 */
int64_t iso8601_to_nanos(const char* iso_str) {
  // validate input
  if (!iso_str || !is_iso8601(iso_str)) {
    return -1;
  }

  // create a buffer for normalized iso8601 with utc timezone
  char utc_buffer[ISO8601_MAX_LEN];
  if (!enforce_utc_timezone(iso_str, utc_buffer, ISO8601_MAX_LEN)) {
    return -1;
  }

  // parse the base time component
  struct tm tm_info = {0};
  char* parse_success = NULL;

  // iso8601 format with Z: "2025-03-13t15:30:45z"
  parse_success = strptime(utc_buffer, "%Y-%m-%dT%H:%M:%S", &tm_info);

  if (!parse_success) {
    return -1;
  }

  // convert to seconds since unix epoch
  time_t unix_seconds = tm_to_utc_time(&tm_info);
  if (unix_seconds == -1) {
    return -1;
  }

  // convert seconds to nanoseconds
  int64_t nanos = (int64_t)unix_seconds * SECONDS_TO_NANOSECONDS;

  // handle fractional seconds if present
  char* fractional = strstr(utc_buffer, ".");
  if (fractional && fractional[1] != '\0' && fractional[1] != 'Z' &&
      fractional[1] != 'z') {
    fractional++;  // move past decimal point

    // extract up to 9 digits for nanoseconds
    char fraction_str[10] = {0};
    size_t i = 0;
    while (i < 9 && isdigit(fractional[i])) {
      fraction_str[i] = fractional[i];
      i++;
    }

    // pad with zeros to complete 9 digits
    while (i < 9) {
      fraction_str[i] = '0';
      i++;
    }

    // convert fraction string to integer
    int64_t fraction_ns = atoll(fraction_str);

    // add fractional part to total nanoseconds
    nanos += fraction_ns;
  }

  return nanos;
}

/**
 * parses a numeric timestamp to iso8601 string with utc timezone
 *
 * Args:
 *     timestamp (int64_t): timestamp value (seconds, millis, nanos, or chrome
 * epoch) buffer (char*): buffer to store the result buffer_size (size_t): size
 * of buffer
 *
 * Returns:
 *     bool: true if parsing succeeded
 */
bool timestamp_to_iso8601(int64_t timestamp, char* buffer, size_t buffer_size) {
  time_t seconds;
  int fraction_ns = 0;

  // determine format based on magnitude
  if (timestamp > 1000000000000000000LL) {
    // nanoseconds since epoch
    seconds = timestamp / SECONDS_TO_NANOSECONDS;
    fraction_ns = timestamp % SECONDS_TO_NANOSECONDS;
  } else if (timestamp > 1000000000000LL) {
    // milliseconds since epoch
    seconds = timestamp / 1000;
    fraction_ns = (timestamp % 1000) * MILLISECONDS_TO_NANOSECONDS / 1000;
  } else if (timestamp > 10000000000LL && timestamp < 1000000000000LL) {
    // likely chrome epoch (microseconds since 1601-01-01)
    seconds = (timestamp / 1000000LL) - CHROME_TO_UNIX_EPOCH_SECONDS;
    fraction_ns = (timestamp % 1000000) * 1000;
  } else {
    // seconds since epoch
    seconds = timestamp;
    fraction_ns = 0;
  }

  return format_iso8601(seconds, fraction_ns, buffer, buffer_size);
}

/**
 * parses a date string in various formats and returns iso8601 string with utc
 * timezone
 *
 * supported formats:
 * - iso 8601 (e.g., "2025-03-13t15:30:45+00:00")
 * - rfc 3339 (e.g., "2025-03-13t15:30:45z")
 * - rfc 2822 (e.g., "thu, 13 mar 2025 15:30:45 +0000")
 * - unix timestamp in seconds (e.g., "1710339045")
 * - unix timestamp in milliseconds (e.g., "1710339045000")
 * - unix timestamp in nanoseconds (e.g., "1710339045000000000")
 * - chrome epoch timestamp (e.g., "13304159845000000")
 * - common date formats (e.g., "2025-03-13 15:30:45")
 *
 * Args:
 *     date_str (const char*): the date string to parse
 *
 * Returns:
 *     char*: newly allocated iso8601 string with utc timezone, or NULL on
 * failure caller must free this memory
 *
 * Throws:
 *     does not throw exceptions, returns NULL on failure
 */
char* parse_date_to_iso8601(const char* date_str) {
  if (!date_str || strlen(date_str) == 0) {
    return NULL;
  }
  // allocate result buffer
  char* result = (char*)malloc(ISO8601_MAX_LEN);
  if (!result) {
    return NULL;
  }
  // first check if already ISO8601
  if (is_iso8601(date_str)) {
    // enforce UTC timezone
    if (enforce_utc_timezone(date_str, result, ISO8601_MAX_LEN)) {
      return result;
    } else {
      printf("iso_8601, not enforce_utc_timezone\n");
      free(result);
      return NULL;
    }
  }

  // check if the string is all digits (could be a timestamp)
  int is_numeric = 1;
  size_t len = strlen(date_str);
  for (size_t i = 0; i < len; i++) {
    if (!isdigit(date_str[i])) {
      is_numeric = 0;
      break;
    }
  }

  if (is_numeric) {
    int64_t timestamp = atoll(date_str);
    if (timestamp_to_iso8601(timestamp, result, ISO8601_MAX_LEN)) {
      return result;
    } else {
      free(result);
      return NULL;
    }
  }

  // try parsing with different formats
  struct tm tm_info = {0};
  char* parse_success = NULL;
  const char* formats[] = {
      "%Y-%m-%dT%H:%M:%S",         // iso 8601: "2025-03-13t15:30:45"
      "%Y-%m-%dT%H:%M:%SZ",        // iso 8601 with Z: "2025-03-13t15:30:45z"
      "%Y-%m-%dT%H:%M:%S%z",       // rfc 3339: "2025-03-13t15:30:45+00:00"
      "%a, %d %b %Y %H:%M:%S %z",  // rfc 2822: "thu, 13 mar 2025 15:30:45
                                   // +0000"
      "%Y-%m-%d %H:%M:%S",         // common format: "2025-03-13 15:30:45"
      "%m/%d/%Y %H:%M:%S",         // us date with time: "03/13/2025 15:30:45"
      "%m/%d/%Y",                  // us date: "03/13/2025"
      "%d/%m/%Y",                  // european date: "13/03/2025"
      "%Y/%m/%d",                  // year first: "2025/03/13"
      "%b %d, %Y",                 // month name: "mar 13, 2025"
      "%d %b %Y",                  // day first with month name: "13 mar 2025"
      "%Y-%m-%d",                  // iso date only: "2025-03-13"
      "%H:%M:%S",                  // time only: "15:30:45" (uses today's date)
      "%I:%M:%S %p"  // 12-hour time: "03:30:45 pm" (uses today's date)
  };

  int num_formats = sizeof(formats) / sizeof(formats[0]);

  for (int i = 0; i < num_formats; i++) {
    memset(&tm_info, 0, sizeof(struct tm));

    // try each format
    parse_success = strptime(date_str, formats[i], &tm_info);

    if (parse_success) {
      // if parsing time only, fill in today's date
      if (strcmp(formats[i], "%H:%M:%S") == 0 ||
          strcmp(formats[i], "%I:%M:%S %p") == 0) {
        time_t now = time(NULL);
        struct tm* today = localtime(&now);
        tm_info.tm_year = today->tm_year;
        tm_info.tm_mon = today->tm_mon;
        tm_info.tm_mday = today->tm_mday;
      }

      time_t unix_seconds = tm_to_utc_time(&tm_info);
      if (unix_seconds != -1) {
        int fraction_ns = 0;

        // handle fractional seconds if present
        char* fractional = strstr(date_str, ".");
        if (fractional && fractional[1] != '\0') {
          fractional++;  // move past decimal point

          // extract up to 9 digits for nanoseconds
          char fraction[10] = {0};
          size_t j = 0;
          while (j < 9 && isdigit(fractional[j])) {
            fraction[j] = fractional[j];
            j++;
          }

          // pad with zeros to complete 9 digits
          while (j < 9) {
            strcat(fraction, "0");
            j++;
          }

          // add fractional part
          fraction_ns = atoi(fraction);
        }

        if (format_iso8601(unix_seconds, fraction_ns, result,
                           ISO8601_MAX_LEN)) {
          return result;
        }
      }
    }
  }

  // failed to parse the date string
  free(result);
  return NULL;
}

int parse_iso8601(const char* iso8601_string, struct tm* tm,
                  long* nanoseconds) {
  int year, month, day, hour, minute, second, nano = 0;
  double fraction = 0.0;
  char timezone_offset_str[7] = {0};  // e.g., "+05:30" or "-08:00"

  int matched =
      sscanf(iso8601_string, "%d-%d-%dT%d:%d:%d.%lf%6s", &year, &month, &day,
             &hour, &minute, &second, &fraction, timezone_offset_str);

  if (matched < 6) {
    matched = sscanf(iso8601_string, "%d-%d-%dT%d:%d:%d%6s", &year, &month,
                     &day, &hour, &minute, &second, timezone_offset_str);
    if (matched < 6) {
      return -1;
    }
    fraction = 0.0;
  }

  // Basic sanity checks
  if (year < 1970 || month < 1 || month > 12 || day < 1 || day > 31 ||
      hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 ||
      second > 59) {
    return -1;  // Invalid date/time
  }

  tm->tm_year = year - 1900;
  tm->tm_mon = month - 1;
  tm->tm_mday = day;
  tm->tm_hour = hour;
  tm->tm_min = minute;
  tm->tm_sec = second;
  tm->tm_isdst = -1;  // Let mktime/localtime determine DST

  // Handle timezone information
  if (matched > 6) {
    if (timezone_offset_str[0] == 'Z') {
#ifdef __USE_MISC
      tm->tm_gmtoff = 0;
      tm->tm_zone = "UTC";
#else
      tm->__tm_gmtoff = 0;
      tm->__tm_zone = "UTC";
#endif
    } else if (timezone_offset_str[0] == '+' || timezone_offset_str[0] == '-') {
#ifdef __USE_MISC
      tm->tm_gmtoff = parse_timezone_offset(timezone_offset_str);

      snprintf((char*)tm->tm_zone, sizeof(tm->tm_zone), "%s",
               timezone_offset_str);
#else
      tm->__tm_gmtoff = parse_timezone_offset(timezone_offset_str);
      snprintf((char*)tm->__tm_zone, sizeof(tm->__tm_zone), "%s",
               timezone_offset_str);
#endif
    } else {
      return -1;  // Invalid timezone format
    }
  } else {
// If no timezone information is present, leave tm_gmtoff to 0 (UTC)
#ifdef __USE_MISC
    tm->tm_gmtoff = 0;
    tm->tm_zone = NULL;
#else
    tm->__tm_gmtoff = 0;
    tm->__tm_zone = NULL;
#endif
  }

  nano = (long)(fraction * 1e9);
  *nanoseconds = nano;

  return 0;
}

/**
 * c implementation of ensure_iso8601 to convert various time formats to iso8601
 *
 * Args:
 *     self (PyObject*): module object
 *     args (PyObject*): positional arguments (time_str, dayfirst, yearfirst,
 * fuzzy) kwargs (PyObject*): keyword arguments
 *
 * Returns:
 *     PyObject*: iso8601 formatted string, or NULL on failure
 *
 * Throws:
 *     ValueError: if time_str has invalid format
 *     TypeError: if time_str is not a string or integer
 */
PyObject* c_ensure_iso8601(PyObject* self, PyObject* args, PyObject* kwargs) {
  PyObject* time_str_obj;
  PyObject* dayfirst_obj = Py_None;
  PyObject* yearfirst_obj = Py_None;
  PyObject* fuzzy_obj = Py_None;

  static char* kwlist[] = {"time_str", "dayfirst", "yearfirst", "fuzzy", NULL};

  /* parse arguments and keyword arguments */
  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|OOO", kwlist, &time_str_obj,
                                   &dayfirst_obj, &yearfirst_obj, &fuzzy_obj)) {
    return NULL;  // parsing failed, exception already set
  }

  /* handle numeric timestamp as int */
  if (PyLong_Check(time_str_obj)) {
    /* handle numeric timestamp */
    int64_t numeric = PyLong_AsLongLong(time_str_obj);
    char numstr[65] = {0};
    snprintf(numstr, sizeof(numstr), "%" PRId64, numeric);

    // convert to iso8601
    char* iso_str = parse_date_to_iso8601(numstr);
    if (!iso_str) {
      // handle parsing failure
      PyErr_Format(PyExc_ValueError, "failed to parse numeric value: %s",
                   numstr);
      return NULL;
    }

    // create python string
    PyObject* result = PyUnicode_FromString(iso_str);
    free(iso_str);
    return result;
  } else if (PyUnicode_Check(time_str_obj)) {
    /* handle string input */
    const char* time_str = PyUnicode_AsUTF8(time_str_obj);
    if (!time_str) {
      // pyunicode_asutf8 sets an exception
      return NULL;
    }

    // maximum length check to prevent malicious inputs
    if (strlen(time_str) > ISO8601_MAX_LEN) {
      PyErr_Format(PyExc_ValueError,
                   "input string too long (max %d characters)",
                   ISO8601_MAX_LEN);
      return NULL;
    }

    // convert to iso8601
    char* iso_str = parse_date_to_iso8601(time_str);
    if (!iso_str) {
      // handle parsing failure
      PyErr_Format(PyExc_ValueError, "failed to parse date string: %s",
                   time_str);
      return NULL;
    }

    // create python string
    PyObject* result = PyUnicode_FromString(iso_str);
    free(iso_str);
    return result;
  }

  PyErr_SetString(PyExc_TypeError, "time_str must be a string or integer");
  return NULL;
}

PyObject* c_number_to_nanos_from_unix_epoch(PyObject* self, PyObject* args,
                                            PyObject* kwargs) {
  /**
   * c implementation of number_to_nanos_from_unix_epoch to convert various time
   *
   * Args:
   *    self (PyObject*): module object
   *    args (PyObject*): positional arguments (numeric)
   *    kwargs (PyObject*): keyword arguments
   *
   * Returns:
   *   PyObject*: nanoseconds from unix epoch as python integer, or None on
   *   failure
   */
  PyObject* numeric_obj = NULL;
  char* kwlist[] = {"numeric", NULL};
  long long result = 0;

  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O", kwlist, &numeric_obj)) {
    Py_RETURN_NONE;  // Parsing failure raises an exception
  }

  if (PyUnicode_Check(numeric_obj)) {
    const char* num_str = PyUnicode_AsUTF8(numeric_obj);
    if (num_str == NULL) {
      PyErr_Format(PyExc_ValueError, "invalid time format: %s", num_str);
      Py_RETURN_NONE;
    }

    char* endptr;
    // TODO: do we need to reduce ref count of numeric_obj since we are
    // technically pointing it to somewhere else?
    numeric_obj = PyLong_FromString(num_str, &endptr, 10);
  }

  if (PyLong_Check(numeric_obj)) {
    // Convert input to a Python integer
    PyObject* numeric_int = PyNumber_Index(numeric_obj);
    if (numeric_int == NULL) {
      Py_RETURN_NONE;  // Conversion failure raises an exception
    }

    // Extract the integer value as a long long
    long long numeric = PyLong_AsLongLong(numeric_int);
    Py_DECREF(numeric_int);  // Release the temporary integer object

    if (numeric == -1 && PyErr_Occurred()) {
      Py_RETURN_NONE;
    }

    if (numeric < 0) {
      PyErr_Format(PyExc_ValueError, "Numeric value must be non-negative: %lld",
                   numeric);
      Py_RETURN_NONE;
    }

    if (numeric > 1000000000000000000LL) {  // Assume nanoseconds
      result = numeric;
    } else if (numeric > 1000000000LL) {  // Assume milliseconds
      result = numeric * MILLISECONDS_TO_NANOSECONDS;
    } else {  // Assume seconds
      result = numeric * SECONDS_TO_NANOSECONDS;
    }
  } else {
    PyErr_Format(PyExc_ValueError, "Unsupported format type for object: %p",
                 numeric_obj);
    Py_RETURN_NONE;
  }

  return PyLong_FromLongLong(result);  // Return result as Python integer
}

/**
 * c implementation of string_to_nanos_from_unix_epoch to convert various time
 * formats to nanos from unix epoch
 *
 * Args:
 *     self (PyObject*): module object
 *     args (PyObject*): positional arguments (time_str, dayfirst, yearfirst,
 * fuzzy) kwargs (PyObject*): keyword arguments
 *
 * Returns:
 *     PyObject*: nanoseconds from unix epoch as python integer, or None on
 * failure
 *
 * Throws:
 *     ValueError: if time_str has invalid format
 *     TypeError: if time_str is not a string or integer
 */
PyObject* c_string_to_nanos_from_unix_epoch(PyObject* self, PyObject* args,
                                            PyObject* kwargs) {
  // convert to iso8601 first, then to nanos
  PyObject* iso8601 = c_ensure_iso8601(self, args, kwargs);
  if (iso8601 == NULL) {
    // c_ensure_iso8601 should have set an exception already
    return NULL;
  }

  // ensure we have a unicode string
  if (!PyUnicode_Check(iso8601)) {
    Py_DECREF(iso8601);
    PyErr_SetString(PyExc_ValueError,
                    "internal error: ensure_iso8601 returned non-string");
    return NULL;
  }

  // extract c string
  const char* time_str = PyUnicode_AsUTF8(iso8601);
  if (time_str == NULL) {
    Py_DECREF(iso8601);
    // pyunicode_asutf8 sets an exception if it fails
    return NULL;
  }

  // convert to nanoseconds
  int64_t nanos = iso8601_to_nanos(time_str);

  // clean up the iso8601 string, we don't need it anymore
  Py_DECREF(iso8601);

  // check if conversion was successful
  if (nanos == -1) {
    PyErr_Format(PyExc_ValueError, "failed to parse iso8601 string: %s",
                 time_str);
    return NULL;
  }

  // return nanoseconds as python integer
  return PyLong_FromLongLong(nanos);
}
