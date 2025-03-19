#include <Python.h>
#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/**
 * check if string is numeric
 *
 * Args:
 *     str (const char*): string to check
 *
 * Returns:
 *     int: 1 if numeric, 0 otherwise
 */
int is_numeric(const char *str) {
  // fast fail for null or empty strings
  if (!str || !*str) {
    return 0;
  }

  const char *p = str;

  // handle negative sign without function call
  if (*p == '-') {
    p++;
    // handle just a minus sign case
    if (!*p) {
      return 0;
    }
  }

  // direct pointer traversal without any function calls
  // using character literals instead of isdigit()
  do {
    if (*p < '0' || *p > '9') {
      return 0;
    }
  } while (*++p);

  return 1;
}

/**
 * convert hexadecimal string to integer
 *
 * Args:
 *     hex_str (const char*): hexadecimal string
 *
 * Returns:
 *     long: converted integer value, -1 if conversion fails
 */
long hex_to_int(const char *hex_str) {
  // fast fail for null or empty strings
  if (!hex_str || !*hex_str) {
    return -1;
  }

  // skip "0x" prefix if present
  if (hex_str[0] == '0' && (hex_str[1] == 'x' || hex_str[1] == 'X')) {
    hex_str += 2;
    // handle "0x" with nothing after
    if (!*hex_str) {
      return -1;
    }
  }

  // manual hex conversion without calling strtol()
  long result = 0;
  char c;
  while ((c = *hex_str++)) {
    result <<= 4;  // multiply by 16

    // convert hex character to value
    if (c >= '0' && c <= '9') {
      result |= (c - '0');
    } else if (c >= 'a' && c <= 'f') {
      result |= (c - 'a' + 10);
    } else if (c >= 'A' && c <= 'F') {
      result |= (c - 'A' + 10);
    } else {
      // invalid hex character
      return -1;
    }
  }

  return result;
}

/**
 * checks if a string is a valid ipv4 or ipv6 address.
 * this function is highly optimized for performance with minimal branching
 * and includes proper bounds checking for memory safety.
 *
 * Args:
 *     ip_str (const char*): the string to check if it's an ip address
 *
 * Returns:
 *     int: 4 if ipv4, 6 if ipv6, 0 if not a valid ip address
 */
int is_valid_ip(const char *ip_str) {
  // fast null/empty check
  if (!ip_str || !*ip_str) {
    return 0;
  }

  // compute length once for bounds checking
  size_t ip_len = strlen(ip_str);

  // special case for ipv4 localhost
  if (ip_len == 9 && strncmp(ip_str, "127.0.0.1", 9) == 0) {
    return 4;  // fast-path for 127.0.0.1
  }

  // special case for ipv6 localhost
  if (ip_len == 3 && strncmp(ip_str, "::1", 3) == 0) {
    return 6;  // fast-path for ::1
  }

  // check if this might be ipv4 (contains dot) or ipv6 (contains colon)
  const char *scan = ip_str;
  unsigned char has_dot = 0;
  unsigned char has_colon = 0;
  size_t pos = 0;

  // quick scan for character type with bounds checking
  while (pos < ip_len && !(has_dot && has_colon)) {
    has_dot |= (scan[pos] == '.');
    has_colon |= (scan[pos] == ':');
    pos++;
  }

  // ipv4 validation (more common, check first)
  if (has_dot) {
    register const unsigned char *ptr = (const unsigned char *)ip_str;
    register const unsigned char *end = ptr + ip_len;  // bounds limit
    register unsigned int dots = 0;
    register unsigned int num = 0;
    register unsigned int digits = 0;
    register size_t octet_start = 0;

    // optimized ipv4 check with minimal branching and bounds checking
    while (ptr < end) {
      if (*ptr == '.') {
        // validate octet format and value
        if (!digits || num > 255) {
          break;
        }

        // check for leading zeros (invalid except single '0')
        if (digits > 1 && ip_str[octet_start] == '0') {
          break;
        }

        if (++dots > 3) {
          break;
        }

        // reset for next octet
        num = 0;
        digits = 0;
        octet_start = ptr - (const unsigned char *)ip_str + 1;
      } else if (*ptr >= '0' && *ptr <= '9') {
        // fast digit processing
        num = (num * 10) + (*ptr - '0');
        if (++digits > 3 || num > 255) {
          break;
        }
      } else {
        // invalid character
        dots = 0;
        break;
      }
      ptr++;
    }

    // final validation: must have 3 dots, valid final octet, and reached end
    if (dots == 3 && digits && num <= 255 && ptr == end) {
      // check for leading zeros in final octet
      if (digits == 1 || ip_str[octet_start] != '0') {
        return 4;
      }
    }

    // if we have colons too, might be ipv6, otherwise invalid
    if (!has_colon) {
      return 0;
    }
  }

  // ipv6 validation
  if (has_colon) {
    register const unsigned char *ptr = (const unsigned char *)ip_str;
    register const unsigned char *end = ptr + ip_len;  // bounds limit
    register unsigned int colons = 0;
    register unsigned int hex_digits = 0;
    register unsigned char double_colon = 0;

    // optimized ipv6 check with bounds checking
    while (ptr < end) {
      if (*ptr == ':') {
        // handle double colon (compression)
        if (ptr + 1 < end && *(ptr + 1) == ':') {
          if (double_colon) {
            return 0;  // only one :: allowed
          }
          double_colon = 1;
          ptr++;  // skip second colon
          colons++;
        }

        // check invalid colon positions with bounds awareness
        if ((hex_digits == 0 && ptr != (const unsigned char *)ip_str &&
             ptr > (const unsigned char *)ip_str && *(ptr - 1) != ':') ||
            (ptr + 1 >= end && !double_colon)) {
          return 0;
        }

        colons++;
        hex_digits = 0;
      }
      // fast hex digit check using bit manipulation
      else {
        unsigned char c = *ptr;
        unsigned char is_hex = ((c >= '0' && c <= '9') ||
                                ((c & 0xDF) >= 'A' && (c & 0xDF) <= 'F'));

        if (!is_hex) {
          return 0;
        }

        if (++hex_digits > 4) {
          return 0;
        }
      }
      ptr++;
    }

    // ipv6 validation rules
    if (colons < 2) {
      return 0;
    }

    // double colon can represent variable number of zero groups
    if (!double_colon && colons != 7) {
      return 0;
    }

    if (double_colon && colons > 7) {
      return 0;
    }

    return 6;
  }

  return 0;  // neither ipv4 nor ipv6
}

/**
 * python wrapper for is_valid_ip function
 *
 * Args:
 *     self (PyObject*): module object
 *     args (PyObject*): python arguments (expects one string)
 *
 * Returns:
 *     PyObject*: 4 for ipv4, 6 for ipv6, 0 for invalid ip
 */
PyObject *c_is_valid_ip(PyObject *self, PyObject *args) {
  const char *ip_str;

  // parse python string argument
  if (!PyArg_ParseTuple(args, "s", &ip_str)) {
    PyErr_Clear();
    return PyLong_FromLong(0);
  }

  // call the c function
  int result = is_valid_ip(ip_str);

  // return result as python integer
  return PyLong_FromLong(result);
}
