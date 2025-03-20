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

// difference between chrome epoch (1601-01-01) and unix epoch (1970-01-01)
#define CHROME_TO_UNIX_EPOCH_SECONDS 11644473600LL

// difference between chrome epoch (1601-01-01) and unix epoch (1970-01-01) in
// microseconds
#define CHROME_TO_UNIX_EPOCH_DIFF_MICROSECONDS 11644473600000000ULL

// maximum length of iso8601 string
#define ISO8601_MAX_LEN 64

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
  // basic validation check (minimum length and fast fail)
  if (!str || str[0] == '\0') {
    return false;
  }

  // verify first 10 chars match YYYY-MM-DD pattern
  // directly check without function calls
  if (!(str[0] >= '0' && str[0] <= '9' && str[1] >= '0' && str[1] <= '9' &&
        str[2] >= '0' && str[2] <= '9' && str[3] >= '0' && str[3] <= '9' &&
        str[4] == '-' && str[5] >= '0' && str[5] <= '9' && str[6] >= '0' &&
        str[6] <= '9' && str[7] == '-' && str[8] >= '0' && str[8] <= '9' &&
        str[9] >= '0' && str[9] <= '9')) {
    return false;
  }

  // check for T separator (scan just until we find T or t or reach the end)
  const char* p = str + 10;  // start after YYYY-MM-DD
  while (*p && *p != 'T' && *p != 't') {
    p++;
  }

  // if string ends here, it's a valid date-only iso8601 format
  if (*p == '\0') {
    return true;
  }

  return (*p == 'T' || *p == 't' ||
          (*p == ' '));  // if we reach here, we have a T or t or space, then
                         // time follows ...
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
 */
int64_t iso8601_to_nanos(const char* iso_str) {
  // validate input
  if (!iso_str || !is_iso8601(iso_str)) {
    return -1;
  }

  // parse the base time component
  struct tm tm_info = {0};
  char* parse_success = NULL;

  // iso8601 format with Z: "2025-03-13t15:30:45z"
  parse_success = strptime(iso_str, "%Y-%m-%dT%H:%M:%S", &tm_info);

  if (!parse_success) {
    return -1;
  }

  // convert to seconds since unix epoch
  time_t unix_seconds = timegm(&tm_info);
  if (unix_seconds == -1) {
    return -1;
  }

  // convert seconds to nanoseconds
  int64_t nanos = (int64_t)unix_seconds * SECONDS_TO_NANOSECONDS;

  // handle fractional seconds if present
  char* fractional = strstr(iso_str, ".");
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
  // printf("format_iso8601: %ld, %d\n", timestamp, fraction_ns);
  // check buffer space early
  if (!buffer || buffer_size < 21) {  // minimum size for iso8601 with Z
    return false;
  }

  struct tm tm_info;
  gmtime_r(&timestamp, &tm_info);

  // format the base time (YYYY-MM-DDThh:mm:ss)
  size_t len = strftime(buffer, buffer_size, "%Y-%m-%dT%H:%M:%S", &tm_info);
  if (len == 0) return false;

  // optimized fractional part handling
  if (fraction_ns > 0) {
    // fast path for common cases to avoid loops and divisions
    if (fraction_ns % 100000000 == 0) {
      // 0.1 format
      if (len + 3 >= buffer_size) return false;
      buffer[len++] = '.';
      buffer[len++] = '0' + (fraction_ns / 100000000);
    } else if (fraction_ns % 10000000 == 0) {
      // 0.01 format
      if (len + 4 >= buffer_size) return false;
      buffer[len++] = '.';
      int val = fraction_ns / 10000000;
      buffer[len++] = '0' + (val / 10);
      buffer[len++] = '0' + (val % 10);
    } else if (fraction_ns % 1000000 == 0) {
      // 0.001 format (milliseconds)
      if (len + 5 >= buffer_size) return false;
      buffer[len++] = '.';
      int val = fraction_ns / 1000000;
      buffer[len++] = '0' + (val / 100);
      buffer[len++] = '0' + ((val / 10) % 10);
      buffer[len++] = '0' + (val % 10);
    } else {
      // general case - optimize to avoid loops when possible
      int fraction_digits = 9;
      int temp = fraction_ns;

      // count trailing zeros (optimize common cases)
      if (temp % 1000 == 0) {
        temp /= 1000;
        fraction_digits -= 3;
      }
      if (temp % 10 == 0) {
        while (temp % 10 == 0 && temp > 0) {
          temp /= 10;
          fraction_digits--;
        }
      }

      // format fraction with appropriate precision
      if (fraction_digits > 0) {
        // check buffer space
        if (len + fraction_digits + 2 >= buffer_size) return false;

        buffer[len++] = '.';

        // calculate divisor directly (avoiding loop)
        int divisor;
        switch (fraction_digits) {
          case 1:
            divisor = 100000000;
            break;
          case 2:
            divisor = 10000000;
            break;
          case 3:
            divisor = 1000000;
            break;
          case 4:
            divisor = 100000;
            break;
          case 5:
            divisor = 10000;
            break;
          case 6:
            divisor = 1000;
            break;
          case 7:
            divisor = 100;
            break;
          case 8:
            divisor = 10;
            break;
          default:
            divisor = 1;
        }

        // format directly without snprintf
        int fraction_scaled = fraction_ns / divisor;

        // convert to string directly (faster than snprintf)
        char temp[10];  // max 9 digits + null
        char* ptr = &temp[9];
        *ptr = '\0';
        int digit_count = 0;

        do {
          *--ptr = '0' + (fraction_scaled % 10);
          fraction_scaled /= 10;
          digit_count++;
        } while (fraction_scaled > 0);

        // pad with leading zeros if needed
        while (digit_count < fraction_digits) {
          *--ptr = '0';
          digit_count++;
        }

        // copy to buffer
        const char* src = ptr;
        while (*src) {
          buffer[len++] = *src++;
        }
      }
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

  // find string length and last character in one pass
  size_t len = 0;
  const char* end = iso_str;
  char last_char = '\0';

  while (*end) {
    last_char = *end++;
    len++;
  }

  // empty string check
  if (len == 0) return false;

  // check for Z at the end (most common case first)
  if (last_char == 'Z' || last_char == 'z') {
    return true;
  }

  // check for +00:00 or -00:00 (only if string is long enough)
  if (len >= 6) {
    end -= 6;  // position at potential timezone start
    // check timezone pattern directly without multiple comparisons
    if ((end[0] == '+' || end[0] == '-') && end[1] == '0' && end[2] == '0' &&
        end[3] == ':' && end[4] == '0' && end[5] == '0') {
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
 *     format_str (char*): format string for strptime
 *     buffer_size (size_t): size of buffer
 *
 * Returns:
 *     bool: true if conversion succeeded
 */
bool enforce_utc_timezone(const char* iso_str, const char* format_str,
                          char* buffer, size_t buffer_size) {
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

  parse_success = strptime(iso_str, format_str, &tm_info);
  if (parse_success) {
    // convert to utc time
    time_t timestamp = timegm(&tm_info);
    if (timestamp != -1) {
      // format as iso8601 with utc timezone
      if (format_iso8601(timestamp, 0, buffer, buffer_size)) {
        return true;
      }
    }
  }
  return false;
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
bool numeric_timestamp_to_iso8601(int64_t timestamp, char* buffer,
                                  size_t buffer_size) {
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
 * improved function to extract microseconds from a date string
 * that reliably works with various formats including ones with
 * microseconds in the middle of the string
 *
 * Args:
 *     date_str (const char*): date string to parse
 *     format_str (const char*): format string with microseconds specifier (%f)
 *
 * Returns:
 *     int: microseconds value, or 0 if not found
 */
int extract_microseconds(const char* date_str, const char* format_str) {
  // validate inputs
  if (!date_str || !format_str) {
    return 0;
  }

  // look for %f in format string
  const char* micro_fmt_pos = strstr(format_str, "%f");
  if (!micro_fmt_pos) {
    return 0;  // no microseconds in format
  }

  // find all decimal points followed by digits in the date string
  const char* decimal_point = NULL;
  const char* p = date_str;

  while (*p) {
    // look for a decimal point followed by at least one digit
    if (*p == '.' && isdigit(*(p + 1))) {
      // remember this position as a candidate
      decimal_point = p;
    }
    p++;
  }

  if (!decimal_point) {
    // printf("debug: no decimal point found in: %s\n", date_str);
    return 0;  // no decimal point with digits found
  }

  // extract microseconds (skip the decimal point)
  char micro_str[7] = {0};  // 6 digits + null terminator
  size_t i = 0;
  decimal_point++;  // skip the decimal point

  // extract up to 6 digits for microseconds
  while (i < 6 && isdigit(decimal_point[i])) {
    micro_str[i] = decimal_point[i];
    i++;
  }

  // convert to integer
  int microseconds = atoi(micro_str);

  // pad with zeros if fewer than 6 digits
  for (size_t j = i; j < 6; j++) {
    microseconds *= 10;
  }

  // printf("debug: extracted microseconds: %d from: %s\n", microseconds,
  // date_str);
  return microseconds;
}

/**
 * improved function to create a clean date string without microseconds
 * that handles various date formats reliably
 *
 * Args:
 *     date_str (const char*): original date string with microseconds
 *     clean_date (char*): buffer to store result without microseconds
 *     buffer_size (size_t): size of the clean_date buffer
 *
 * Returns:
 *     bool: true if successful, false otherwise
 */
bool create_clean_date_string(const char* date_str, char* clean_date,
                              size_t buffer_size) {
  // validate inputs
  if (!date_str || !clean_date || buffer_size < 2) {
    return false;
  }

  // check if we have enough buffer space
  if (strlen(date_str) >= buffer_size) {
    return false;
  }

  // initialize the clean buffer
  *clean_date = '\0';

  // find the decimal point for microseconds
  const char* decimal_point = NULL;
  const char* p = date_str;

  while (*p) {
    if (*p == '.' && isdigit(*(p + 1))) {
      decimal_point = p;
      break;
    }
    p++;
  }

  if (!decimal_point) {
    // no microseconds found, just copy the original
    strcpy(clean_date, date_str);
    return true;
  }

  // copy the part before the decimal point
  size_t prefix_len = decimal_point - date_str;
  if (prefix_len >= buffer_size) {
    return false;
  }

  strncpy(clean_date, date_str, prefix_len);
  clean_date[prefix_len] = '\0';

  // find the end of microseconds (stop at first non-digit)
  const char* micro_end = decimal_point + 1;
  while (isdigit(*micro_end)) {
    micro_end++;
  }

  // append everything after the microseconds
  if (strlen(clean_date) + strlen(micro_end) + 1 > buffer_size) {
    return false;
  }

  strcat(clean_date, micro_end);

  // printf("debug: cleaned date string: \"%s\" from: \"%s\"\n", clean_date,
  // date_str);
  return true;
}

/**
 * normalizes format strings to handle common mismatches like 'T' vs space and
 * 'Z' vs named timezones
 *
 * Args:
 *     format_str (const char*): original format string
 *     date_str (const char*): date string being parsed
 *     normalized_format (char*): buffer to store normalized format
 *     buffer_size (size_t): size of the normalized_format buffer
 *
 * Returns:
 *     bool: true if normalization was done, false if no changes needed
 */
bool normalize_format_string(const char* format_str, const char* date_str,
                             char* normalized_format, size_t buffer_size) {
  // validate inputs
  if (!format_str || !date_str || !normalized_format || buffer_size < 2) {
    return false;
  }

  bool changes_made = false;

  // copy format while normalizing
  char* dst = normalized_format;
  for (const char* src = format_str;
       *src && dst - normalized_format < (buffer_size - 1); src++) {
    // handle 'T' vs space in date-time separator
    if (*src == 'T') {
      // find corresponding position in date string
      size_t pos = src - format_str;
      const char* date_pos = date_str;
      size_t count = 0;

      // skip to corresponding position in date string
      while (*date_pos && count < pos) {
        date_pos++;
        count++;
      }

      // if date has a space where format has 'T', use space in normalized
      // format
      if (*date_pos == ' ') {
        *dst++ = ' ';
        changes_made = true;
      } else {
        *dst++ = 'T';
      }
    } else {
      *dst++ = *src;
    }
  }

  // null terminate
  *dst = '\0';

  // debug output
  /*if (changes_made) {
    printf("debug: normalized format: \"%s\" from \"%s\"\n", normalized_format,
  format_str);
  }*/

  return changes_made;
}

/**
 * improved function to parse a date with microseconds
 * that handles a wider variety of formats reliably
 *
 * Args:
 *     date_str (const char*): the date string to parse
 *     format_str (const char*): the format string with possible %f
 *     tm_info (struct tm*): struct to fill with parsed time
 *     microseconds_out (int*): output parameter for microseconds
 *
 * Returns:
 *     bool: true if parsing succeeded, false otherwise
 */
bool parse_date_with_microseconds(const char* date_str, const char* format_str,
                                  struct tm* tm_info, int* microseconds_out) {
  // validate inputs
  if (!date_str || !format_str || !tm_info || !microseconds_out) {
    return false;
  }

  // default microseconds to 0
  *microseconds_out = 0;

  // check if format contains microseconds specifier
  const char* micro_fmt = strstr(format_str, "%f");
  if (!micro_fmt) {
    // no microseconds in format, just parse normally
    char* result = strptime(date_str, format_str, tm_info);
    // printf("debug: direct parse result: %s\n", result ? "success" :
    // "failure");

    // if direct parse failed, try with normalized format
    if (!result) {
      char normalized_format[64] = {0};
      if (normalize_format_string(format_str, date_str, normalized_format,
                                  sizeof(normalized_format))) {
        result = strptime(date_str, normalized_format, tm_info);
        // printf("debug: parse with normalized format result: %s\n", result ?
        // "success" : "failure");
      }
    }

    return result != NULL;
  }

  // extract microseconds first
  *microseconds_out = extract_microseconds(date_str, format_str);

  // create a cleaned format string - removing both dot and %f
  char clean_format[64] = {0};
  if (strlen(format_str) >= sizeof(clean_format)) {
    return false;  // format string too long
  }

  // handle format string cleaning with special care for the decimal point
  char* format_ptr = clean_format;
  for (const char* p = format_str; *p; p++) {
    // check if we're at a decimal point followed by %f
    if (*p == '.' && p[1] == '%' && p[2] == 'f') {
      // skip the dot and %f (next iteration will skip past 'f')
      p += 2;
      continue;
    }
    // check if we're at %f without a preceding dot
    else if (*p == '%' && p[1] == 'f') {
      // skip the %f
      p++;
      continue;
    }
    *format_ptr++ = *p;
  }
  *format_ptr = '\0';

  // create a clean date string without the microseconds
  char clean_date[ISO8601_MAX_LEN] = {0};
  if (!create_clean_date_string(date_str, clean_date, sizeof(clean_date))) {
    return false;
  }

  // printf("debug: parse with format: \"%s\", clean date: \"%s\"\n",
  // clean_format, clean_date);

  // normalize the cleaned format to handle T/space and Z/timezone mismatches
  char normalized_format[64] = {0};
  bool has_normalized = normalize_format_string(
      clean_format, clean_date, normalized_format, sizeof(normalized_format));

  // try parsing with the normalized format if we created one
  if (has_normalized) {
    char* result = strptime(clean_date, normalized_format, tm_info);
    // printf("debug: strptime with normalized format result: %s\n", result ?
    // "success" : "failure");
    if (result != NULL) {
      return true;
    }
  }

  // fall back to original clean format if normalization failed or wasn't needed
  char* result = strptime(clean_date, clean_format, tm_info);
  // printf("debug: strptime result: %s\n", result ? "success" : "failure");

  return result != NULL;
}

/**
 * tries parsing a date string with multiple common date formats
 * using improved microseconds handling
 *
 * Args:
 *     date_str (const char*): the date string to parse
 *     buffer (char*): buffer to store the result
 *     buffer_size (size_t): size of the buffer
 *
 * Returns:
 *     bool: true if parsing succeeded with any format, false otherwise
 */
bool try_parse_with_common_formats(const char* date_str, char* buffer,
                                   size_t buffer_size) {
  // array of common date format strings to try
  const char* formats[] = {
      // formats with common space separators and named timezones - try first
      "%Y-%m-%d %H:%M:%S %Z",  // with timezone name: "2016-06-29 15:25:08 UTC"
      "%Y-%m-%d %H:%M:%S.%f %Z",  // with timezone name and fractions:
                                  // "2016-06-29 15:25:08.822 UTC"
      "%a %b %d %H:%M:%S %Y",     // rfc 2822: "thu mar 13 15:30:45 2025"
      "%a %b %d %H:%M:%S.%f %Y",  // rfc 2822 with fractions: "thu mar 13
                                  // 15:30:45.123 2025"
      "%a %b %d %H:%M:%S %Z %Y",  // variant with timezone: "tue jan 12 15:21:39
                                  // UTC 2021"
      "%a %b %d %H:%M:%S.%f %Z %Y",  // variant with timezone and fractions:
                                     // "tue jan 12 15:21:39.355083 UTC 2021"

      // common space-separated formats
      "%Y-%m-%d %H:%M:%S",     // common format: "2025-03-13 15:30:45"
      "%Y-%m-%d %H:%M:%S.%f",  // common format with fractions: "2025-03-13
                               // 15:30:45.123"

      // ISO 8601 formats with T separators
      "%Y-%m-%dT%H:%M:%S",       // iso 8601: "2025-03-13T15:30:45"
      "%Y-%m-%dT%H:%M:%SZ",      // iso 8601 with Z: "2025-03-13T15:30:45Z"
      "%Y-%m-%dT%H:%M:%S.%f",    // iso 8601 with fractional seconds:
                                 // "2025-03-13T15:30:45.123456"
      "%Y-%m-%dT%H:%M:%S.%fZ",   // iso 8601 with fractional seconds and Z:
                                 // "2025-03-13T15:30:45.123456Z"
      "%Y-%m-%dT%H:%M:%S%z",     // rfc 3339: "2025-03-13T15:30:45+00:00"
      "%Y-%m-%dT%H:%M:%S.%f%z",  // rfc 3339 with fractional seconds:
                                 // "2025-03-13T15:30:45.123+00:00"

      // other common formats
      "%m/%d/%Y %H:%M:%S",    // us date with time: "03/13/2025 15:30:45"
      "%m/%d/%Y %H:%M:%S.%f"  // us date with time and fractions:
                              // "03/13/2025 15:30:45.123456"};
  };
  // get number of formats to try
  int num_formats = sizeof(formats) / sizeof(formats[0]);

  // try each format until one succeeds
  for (int i = 0; i < num_formats; i++) {
    struct tm tm_info = {0};
    int microseconds = 0;

    // parse date with improved microseconds handling
    bool success = parse_date_with_microseconds(date_str, formats[i], &tm_info,
                                                &microseconds);

    // printf("debug: try_parse_with_common_formats: %s, format: %s,
    // success=%s\n", date_str, formats[i], success ? "yes" : "no");

    if (success) {
      // if parsing time-only format, fill in today's date
      if (strstr(formats[i], "%Y") == NULL &&
          strstr(formats[i], "%y") == NULL) {
        time_t now = time(NULL);
        struct tm* today = localtime(&now);
        tm_info.tm_year = today->tm_year;
        tm_info.tm_mon = today->tm_mon;
        tm_info.tm_mday = today->tm_mday;
      }

      // convert to utc time
      time_t unix_seconds = timegm(&tm_info);
      if (unix_seconds != -1) {
        // handle fractional seconds
        int fraction_ns =
            microseconds * 1000;  // convert microseconds to nanoseconds

        // format the result
        return format_iso8601(unix_seconds, fraction_ns, buffer, buffer_size);
      }
    }
  }

  return false;
}

/**
 * detects timezone information in a date string
 *
 * Args:
 *     date_str (const char*): the date string to check
 *     has_tz_out (bool*): output parameter to store if timezone was found
 *     is_utc_out (bool*): output parameter to store if timezone is utc
 *
 * Returns:
 *     bool: true if processing succeeded
 */
bool detect_timezone_info(const char* date_str, bool* has_tz_out,
                          bool* is_utc_out) {
  // validate inputs
  if (!date_str || !has_tz_out || !is_utc_out) {
    return false;
  }

  *has_tz_out = false;
  *is_utc_out = false;

  // get string length
  size_t len = strlen(date_str);
  if (len < 3) {
    return true;  // too short for timezone, but not an error
  }

  // check for common timezone indicators

  // check for named utc timezone at the end (UTC or GMT)
  if (len >= 3) {
    const char* end = date_str + len - 3;
    if ((end[0] == 'U' || end[0] == 'u') && (end[1] == 'T' || end[1] == 't') &&
        (end[2] == 'C' || end[2] == 'c')) {
      *has_tz_out = true;
      *is_utc_out = true;
      return true;
    }

    if ((end[0] == 'G' || end[0] == 'g') && (end[1] == 'M' || end[1] == 'm') &&
        (end[2] == 'T' || end[2] == 't')) {
      *has_tz_out = true;
      *is_utc_out = true;
      return true;
    }
  }

  // check for Z at the end (iso8601 utc indicator)
  if (date_str[len - 1] == 'Z' || date_str[len - 1] == 'z') {
    *has_tz_out = true;
    *is_utc_out = true;
    return true;
  }

  // check for +00:00 or -00:00 (utc timezone offset)
  if (len >= 6) {
    const char* tzpart = date_str + len - 6;
    if ((tzpart[0] == '+' || tzpart[0] == '-') && tzpart[1] == '0' &&
        tzpart[2] == '0' && tzpart[3] == ':' && tzpart[4] == '0' &&
        tzpart[5] == '0') {
      *has_tz_out = true;
      *is_utc_out = true;
      return true;
    }

    // check for non-zero timezone offset
    if ((tzpart[0] == '+' || tzpart[0] == '-') && isdigit(tzpart[1]) &&
        isdigit(tzpart[2]) && tzpart[3] == ':' && isdigit(tzpart[4]) &&
        isdigit(tzpart[5])) {
      *has_tz_out = true;
      // not utc if it's not +00:00/-00:00
      *is_utc_out = false;
      return true;
    }
  }

  return true;  // no timezone found, but not an error
}

/**
 * parses a date string using a specified format and converts it to iso8601
 * string with utc timezone
 * supports custom format strings including formats with microseconds
 *
 * Args:
 *     date_str (const char*): the date string to parse
 *     format_str (const char*): the format string to use with strptime (can be
 *                              NULL for auto-detection)
 *     buffer (char*): buffer to store the result
 *     buffer_size (size_t): size of the buffer
 *
 * Returns:
 *     bool: true if parsing succeeded, false otherwise
 */
bool parse_date_with_format_to_iso8601(const char* date_str,
                                       const char* format_str, char* buffer,
                                       size_t buffer_size) {
  // printf("parse_date_with_format_to_iso8601: %s, format_str: %s\n", date_str,
  // format_str);

  // validate inputs
  if (!date_str || !buffer || buffer_size < ISO8601_MAX_LEN) {
    return false;
  }

  // check if the string is all digits (could be a timestamp)
  bool is_numeric = true;
  size_t len = strlen(date_str);
  for (size_t i = 0; i < len; i++) {
    if (!isdigit(date_str[i])) {
      is_numeric = false;
      break;
    }
  }

  if (is_numeric) {
    // handle numeric timestamps
    int64_t timestamp = atoll(date_str);
    return numeric_timestamp_to_iso8601(timestamp, buffer, buffer_size);
  }

  // check if string has timezone information
  bool has_timezone = false;
  bool is_utc = false;
  detect_timezone_info(date_str, &has_timezone, &is_utc);

  // if format_str is NULL, try multiple common formats
  if (format_str == NULL) {
    return try_parse_with_common_formats(date_str, buffer, buffer_size);
  }

  // parse using the provided format with improved microseconds handling
  struct tm tm_info = {0};
  int microseconds = 0;

  bool parse_success = parse_date_with_microseconds(date_str, format_str,
                                                    &tm_info, &microseconds);

  if (parse_success) {
    // if parsing time-only format, fill in today's date
    if (format_str && strstr(format_str, "%Y") == NULL &&
        strstr(format_str, "%y") == NULL) {
      time_t now = time(NULL);
      struct tm* today = localtime(&now);
      tm_info.tm_year = today->tm_year;
      tm_info.tm_mon = today->tm_mon;
      tm_info.tm_mday = today->tm_mday;
    }

    // convert to UTC time
    time_t unix_seconds = timegm(&tm_info);
    if (unix_seconds != -1) {
      int fraction_ns =
          microseconds * 1000;  // convert microseconds to nanoseconds

      // format the result using existing utility
      return format_iso8601(unix_seconds, fraction_ns, buffer, buffer_size);
    }
  }

  return false;
}

/**
 * c implementation of ensure_iso8601 to convert various time formats to iso8601
 *
 * Args:
 *     self (PyObject*): module object
 *     args (PyObject*): positional arguments (time_str, format_str)
 *
 * Returns:
 *     PyObject*: iso8601 formatted string, or None on failure
 *
 */
PyObject* c_ensure_iso8601(PyObject* self, PyObject* args) {
  PyObject* time_str_obj;
  PyObject* format_obj = NULL;
  const char* format_str = NULL;

  // parse arguments: time_str and optional format_obj
  if (!PyArg_ParseTuple(args, "O|O", &time_str_obj, &format_obj)) {
    // printf("error parsing arguments!\n");
    PyErr_Clear();
    Py_RETURN_NONE;
  }

  // convert format_obj to format_str if it's a string and not None
  if (format_obj != NULL && format_obj != Py_None) {
    if (PyUnicode_Check(format_obj)) {
      format_str = PyUnicode_AsUTF8(format_obj);
    } else {
      // printf("format_str must be a string or None\n");
      Py_RETURN_NONE;
    }
  }

  // handle numeric timestamp as int
  if (PyLong_Check(time_str_obj)) {
    uint64_t numeric = PyLong_AsLongLong(time_str_obj);
    if (PyErr_Occurred()) {
      // conversion failure, clear the error
      PyErr_Clear();
      Py_RETURN_NONE;
    }

    // allocate result buffer
    char* result = (char*)malloc(ISO8601_MAX_LEN);
    if (!result) {
      Py_RETURN_NONE;
    }

    // convert to iso8601 using numeric handling
    if (numeric_timestamp_to_iso8601(numeric, result, ISO8601_MAX_LEN)) {
      // create python string
      PyObject* py_result = PyUnicode_FromString(result);
      free(result);
      return py_result;
    } else {
      free(result);
      Py_RETURN_NONE;
    }
  } else if (PyUnicode_Check(time_str_obj)) {
    // handle string input
    const char* time_str = PyUnicode_AsUTF8(time_str_obj);
    if (!time_str) {
      // pyunicode_asutf8 sets an exception
      Py_RETURN_NONE;
    }
    // printf("c_ensure_iso8601: time_str=%s, format_str=%s\n", time_str,
    // format_str); maximum length check to prevent malicious inputs
    if (strlen(time_str) > ISO8601_MAX_LEN) {
      // string too long, max size=iso8601_max_len
      Py_RETURN_NONE;
    }

    // allocate result buffer
    char* result = (char*)malloc(ISO8601_MAX_LEN);
    if (!result) {
      Py_RETURN_NONE;
    }

    // convert to iso8601 with the specified format
    if (parse_date_with_format_to_iso8601(time_str, format_str, result,
                                          ISO8601_MAX_LEN)) {
      // create python string
      PyObject* py_result = PyUnicode_FromString(result);
      free(result);
      return py_result;
    } else {
      free(result);
      Py_RETURN_NONE;
    }
  }

  // failure, time_str must be a string or integer
  Py_RETURN_NONE;
}

/**
 * converts various numeric timestamp formats to nanoseconds since unix epoch
 *
 * supports detection and conversion of:
 * - nanoseconds since epoch (large values > 10^18)
 * - microseconds since epoch (values around 10^15-10^16)
 * - milliseconds since epoch (values around 10^12-10^13)
 * - seconds since epoch (values < 10^10)
 *
 * Args:
 *     self (PyObject*): module object
 *     args (PyObject*): positional arguments (numeric timestamp)
 *
 * Returns:
 *     PyObject*: nanoseconds from unix epoch as python integer, or 0 on failure
 */
PyObject* c_number_to_nanos_from_unix_epoch(PyObject* self, PyObject* args) {
  PyObject* numeric_obj = NULL;

  // parse "numeric" argument
  if (!PyArg_ParseTuple(args, "O", &numeric_obj)) {
    PyErr_Clear();
    Py_RETURN_NONE;
  }

  long long result = 0;

  // handle string input by converting to number
  if (PyUnicode_Check(numeric_obj)) {
    const char* num_str = PyUnicode_AsUTF8(numeric_obj);
    if (num_str == NULL) {
      // invalid string format
      return PyLong_FromLong(0);
    }

    char* endptr;
    // convert string to python long integer
    numeric_obj = PyLong_FromString(num_str, &endptr, 10);
  }

  if (PyLong_Check(numeric_obj)) {
    // convert input to a python integer
    PyObject* numeric_int = PyNumber_Index(numeric_obj);
    if (numeric_int == NULL) {
      if (PyErr_Occurred()) {
        // conversion failure raises an exception, so we clear it
        PyErr_Clear();
      }
      return PyLong_FromLong(0);
    }

    // extract the integer value as a long long
    long long numeric = PyLong_AsLongLong(numeric_int);

    // release the temporary integer object
    Py_DECREF(numeric_int);

    if (PyErr_Occurred()) {
      // conversion failure raises an exception, so we clear it
      PyErr_Clear();
      return PyLong_FromLong(0);
    }

    if (numeric < 0) {
      // negative timestamp values are not valid
      return PyLong_FromLong(0);
    }

    // determine format based on magnitude
    if (numeric > 1000000000000000000LL) {
      // nanoseconds (> 10^18) - already in the correct format
      result = numeric;
    } else if (numeric > 1000000000000000LL) {
      // microseconds (> 10^15) - multiply by 1000 to get nanoseconds
      result = numeric * MICROSECONDS_TO_NANOSECONDS;
    } else if (numeric > 1000000000000LL) {
      // milliseconds (> 10^12) - multiply by 1,000,000 to get nanoseconds
      result = numeric * MILLISECONDS_TO_NANOSECONDS;
    } else {
      // seconds (typical unix timestamp) - multiply by 1,000,000,000 to get
      // nanoseconds
      result = numeric * SECONDS_TO_NANOSECONDS;
    }
  } else {
    // unsupported input format (not a number or string)
    return PyLong_FromLong(0);
  }

  // return result as python integer
  return PyLong_FromLongLong(result);
}

/**
 * c implementation of string_to_nanos_from_unix_epoch to convert various time
 * formats to nanos from unix epoch
 *
 * Args:
 *     self (PyObject*): module object
 *     args (PyObject*): positional arguments (time_str)
 *
 * Returns:
 *     PyObject*: nanoseconds from unix epoch as python integer, or 0 on failure
 * (invalid input date)
 *
 */
PyObject* c_string_to_nanos_from_unix_epoch(PyObject* self, PyObject* args) {
  // convert to iso8601 first, then to nanos
  PyObject* iso8601 = c_ensure_iso8601(self, args);
  if (iso8601 == NULL) {
    // c_ensure_iso8601 should have set an exception already
    return PyLong_FromLong(0);
  }

  // ensure we have a unicode string
  if (!PyUnicode_Check(iso8601)) {
    // invalid string
    Py_DECREF(iso8601);
    return PyLong_FromLong(0);
  }

  // extract c string
  const char* time_str = PyUnicode_AsUTF8(iso8601);
  if (time_str == NULL) {
    // invalid string
    Py_DECREF(iso8601);
    return PyLong_FromLong(0);
  }

  // convert to nanoseconds
  int64_t nanos = iso8601_to_nanos(time_str);

  // clean up the iso8601 string, we don't need it anymore
  Py_DECREF(iso8601);

  // check if conversion was successful
  if (nanos == -1) {
    // invalid string
    return PyLong_FromLong(0);
  }

  // return nanoseconds as python integer
  return PyLong_FromLongLong(nanos);
}

/**
 * converts a chrome timestamp to nanoseconds since unix epoch
 *
 * chrome timestamps are microseconds since 1601-01-01 00:00:00 UTC
 * this function converts them to nanoseconds since 1970-01-01 00:00:00 UTC
 *
 * Args:
 *     timestamp: chrome timestamp in microseconds
 *
 * Returns:
 *     nanoseconds since unix epoch, or 0 on error
 */
PyObject* c_chrome_epoch_to_nanos_from_unix_epoch(PyObject* self,
                                                  PyObject* args) {
  unsigned long long timestamp;

  // parse the python arguments
  if (!PyArg_ParseTuple(args, "K", &timestamp)) {
    // invalid input
    PyErr_Clear();
    return PyLong_FromLong(0);
  }

  // check for potential underflow
  if (timestamp < CHROME_TO_UNIX_EPOCH_DIFF_MICROSECONDS) {
    // invalid input
    return PyLong_FromLong(0);
  }

  // convert from chrome epoch to unix epoch (microseconds)
  unsigned long long unix_microseconds =
      timestamp - CHROME_TO_UNIX_EPOCH_DIFF_MICROSECONDS;

  // convert from microseconds to nanoseconds
  unsigned long long unix_nanoseconds = unix_microseconds * 1000ULL;

  // return result as python integer
  return PyLong_FromUnsignedLongLong(unix_nanoseconds);
}

/**
 * python wrapper for is_iso8601 c function
 *
 * checks if a string is in iso8601 format
 *
 * Args:
 *     self (PyObject*): module object (unused)
 *     args (PyObject*): arguments from python
 *
 * Returns:
 *     PyObject*: returns True if the string is in iso8601 format
 * otherwise
 *
 */
PyObject* c_is_iso8601(PyObject* self, PyObject* args) {
  // declare variable to hold the string
  const char* date_str = NULL;

  // parse the input argument as a string
  if (!PyArg_ParseTuple(args, "s", &date_str)) {
    // if parsing fails, return false
    PyErr_Clear();
    Py_RETURN_FALSE;
  }

  // call the c function
  bool result = is_iso8601(date_str);

  // convert c bool to python bool and return
  if (result) {
    Py_RETURN_TRUE;
  }
  Py_RETURN_FALSE;
}
