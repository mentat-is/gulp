/**
 * 
 */
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
bool format_iso8601(time_t timestamp, int fraction_ns, char *buffer,
                    size_t buffer_size) {
  // check buffer space early
  if (!buffer || buffer_size < 21) {  // minimum size for iso8601 with Z
    return false;
  }

  struct tm tm_info;

#ifdef _WIN32
  gmtime_s(&tm_info, &timestamp);
#else
  gmtime_r(&timestamp, &tm_info);
#endif

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
        char *ptr = &temp[9];
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
        const char *src = ptr;
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
bool has_utc_timezone(const char *iso_str) {
  if (!iso_str) return false;

  // find string length and last character in one pass
  size_t len = 0;
  const char *end = iso_str;
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
 * checks if a string is in iso8601 format
 *
 * Args:
 *     str (const char*): string to check
 *
 * Returns:
 *     bool: true if string is in iso8601 format
 */
bool is_iso8601(const char *str) {
  // basic validation check (minimum length and fast fail)
  if (!str || str[0] == '\0') return false;

  const size_t min_len = 10;  // YYYY-MM-DD

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
  const char *p = str + 10;  // start after YYYY-MM-DD
  while (*p && *p != 'T' && *p != 't') {
    p++;
  }

  return (*p == 'T' || *p == 't');
}