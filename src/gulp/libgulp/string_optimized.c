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
        result <<= 4; // multiply by 16
        
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
 * check if string represents a valid ip address
 *
 * Args:
 *     str (const char*): string to check
 *
 * Returns:
 *     int: 1 if valid ip, 0 otherwise
 */
int is_valid_ip(const char *str) {
    // fast fail for null or empty strings
    if (!str || !*str) {
        return 0;
    }

    // cache values for speed
    const char *p = str;
    int dots = 0;
    int octet_val = 0;
    int digits = 0;
    char c;

    // process each character only once
    while ((c = *p++)) {
        if (c == '.') {
            // validate octet: must be between 0-255 and have 1-3 digits
            if (digits == 0 || digits > 3 || octet_val > 255) {
                return 0;
            }
            dots++;
            octet_val = 0; // reset for next octet
            digits = 0;    // reset digit count
        }
        // check if digit without calling isdigit()
        else if (c >= '0' && c <= '9') {
            // calculate octet value on the fly
            octet_val = octet_val * 10 + (c - '0');
            digits++;
        }
        else {
            return 0; // invalid character
        }
    }

    // validate final octet and total dot count
    return (dots == 3 && digits > 0 && digits <= 3 && octet_val <= 255);
}