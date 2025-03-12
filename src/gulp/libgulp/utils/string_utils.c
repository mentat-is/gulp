#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include <ctype.h>

/**
 * check if string is numeric
 *
 * Args:
 *     str: string to check
 *
 * Returns:
 *     int: 1 if numeric, 0 otherwise
 */
int is_numeric(const char *str)
{
	if (!str || !*str)
	{
		return 0;
	}

	// handle negative numbers
	if (*str == '-')
	{
		str++;
	}

	while (*str)
	{
		if (!isdigit(*str))
		{
			return 0;
		}
		str++;
	}

	return 1;
}

/**
 * convert hexadecimal string to integer
 *
 * Args:
 *     hex_str: hexadecimal string
 *
 * Returns:
 *     long: converted integer value, -1 if conversion fails
 */
long hex_to_int(const char *hex_str)
{
	if (!hex_str || !*hex_str)
	{
		return -1;
	}

	// skip "0x" prefix if present
	if (hex_str[0] == '0' && (hex_str[1] == 'x' || hex_str[1] == 'X'))
	{
		hex_str += 2;
	}

	char *endptr;
	long value = strtol(hex_str, &endptr, 16);

	if (*endptr != '\0')
	{
		return -1; // conversion failed
	}

	return value;
}

/**
 * check if string represents a valid ip address
 *
 * Args:
 *     str: string to check
 *
 * Returns:
 *     int: 1 if valid ip, 0 otherwise
 */
int is_valid_ip(const char *str)
{
	if (!str || !*str)
	{
		return 0;
	}

	int dots = 0;
	int digits = 0;

	while (*str)
	{
		if (*str == '.')
		{
			if (digits == 0 || digits > 3)
			{
				return 0;
			}
			dots++;
			digits = 0;
		}
		else if (isdigit(*str))
		{
			digits++;
		}
		else
		{
			return 0; // invalid character
		}
		str++;
	}

	// must have exactly 3 dots (4 octets) and last octet must have between 1-3 digits
	return (dots == 3 && digits > 0 && digits <= 3);
}
