#ifndef __STRING_UTILS_H__
#define __STRING_UTILS_H__

#ifdef __cplusplus
extern "C"
{
#endif

	int is_numeric(const char *str);
	long hex_to_int(const char *hex_str);
	int is_valid_ip(const char *str);

#ifdef __cplusplus
}
#endif
#endif