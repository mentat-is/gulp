"""
String utilities.
"""

import codecs
import random
import re
import time
import uuid


def make_shorter(s: str, max_len: int = 50) -> str:
    """
    Shorten a string to a maximum length, adding an ellipsis if it is shortened.

    Args:
        s (str): The input string.
        max_len (int, optional): The maximum length of the string. Defaults to 50.

    Returns:
        str: The shortened string.
    """
    if s is None:
        return ""

    len_s = len(s)
    if len_s <= max_len:
        return s

    # build string with ellipsis and truncated number of characters
    ss = s[:max_len]
    truncated_len = len_s - len(ss)
    if truncated_len > 0:
        ss += f"(...+{truncated_len})"
    return ss


def ensure_no_space_no_special(s: str, lowercase: bool = True) -> str:
    """
    Ensure that a string contains no spaces (replaced by "_") or special characters (removed).

    Args:
        s (str): The input string.
        lowercase (bool, optional): Whether to convert the string to lowercase. Defaults to True.

    Returns:
        str: The modified string.
    """
    if lowercase:
        s = s.lower()
    s = s.replace(" ", "_")
    s = "".join([c for c in s if c.isalnum() or c == "_"])
    return s


def remove_unicode_bom(s: str, unenclose: bool = False) -> str:
    """
    Removes the Unicode Byte Order Mark (BOM) from the beginning of a string, if present.

    Args:
        s (str): The input string.
        unenclose (bool, optional): Whether to remove enclosing characters " and ' at position 0 and (len-1), only effective if the string is enclosed. Defaults to False.

    Returns:
        str: The string with the Unicode BOM removed, if present.
    """
    if s.startswith(codecs.BOM_UTF8.decode("utf8")):
        s = s.lstrip(codecs.BOM_UTF8.decode("utf8"))
    elif s.startswith(codecs.BOM_UTF16_LE.decode("utf-16-le")):
        s = s.lstrip(codecs.BOM_UTF16_LE.decode("utf-16-le"))
    elif s.startswith(codecs.BOM_UTF16_BE.decode("utf-16-be")):
        s = s.lstrip(codecs.BOM_UTF16_BE.decode("utf-16-be"))

    if unenclose and len(s) > 2 and s[0] == s[-1] and s[0] in ['"', "'"]:
        s = s[1:-1]
    return s


def generate_unique(pre: str = None, post: str = None, use_uuid: bool = True) -> str:
    """
    Generate a unique string using either uuid4 or time + random.

    Args:
        pre (str, optional): Prefix to add to the generated string. Defaults to None.
        post (str, optional): Suffix to add to the generated string. Defaults to None.
        use_uuid (bool, optional): Whether to use uuid or time + random. Defaults to True.

    Returns:
        str: The generated unique string.
    """
    if use_uuid:
        # use the uuid lib
        ms = str(uuid.uuid4())
    else:
        # use time + random
        ms = str(time.time_ns() + random.randint(1, 64000) + random.randint(1, 64000))
    if pre is None and post is None:
        return ms

    # add pre and/or post
    s = ""
    if pre is not None:
        s += pre

    s += ms
    if post is not None:
        s += post

    return s


def escape(s: str, to_escape: list[str]) -> str:
    """
    Escape a string by replacing the characters in to_escape with their escaped counterparts.

    Args:
        s (str): The string to be escaped.
        to_escape (list[str]): The characters to be escaped.

    Returns:
        str: The escaped string.
    """
    for c in to_escape:
        s = s.replace(c, "\\" + c)
    return s


def enclose(s: str, enclosure: str = '"') -> str:
    """
    Enclose a string with a specified enclosure.

    Args:
        s (str): The string to be enclosed.
        enclosure (str, optional): The enclosure to use (default=").

    Returns:
        str: The enclosed string.
    """
    return f"{enclosure}{s}{enclosure}"


def to_camel_case(name: str) -> str:
    """
    Convert a string to CamelCase (i.e. turn "hello world"/"hello_world" into "HelloWorld")

    Args:
        name (str): The input string.
    Returns:
        str: The CamelCase string.
    """
    return re.sub(r"(?:^|[-_])([a-zA-Z0-9])", lambda m: m.group(1).upper(), name)
