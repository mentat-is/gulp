"""
xml utility functions
"""

import lxml.etree
import xmltodict
from lxml.etree import Element


def to_dict(s: str) -> dict:
    """
    Convert an XML string to a dictionary.

    Args:
        s (str): The XML string to be converted.

    Returns:
        dict: The resulting dictionary.

    """
    return xmltodict.parse(s)


def strip_namespace(s: str) -> str:
    """
    Strips the namespace from a given XML element name.

    Args:
        s (str): The XML element name.

    Returns:
        str: The element name without the namespace.

    """
    try:
        return lxml.etree.QName(s).localname
    except:
        return s


def child_node_text(e: Element, tag: str) -> str:
    """
    Retrieves the text content of the first child node with the specified tag.

    Args:
        e (Element): The parent element.
        tag (str): The tag name of the child node.

    Returns:
        str: The text content of the first child node with the specified tag.
    """
    return e.xpath('.//*[local-name()="%s"]/text()' % (tag))[0]


def child_node(e: Element, tag: str) -> Element:
    """
    Find and return the first child element with the specified tag name.

    Args:
        e (Element): The parent element to search within.
        tag (str): The tag name of the child element to find.

    Returns:
        Element: The first child element with the specified tag name.

    Raises:
        IndexError: If no child element with the specified tag name is found.
    """
    return e.xpath('.//*[local-name()="%s"]' % (tag))[0]


def child_attrib(e: Element, tag: str, a: str) -> str:
    """
    Retrieves the value of the specified attribute from the child element with the given tag.

    Args:
        e (Element): The parent element.
        tag (str): The tag of the child element.
        a (str): The attribute name.

    Returns:
        str: The value of the specified attribute.

    Raises:
        KeyError: If the child element with the given tag does not exist.
        KeyError: If the specified attribute does not exist in the child element.
    """
    return str(child_node(e, tag).attrib[a])
