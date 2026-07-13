"""
list utilities
"""

def split_list_in_n_lists(l: list, n: int = 1) -> list[list]:
    """
    splits a list in multple lists with max n elements each

    Args:
        l (list): The list to split.
        n (int, optional): The max number of elements per list. Defaults to 1. if n == 0, returns an array with the original list as the sole element.

    Returns:
        list: A list of lists.
    """
    if len(l) < n or n == 0:
        return [l]
    return [l[i : i + n] for i in range(0, len(l), n)]
