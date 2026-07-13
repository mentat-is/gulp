"""
dynamic loading of modules

TODO: module encryption/license manager
"""

import importlib
import importlib.util
import os
import sys
import muty.string
from types import ModuleType

from muty.log import MutyLogger


def load_dynamic_module_from_file(
    module_name: str, path: str, add_to_sys_modules: bool = True
) -> ModuleType:
    """
    Load a dynamic module from a file.

    Args:
        module_name (str): The name of the module.
        path (str): The path to the file containing the module.
        add_to_sys_modules (bool, optional): If True (default), add the module to sys.modules

    Returns:
        ModuleType: The loaded module.
    """
    spec = importlib.util.spec_from_file_location(module_name, path)
    mod = importlib.util.module_from_spec(spec)
    MutyLogger.get_instance().debug(
        "loading module:%s, module_name=%s" % (mod, module_name)
    )
    # add the module to the sys.modules dictionary (required for pickle)
    if add_to_sys_modules:
        sys.modules[module_name] = mod

    spec.loader.exec_module(mod)
    # print('loaded module:%s, module_name=%s' % (mod, module_name))
    return mod


def load_dynamic_module_from_buffer(
    module_name: str, buffer: bytes, add_to_sys_modules: bool = True
) -> ModuleType:
    """
    Load a dynamic module from a buffer.

    Args:
        module_name (str): The name of the module.
        buffer (bytes): The buffer containing the module code.
        add_to_sys_modules (bool, optional): If True (default), add the module to sys.modules
    Returns:
        ModuleType: The loaded module.
    """
    sp = importlib.util.spec_from_loader(module_name, loader=None)
    md = importlib.util.module_from_spec(sp)
    if add_to_sys_modules:
        sys.modules[sp.name] = md

    exec(buffer, md.__dict__)
    return md
