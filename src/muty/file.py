"""
file utilities

TODO: maybe try to avoid code duplication for async/sync functions, if possible,
without adding stuff like nest_asyncio which would introduce instability.
"""

import asyncio
import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import AsyncIterable
from zipfile import ZipFile

import aiofiles
import aiofiles.os
import aiofiles.ospath
import aioshutil
from async_unzip.unzipper import unzip as unzip_async

import muty.log
from muty.log import MutyLogger


async def write_temporary_file_async(content: any, suffix: str = None) -> str:
    """
    Write content to a temporary file (async).

    Args:
        content (any): The content to write to the file.
        suffix (str, optional): The suffix of the temporary file. Defaults to None.

    Returns:
        str: The path of the temporary file.
    """
    fd, path = tempfile.mkstemp(suffix=suffix)
    os.close(fd)
    await write_file_async(path, content)
    return path


def safe_path_join(root: str, *segments, allow_relative=False):
    """Safely joins a path starting from `root` with all `*segments`.
    If resulting path escapes `root`, `root` is returned instead.
    `root` is ALWAYS treated as an absolute, normalized path.

    If `allow_relative` is set to True, it allows joining paths from `root` down, else
    only the last segment's basename is considered.

    Args:
        root (str): directory root
        *segments (str): parts of path to join
        allow_relative (bool): if True, allows joining segments starting from root
    """
    root = os.path.normpath(root)
    root = os.path.abspath(root)

    candidate = os.path.join(root, *segments)  # join paths

    # get absolute path (e.g. /root/segment1/../../segment2/segment3)
    candidate = os.path.abspath(candidate)

    # resolve any directory movement (e.g. above example becomes: /segment2/segment3)
    candidate = os.path.normpath(candidate)

    if allow_relative:
        if os.path.commonprefix([candidate, root]) != root:
            # Possible path traversal happened, return root.
            return root
        else:
            return candidate

    return os.path.join(root, os.path.basename(candidate))


def delete_file_or_dir(path, ignore_errors=True) -> None:
    """
    Deletes a file or directory at the given path.

    Args:
        path (str): The path to the file or directory to delete.
        ignore_errors (bool, optional): Whether to ignore errors if the file or directory does not exist. Defaults to True.
    """
    if path is None:
        return
    if not ignore_errors:
        if not os.path.exists(path):
            raise FileNotFoundError("file %s does not exist" % (path))

    if os.path.isfile(path):
        # delete file
        os.remove(path)
    elif os.path.isdir(path):
        shutil.rmtree(path, ignore_errors=ignore_errors)


async def delete_file_or_dir_async(path: str, ignore_errors: bool = True) -> None:
    """
    Deletes a file or directory at the given path (async).

    Args:
        path (str): The path to the file or directory to delete.
        ignore_errors (bool, optional): Whether to ignore errors if the file or directory does not exist. Defaults to True.
    """
    if path is None:
        return

    if not ignore_errors:
        if not await aiofiles.ospath.exists(path):
        # if not await p.exists():
            raise FileNotFoundError("file %s does not exist" % (path))

    if await aiofiles.ospath.isfile(path):
        # delete file
        # await p.unlink()
        try:
            await aiofiles.os.remove(path)
        except FileNotFoundError:
            if not ignore_errors:
                raise

    elif await aiofiles.ospath.isdir(path):
        # delete directory tree
        await aioshutil.rmtree(path, ignore_errors=ignore_errors)


def read_file(path: str) -> bytes:
    """
    Read the contents of a file at the given path

    Args:
        path (str): The path to the file.

    Returns:
        bytes: The contents of the file.
    """
    with open(path, "rb") as f:
        b = f.read()
    return b


async def read_file_async(path: str) -> bytes:
    """
    Reads the contents of a file at the given path and returns it as bytes (async).

    Args:
        path (str): The path to the file to read.

    Returns:
        bytes: The contents of the file as bytes.
    """
    async with aiofiles.open(path, "rb") as f:
        b = await f.read()
    return b


async def write_file_async(path: str, content: any, append: bool = False) -> None:
    """
    Write content to a file at the given path (async).

    Args:
        path (str): The path to the file.
        content (any): The content to write to the file.
        append (bool, optional): Whether to append to the file or overwrite it. Defaults to False.

    Returns:
        None
    """
    async with aiofiles.open(path, "ab" if append else "wb") as f:
        await f.write(content)


def write_file(path: str, content: bytes, append: bool = False) -> None:
    """
    Write content to a file at the given path

    Args:
        path (str): The path to the file.
        content (bytes): The content to write to the file.
        append (bool, optional): Whether to append to the file or overwrite it. Defaults to False.

    Returns:
        None
    """
    with open(path, "ab" if append else "wb") as f:
        f.write(content)

async def list_directory_async(
    path: str,
    mask: str = None,
    recursive: bool = False,
    files_only: bool = False,
    case_sensitive: bool = None,
) -> list[str]:
    """
    List all files and directories in a given path that match a specified mask (async).

    Args:
        path (str): The path to list files and directories from.
        mask (str, optional): The mask to filter files and directories by. Defaults to '*'.
        recursive (bool, optional): Whether to list files and directories recursively. Defaults to False.
        files_only (bool, optional): Whether to only list files (or also include directories in the output). Defaults to False.
        case_sensitive (bool, optional): Whether to match the mask in a case-sensitive manner. Defaults to None (case sensitive=True).
    Returns:
        list[str]: A list of paths to files and directories that match the specified mask, possibly empty.
    """
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None, list_directory, path, mask, recursive, files_only, case_sensitive)

def list_directory(
    path: str, mask: str = None, recursive: bool = False, files_only: bool = False, case_sensitive: bool = None
) -> list[str]:
    """
    List all files and directories in a given path that match a specified mask

    Args:
        path (str): The path to list files and directories from.
        mask (str, optional): The mask to filter files and directories by. Defaults to '*'.
        recursive (bool, optional): Whether to list files and directories recursively. Defaults to False.
        files_only (bool, optional): Whether to only list files (or also include directories in the output). Defaults to False.

    Returns:
        list[str]: A list of paths to files and directories that match the specified mask, possibly empty.
    """
    paths = []
    if mask is None:
        mask = "*"
    p = Path(path)
    if recursive:
        f = p.rglob
    else:
        f = p.glob

    for pp in f(mask, case_sensitive=case_sensitive):
        if files_only and pp.is_dir():
            # skip directories
            continue
        paths.append(str(pp))

    return paths


async def copy_file_async(
    source_path: str, destination_path: str, overwrite: bool = True
) -> None:
    """
    copy a file from source_path to destination_path (async), overwriting if exists.

    Args:
        source_path (str): The path of the source file.
        destination_path (str): The path of the destination file.
        overwrite (bool, optional): Whether to overwrite the destination file if it exists. Defaults to True.
    Returns:
        None
    """
    if not overwrite:
        if await exists_async(destination_path):
            raise FileExistsError("file %s already exists" % (destination_path))
    await aioshutil.copy2(source_path, destination_path)


def copy_file(source_path: str, destination_path: str, overwrite: bool = True) -> None:
    """
    copy a file from source_path to destination_path, overwriting if exists.

    Args:
        source_path (str): The path of the source file.
        destination_path (str): The path of the destination file.
        overwrite (bool, optional): Whether to overwrite the destination file if it exists. Defaults to True.
    Returns:
        None
    """
    if not overwrite:
        if exists(destination_path):
            raise FileExistsError("file %s already exists" % (destination_path))
    shutil.copy2(source_path, destination_path)


async def copy_dir_async(
    source_path: str, destination_path: str, dirs_exists_ok: bool = True
) -> None:
    """
    copy a directory from source_path to destination_path (async)

    the destination directory will be created if it does not exist.

    Args:
        source_path (str): The path of the source directory.
        destination_path (str): The path of the destination directory.
        dirs_exists_ok (bool, optional): Whether to ignore errors if the destination directory already exists. Defaults to True.
    Returns:
        None
    """
    await aioshutil.copytree(
        source_path, destination_path, dirs_exist_ok=dirs_exists_ok
    )


async def abspath_async(path: str, resolve: bool = False) -> str:
    """
    Return the absolute path of a file or directory (async).

    Args:
        path (str): The path to return the absolute path of.
        resolve (bool, optional): Whether to resolve symlinks. Defaults to False.
    Returns:
        str: The absolute path of the file or directory.
    """
    loop = asyncio.get_running_loop()

    def _sync_abspath(p: str) -> str:
        # expanduser first (handles ``~``)
        p = os.path.expanduser(p)
        if resolve:
            # realpath will resolve symlinks and normalize the path
            p = os.path.realpath(p)
        return os.path.abspath(p)

    return await loop.run_in_executor(None, _sync_abspath, path)


def abspath(path: str, resolve: bool = False) -> str:
    """
    Return the absolute path of a file or directory

    Args:
        path (str): The path to return the absolute path of.
        resolve (bool, optional): Whether to resolve symlinks. Defaults to False.
    Returns:
        str: The absolute path of the file or directory.
    """
    p = Path(path)
    pp = p.expanduser()
    if resolve:
        pp = pp.resolve()

    pp = pp.absolute()
    return pp.as_posix()


async def exists_async(path: str) -> bool:
    """
    Check if a file or directory exists at the given path (async).

    Args:
        path (str): The path to check.

    Returns:
        bool: True if the file or directory exists, False otherwise.
    """
    return await aiofiles.ospath.exists(path)


def exists(path: str) -> bool:
    """
    Check if a file or directory exists at the given path.

    Args:
        path (str): The path to check.

    Returns:
        bool: True if the file or directory exists, False otherwise.
    """
    return os.path.exists(path)


async def unzip(f: str, dest_dir: str = None) -> str:
    """
    unzip a file to a destination directory (async).

    Args:
        f (str): The path to the file to unzip.
        dest_dir (str, optional): The destination directory where the file will be unzipped. if None, will use a temporary directory.

    Returns:
        str: The path of the destination directory where the file was unzipped. if dest_dir is None, will return the path of the temporary directory and the caller is responsible to delete it.
    """

    def _extract():
        with ZipFile(f) as zf:
            zf.extractall(path=dest_dir)

    created = False
    try:
        # unzip
        if dest_dir is None:
            created = True
            dest_dir = tempfile.mkdtemp()

        dest_dir = await abspath_async(dest_dir)

        try:
            os.makedirs(dest_dir)
        except OSError:
            os.makedirs(dest_dir, exist_ok=True)
            created = True

        MutyLogger.get_instance().debug("unzipping %s to %s ..." % (f, dest_dir))
        await asyncio.get_event_loop().run_in_executor(None, _extract)
        # await unzip_async(f, path=dest_dir)

    except Exception as e:
        if created:
            await muty.file.delete_file_or_dir_async(dest_dir)
        raise e

    return dest_dir


async def get_size(path: str, error_if_not_exists: bool = False) -> int:
    """
    Get the size of a file.

    Args:
        path (str): The path to the file or directory.
        error_if_not_exists (bool, optional): Whether to raise an error if the file or directory does not exist. Defaults to False.
    Returns:
        int: The size of the file or directory in bytes.
    raises:
        FileNotFoundError: If the file or directory does not exist and error_if_not_exists is True.
    """
    try:
        s = await aiofiles.os.stat(path)
    except FileNotFoundError:
        if error_if_not_exists:
            raise
        return 0

    fsize = s.st_size
    return fsize
