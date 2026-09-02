"""File utilities sharing implementations only when both call styles are used."""

import asyncio
import codecs
import os
import shutil
import tempfile
from collections.abc import AsyncIterator
from pathlib import Path

import aiofiles
import aiofiles.os
import aiofiles.ospath
import aioshutil
from async_unzip.unzipper import unzip as _unzip

from muty.log import MutyLogger


DEFAULT_TEXT_READ_CHUNK_SIZE = 4 * 1024 * 1024
_TEXT_LINE_ENDINGS = ("\n", "\r")


def _split_text_chunk(
    text: str, line_parts: list[str], trailing_cr: bool
) -> tuple[list[str], bool]:
    """Split decoded text while retaining an incomplete line between chunks."""
    lines: list[str] = []

    if trailing_cr:
        if not text:
            return lines, trailing_cr
        if text[0] == "\n":
            line_parts.append("\n")
            lines.append("".join(line_parts))
            line_parts.clear()
            text = text[1:]
        else:
            lines.append("".join(line_parts))
            line_parts.clear()
        trailing_cr = False

    chunks = text.splitlines(keepends=True)
    for index, chunk in enumerate(chunks):
        is_last = index == len(chunks) - 1
        if is_last and text.endswith("\r"):
            line_parts.append(chunk)
            trailing_cr = True
            continue

        line_parts.append(chunk)
        if not chunk.endswith(_TEXT_LINE_ENDINGS):
            continue
        lines.append("".join(line_parts))
        line_parts.clear()
    return lines, trailing_cr


def _normalize_text_line_ending(line: str, newline: str | None) -> str:
    """Apply TextIO-compatible universal newline translation when requested."""
    if newline == "":
        return line
    if line.endswith("\r\n"):
        return line[:-2] + "\n"
    if line.endswith("\r"):
        return line[:-1] + "\n"
    return line


async def iter_text_file_lines(
    path: str | os.PathLike[str],
    encoding: str = "utf-8",
    errors: str = "strict",
    chunk_size: int = DEFAULT_TEXT_READ_CHUNK_SIZE,
    newline: str | None = "",
) -> AsyncIterator[str]:
    """Yield text lines decoded from fixed-size asynchronous byte reads.

    By default line endings are retained exactly. The final line is yielded
    even when it has no terminator, and incremental decoding keeps multibyte
    characters intact when their bytes span adjacent chunks.

    Args:
        path: Path of the text file to read.
        encoding: Text encoding passed to the incremental decoder.
        errors: Decoder error-handling strategy.
        chunk_size: Number of bytes read per asynchronous file operation.
        newline: ``""`` preserves CRLF/CR terminators; ``None`` translates them
            to LF like the default text-mode ``open()`` behavior.

    Raises:
        ValueError: If ``chunk_size`` is not positive or ``newline`` is not a
            supported text-mode policy.
    """
    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than zero")
    if newline not in (None, ""):
        raise ValueError("newline must be either None or an empty string")

    decoder = codecs.getincrementaldecoder(encoding)(errors=errors)
    line_parts: list[str] = []
    trailing_cr = False

    async with aiofiles.open(path, "rb") as source:
        while chunk := await source.read(chunk_size):
            decoded = decoder.decode(chunk)
            if not decoded:
                continue
            lines, trailing_cr = _split_text_chunk(
                decoded, line_parts, trailing_cr
            )
            for line in lines:
                yield _normalize_text_line_ending(line, newline)

    decoded = decoder.decode(b"", final=True)
    if decoded:
        lines, trailing_cr = _split_text_chunk(decoded, line_parts, trailing_cr)
        for line in lines:
            yield _normalize_text_line_ending(line, newline)

    if trailing_cr or line_parts:
        yield _normalize_text_line_ending("".join(line_parts), newline)


async def write_temporary_file(content: bytes, suffix: str | None = None) -> str:
    """
    Write content to a temporary file (async).

    Args:
        content (bytes): The content to write to the file.
        suffix (str, optional): The suffix of the temporary file. Defaults to None.

    Returns:
        str: The path of the temporary file.
    """
    fd, path = tempfile.mkstemp(suffix=suffix)
    os.close(fd)
    await write_file(path, content)
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


async def delete_file_or_dir(path: str | None, ignore_errors: bool = True) -> None:
    """
    Deletes a file or directory at the given path.

    Args:
        path (str): The path to the file or directory to delete.
        ignore_errors (bool, optional): Whether to ignore errors if the file or directory does not exist. Defaults to True.
    """
    if path is None:
        return
    if not ignore_errors:
        # Keep the explicit check to provide the documented error.
        if not await aiofiles.ospath.exists(path):
            raise FileNotFoundError("file %s does not exist" % (path))

    if await aiofiles.ospath.isfile(path):
        # delete file
        try:
            await aiofiles.os.remove(path)
        except FileNotFoundError:
            if not ignore_errors:
                raise
    elif await aiofiles.ospath.isdir(path):
        # delete directory tree
        await aioshutil.rmtree(path, ignore_errors=ignore_errors)


async def read_file(path: str) -> bytes:
    """
    Read the contents of a file at the given path (async).

    Args:
        path (str): The path to the file.

    Returns:
        bytes: The contents of the file.
    """
    async with aiofiles.open(path, "rb") as f:
        return await f.read()


async def write_file(path: str, content: bytes, append: bool = False) -> None:
    """
    Write content to a file at the given path (async).

    Args:
        path (str): The path to the file.
        content (bytes): The content to write to the file.
        append (bool, optional): Whether to append to the file or overwrite it. Defaults to False.

    Returns:
        None
    """
    async with aiofiles.open(path, "ab" if append else "wb") as f:
        await f.write(content)


async def list_directory_async(
    path: str,
    mask: str | None = None,
    recursive: bool = False,
    files_only: bool = False,
    case_sensitive: bool | None = None,
) -> list[str]:
    """List a directory without blocking the event loop."""
    return await asyncio.to_thread(
        list_directory, path, mask, recursive, files_only, case_sensitive
    )


def list_directory(
    path: str,
    mask: str | None = None,
    recursive: bool = False,
    files_only: bool = False,
    case_sensitive: bool | None = None,
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
        if os.path.exists(destination_path):
            raise FileExistsError("file %s already exists" % (destination_path))
    shutil.copy2(source_path, destination_path)


async def copy_dir(
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


async def abspath(path: str, resolve: bool = False) -> str:
    """
    Return the absolute path of a file or directory (async).

    Args:
        path (str): The path to return the absolute path of.
        resolve (bool, optional): Whether to resolve symlinks. Defaults to False.
    Returns:
        str: The absolute path of the file or directory.
    """
    # expanduser first (handles ``~``)
    p = os.path.expanduser(path)
    if resolve:
        # resolve symlinks and normalize the path
        p = os.path.realpath(p)
    return await aiofiles.ospath.abspath(p)


async def exists(path: str) -> bool:
    """
    Check if a file or directory exists at the given path (async).

    Args:
        path (str): The path to check.

    Returns:
        bool: True if the file or directory exists, False otherwise.
    """
    return await aiofiles.ospath.exists(path)


async def unzip(f: str, dest_dir: str | None = None) -> str:
    """
    unzip a file to a destination directory (async).

    Args:
        f (str): The path to the file to unzip.
        dest_dir (str, optional): The destination directory where the file will be unzipped. if None, will use a temporary directory.

    Returns:
        str: The path of the destination directory where the file was unzipped. if dest_dir is None, will return the path of the temporary directory and the caller is responsible to delete it.
    """

    created = False
    try:
        # unzip
        if dest_dir is None:
            created = True
            dest_dir = tempfile.mkdtemp()

        dest_dir = await abspath(dest_dir)

        try:
            await aiofiles.os.makedirs(dest_dir)
        except OSError:
            await aiofiles.os.makedirs(dest_dir, exist_ok=True)
            created = True

        MutyLogger.get_instance().debug("unzipping %s to %s ..." % (f, dest_dir))
        # extract entries with async I/O
        await _unzip(f, path=dest_dir)

    except Exception as e:
        if created:
            await delete_file_or_dir(dest_dir)
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
