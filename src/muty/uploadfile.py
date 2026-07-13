"""utilities to work with UploadFile objects from FastAPI
"""

import os
import tempfile

import aiofiles
import aiofiles.os
import aiofiles.ospath
import aiofiles.tempfile
from fastapi import UploadFile

import muty.file
import muty.string
from muty.log import MutyLogger


async def to_path(
    f: UploadFile,
    dest_dir: str = None,
    use_random_filename: bool = False,
    chunk_size: int = 256 * 1024,
) -> str:
    """
    Downloads an UploadFile object (in chunks) to the specified destination directory.

    Args:
        f (UploadFile): The UploadFile object to be downloaded and saved.
        dest_dir (str): The destination directory where the file will be saved (will be created if it doesn't exists).
            if None, file will be downloaded in a unique temporary directory under $TEMP (the caller is responsible to delete file PARENT directory then).
        use_random_filename (bool, optional): Flag indicating whether to use a random filename. Defaults to False.
        chunk_size (int, optional): The size of the chunks to be downloaded. Defaults to 256*1024 bytes (256k).
    Returns:
        str: The path of the saved file.

    """
    created = False
    if dest_dir is None:
        # use a temporary directory
        dest_dir = os.path.join(tempfile.gettempdir(), muty.string.generate_unique())

    # make sure the destination directory exists
    try:
        await aiofiles.os.makedirs(dest_dir)
    except OSError:
        await aiofiles.os.makedirs(dest_dir, exist_ok=True)
        created = True

    if use_random_filename:
        path = os.path.join(dest_dir, muty.string.generate_unique())
    else:
        path = os.path.join(dest_dir, f.filename)

    MutyLogger.get_instance().debug("downloading UploadFile to %s ..." % (path))
    try:
        async with aiofiles.open(path, "wb") as out_file:
            while content := await f.read(chunk_size):
                await out_file.write(content)
    except Exception as e:
        # delete file on error
        await muty.file.delete_file_or_dir_async(path)
        if created:
            # delete directory on error, if created
            await muty.file.delete_file_or_dir_async(dest_dir)
        raise e

    return path


async def to_path_multi(
    f: list[UploadFile],
    dest_dir: str = None,
    use_random_filename: bool = False,
    chunk_size: int = 256 * 1024,
) -> tuple[str, list[str]]:
    """
    Downloads multiple files to a destination directory.

    Args:
        f (list[UploadFile]): A list of UploadFile objects representing the files to download.
        dest_dir (str, optional): The destination directory where the files will be downloaded.
            if None, files will be downloaded in a unique temporary directory under $TEMP (the caller is responsible to delete the destination directory then).
        use_random_filename (bool, optional): Whether to use a random filename for each uploaded file. Defaults to False.
        chunk_size (int, optional): The chunk size used for uploading the files. Defaults to 256 * 1024.

    Returns:
        tuple[str, list[str]]: A tuple containing the destination directory and a list of paths to the uploaded files.
    """

    l = []
    created = False
    if dest_dir is None:
        created = True
        dest_dir = os.path.join(tempfile.gettempdir(), muty.string.generate_unique())

    try:
        for ff in f:
            p = await to_path(ff, dest_dir, use_random_filename, chunk_size)
            l.append(p)
    except Exception as e:
        if created:
            # delete directory on error, if created
            await muty.file.delete_file_or_dir_async(dest_dir)

        raise e

    return dest_dir, l


async def unzip(
    f: UploadFile, dest_dir: str = None, chunk_size: int = 1024 * 1000
) -> str:
    """
    Downloads an UploadFile object (in chunks) and unzips it to the specified destination directory.

    Args:
        f (UploadFile): The file to be downloaded and unzipped.
        dest_dir (str, optional): The destination directory where the file will be unzipped. if None, will use a temporary directory.
        chunk_size (int, optional): The size of the chunks to be downloaded. Defaults to 1mb.

    Returns:
        str: The path of the destination directory where the file was unzipped. if dest_dir was None, will return the path of the temporary directory and the caller is responsible to delete it.

    Raises:
        Exception: Any exception raised during the process.
    """
    dwnl_path = None
    dd = None
    try:
        # download
        dwnl_path = await to_path(f, use_random_filename=True, chunk_size=chunk_size)

        # unzip
        dd = await muty.file.unzip(dwnl_path, dest_dir=dest_dir)
    except Exception as e:
        if dest_dir is None:
            # delete temp folder
            await muty.file.delete_file_or_dir_async(dd)
        raise e
    finally:
        # delete downloaded file anyway
        await muty.file.delete_file_or_dir_async(dwnl_path)

    return dd
