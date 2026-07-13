"""crypto utilities""" ""

import xxhash
import aiofiles
from Crypto.Hash import MD5, SHA1, SHA256, BLAKE2b


def hash_xxh64_int(buffer: str | bytes, enforce_positive: bool = True) -> int:
    """
    Hashes the input buffer using the xxhash algorithm and returns the resulting digest as a unique integer.

    Args:
        buffer (str | bytes): The buffer to be hashed.
        enforce_positive (bool, optional): Whether to enforce the hash to be positive. Defaults to True.

    Returns:
        int: The hash value as a unique integer.
    """
    h = xxhash.xxh64(buffer).intdigest()
    if enforce_positive:
        return h & 0x7FFFFFFFFFFFFFFF
    return h


def _hash_internal(
    buffer: str | bytes, digest, return_bytes: bool = False
) -> str | bytes:
    if isinstance(buffer, str):
        bb = buffer.encode()
    else:
        bb = buffer
    digest.update(bb)
    if return_bytes:
        return digest.digest()
    return digest.hexdigest()


async def _hash_file_internal(path: str, chunk_size: int, digest, return_bytes: bool):
    async with aiofiles.open(path, "rb") as f:
        while True:
            buf = await f.read(chunk_size)
            digest.update(buf)
            if len(buf) < chunk_size:
                break

    if return_bytes:
        return digest.digest()
    return digest.hexdigest()


def hash_xxh64(buffer: str | bytes, return_bytes: bool = False) -> str | bytes:
    """
    Hashes the input buffer using the xxhash algorithm and returns the resulting digest as a hex string.

    Args:
        buffer (str | bytes): The input buffer to hash.
        return_bytes (bool, optional): Whether to return the digest as bytes. Defaults to False.

    Returns:
        str | bytes: The resulting digest as a hex string or bytes.
    """
    digest = xxhash.xxh64()
    return _hash_internal(buffer, digest, return_bytes)


def hash_xxh128(buffer: str | bytes, return_bytes: bool = False) -> str | bytes:
    """
    Hashes the input buffer using the xxhash algorithm and returns the resulting digest as a hex string.

    Args:
        buffer (str | bytes): The input buffer to hash.
        return_bytes (bool, optional): Whether to return the digest as bytes. Defaults to False.

    Returns:
        str | bytes: The resulting digest as a hex string or bytes.
    """
    digest = xxhash.xxh128()
    return _hash_internal(buffer, digest, return_bytes)


def hash_xxh64_file(
    path: str, chunk_size: int = 1024 * 1000, return_bytes: bool = False
) -> str | bytes:
    """
    Calculate the xxhash of a file.

    Args:
        path (str): The path to the file.
        chunk_size (int, optional): The size of each chunk to read from the file. Defaults to 1024*1000.
        return_bytes (bool, optional): Whether to return the digest as bytes. Defaults to False.

    Returns:
        str | bytes: The resulting digest as a hex string or bytes.
    """
    digest = xxhash.xxh64()
    return _hash_file_internal(path, chunk_size, digest, return_bytes)


def hash_md5(buffer: str | bytes, return_bytes: bool = False) -> str | bytes:
    """
    Hashes the input buffer using the MD5 algorithm and returns the resulting digest as a hex string.

    Args:
        buffer (str or bytes): The input buffer to hash.
        return_bytes (bool, optional): Whether to return the digest as bytes. Defaults to False.

    Returns:
        The resulting digest as a hex string or bytes.
    """
    digest = MD5.new()
    return _hash_internal(buffer, digest, return_bytes)


def hash_sha1(buffer: str | bytes, return_bytes: bool = False) -> str | bytes:
    """
    Hashes the input buffer using the SHA1 algorithm and returns the resulting digest as a hex string.

    Args:
        buffer (str or bytes): The input buffer to hash.
        return_bytes (bool, optional): Whether to return the digest as bytes. Defaults to False.

    Returns:
        The resulting digest as a hex string or bytes.
    """
    digest = SHA1.new()
    return _hash_internal(buffer, digest, return_bytes)


def hash_blake2b(buffer: str | bytes, return_bytes: bool = False) -> str | bytes:
    """
    Hashes the input buffer using the BLAKE2b algorithm and returns the resulting digest as a hex string.

    Args:
        buffer (str or bytes): The input buffer to hash.
        return_bytes (bool, optional): Whether to return the digest as bytes. Defaults to False.

    Returns:
        The resulting digest as a hex string or bytes.
    """
    digest = BLAKE2b.new()
    return _hash_internal(buffer, digest, return_bytes)


def hash_sha256(buffer: str | bytes, return_bytes: bool = False) -> str | bytes:
    """
    Hashes the input buffer using the SHA256 algorithm and returns the resulting digest as a hex string.

    Args:
        buffer (str or bytes): The input buffer to hash.
        return_bytes (bool, optional): Whether to return the digest as bytes. Defaults to False.
    Returns:
        str: The resulting digest as a hex string or bytes.
    """
    digest = SHA256.new()
    return _hash_internal(buffer, digest, return_bytes)


async def hash_sha256_file(
    path: str, chunk_size: int = 1024 * 1000, return_bytes: bool = False
) -> str | bytes:
    """
    Calculate the SHA256 hash of a file.

    Args:
        path (str): The path to the file.
        chunk_size (int, optional): The size of each chunk to read from the file. Defaults to 1024*1000.
        return_bytes (bool, optional): Whether to return the digest as bytes. Defaults to False.

    Returns:
        str: The resulting digest as a hex string or bytes.
    """
    digest = SHA256.new()
    return await _hash_file_internal(path, chunk_size, digest, return_bytes)


async def hash_sha1_file(
    path: str, chunk_size: int = 1024 * 1000, return_bytes: bool = False
) -> str | bytes:
    """
    Calculate the SHA1 hash of a file.

    Args:
        path (str): The path to the file.
        chunk_size (int, optional): The size of each chunk to read from the file. Defaults to 1024*1000.
        return_bytes (bool, optional): Whether to return the digest as bytes. Defaults to False.

    Returns:
        str: The resulting digest as a hex string or bytes.
    """
    digest = SHA1.new()
    return await _hash_file_internal(path, chunk_size, digest, return_bytes)


async def hash_blake2b_file(
    path: str, chunk_size: int = 1024 * 1000, return_bytes: bool = False
) -> str | bytes:
    """
    Calculate the BLAKE2b hash of a file.

    Args:
        path (str): The path to the file.
        chunk_size (int, optional): The size of each chunk to read from the file. Defaults to 1024*1000.

    Returns:
        str: The resulting digest as a hex string or bytes.
    """
    digest = BLAKE2b.new()
    return await _hash_file_internal(path, chunk_size, digest, return_bytes)


def hash_crc24(value: str | bytes, encoder: str = "utf-8"):
    """
    Calculates the CRC-24 hash of the given value.
    Args:
        value (str | bytes): The value to calculate the hash for. If it's a string, it will be encoded using the specified encoder.
        encoder (str, optional): The encoding to use if the value is a string. Defaults to "utf-8".
    Returns:
        int: The CRC-24 hash of the value.
    """
    if isinstance(value, str):
        value = value.encode(encoder)

    INIT = 0xB704CE
    POLY = 0x1864CFB
    crc = INIT
    for octet in value:
        crc ^= octet << 16
        for i in range(0, 8):
            crc <<= 1
            if crc & 0x1000000:
                crc ^= POLY
    return crc & 0xFFFFFF
