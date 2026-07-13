import asyncio
import os
import shutil
import tempfile
import unittest

from muty.file import (
    copy_file,
    copy_file_async,
    delete_file_or_dir,
    delete_file_or_dir_async,
    list_directory,
    list_directory_async,
    read_file,
    read_file_async,
    write_file,
    write_file_async,
)


class TestFileUtils(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.temp_file = os.path.join(self.temp_dir, "test.txt")
        with open(self.temp_file, "w") as f:
            f.write("test")

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def test_delete_file_or_dir(self):
        self.assertTrue(os.path.exists(self.temp_file))
        delete_file_or_dir(self.temp_file)
        self.assertFalse(os.path.exists(self.temp_file))

    def test_delete_file_or_dir_async(self):
        async def test():
            self.assertTrue(os.path.exists(self.temp_file))
            await delete_file_or_dir_async(self.temp_file)
            self.assertFalse(os.path.exists(self.temp_file))

        asyncio.run(test())

    def test_read_file(self):
        content = read_file(self.temp_file)
        self.assertEqual(content, b"test")

    def test_read_file_async(self):
        async def test():
            content = await read_file_async(self.temp_file)
            self.assertEqual(content, b"test")

        asyncio.run(test())

    def test_write_file(self):
        temp_file = os.path.join(self.temp_dir, "test_write.txt")
        write_file(temp_file, b"test")
        with open(temp_file, "rb") as f:
            content = f.read()
        self.assertEqual(content, b"test")

    def test_write_file_async(self):
        async def test():
            temp_file = os.path.join(self.temp_dir, "test_write_async.txt")
            await write_file_async(temp_file, b"test")
            with open(temp_file, "rb") as f:
                content = f.read()
            self.assertEqual(content, b"test")

        asyncio.run(test())

    def test_list_directory(self):
        paths = list_directory(self.temp_dir)
        self.assertIn(self.temp_file, paths)

    def test_list_directory_async(self):
        async def test():
            paths = await list_directory_async(self.temp_dir)
            self.assertIn(self.temp_file, paths)

        asyncio.run(test())

    def test_copy_file(self):
        temp_file = os.path.join(self.temp_dir, "test_copy.txt")
        copy_file(self.temp_file, temp_file)
        with open(temp_file, "rb") as f:
            content = f.read()
        self.assertEqual(content, b"test")

    def test_copy_file_async(self):
        async def test():
            temp_file = os.path.join(self.temp_dir, "test_copy_async.txt")
            await copy_file_async(self.temp_file, temp_file)
            with open(temp_file, "rb") as f:
                content = f.read()
            self.assertEqual(content, b"test")

        asyncio.run(test())
