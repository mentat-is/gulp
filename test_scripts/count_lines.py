#!/usr/bin/env python3

import sys
from pathlib import Path


def count_lines(file_path):
    """
    Count the number of lines in a text file.

    Args:
        file_path (str): Path to the text file

    Returns:
        int: Number of lines in the file

    Raises:
        FileNotFoundError: If the file doesn't exist
        IOError: If there's an error reading the file
    """
    try:
        # Convert string path to Path object
        path = Path(file_path)

        # Check if file exists
        if not path.is_file():
            raise FileNotFoundError(f"File not found: {file_path}")

        # Open and count lines
        with open(path, 'r', encoding='utf-8') as file:
            line_count = sum(1 for line in file)

        return line_count

    except FileNotFoundError as e:
        print(f"Error: {e}")
        return None
    except IOError as e:
        print(f"Error reading file: {e}")
        return None


def main():
    # Check if filename was provided as command line argument
    if len(sys.argv) != 2:
        print("Usage: %s <filename>" % (sys.argv[0]))
        return

    file_path = sys.argv[1]
    result = count_lines(file_path)

    if result is not None:
        print(f"Number of lines in {file_path}: {result}")


if __name__ == "__main__":
    main()
