#!/usr/bin/env python3
import sys


def count_string_in_file(search_string, filename):
    total_occurrences = 0
    line_count = 0

    try:
        with open(filename, "r") as file:
            for line in file:
                line_count += 1
                total_occurrences += line.count(search_string)

        print(f"Found {total_occurrences} occurrences of '{search_string}'")
        print(f"Total lines processed: {line_count}")

    except FileNotFoundError:
        print(f"Error: File '{filename}' not found")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)


def main():
    if len(sys.argv) != 3:
        print("Usage: python string_counter.py <search_string> <filename>")
        sys.exit(1)

    search_string = sys.argv[1]
    filename = sys.argv[2]
    count_string_in_file(search_string, filename)


if __name__ == "__main__":
    main()
