#!/usr/bin/env python3
import json
import random
import datetime
import uuid
import os
import argparse


def generate_log_record(index, base_time):
    time_offset = datetime.timedelta(
        minutes=index,
        seconds=random.randint(0, 59),
        milliseconds=random.randint(0, 999),
    )
    create_datetime = (base_time + time_offset).isoformat() + "Z"

    return {
        "log_id": f"LOG{random.randint(100000000, 999999999)}",
        "application": random.choice(["FakeApp", "WebService", "DataProcessor"]),
        "version": f"{random.randint(1, 3)}.{random.randint(0, 5)}.{random.randint(0, 9)}",
        "create_datetime": create_datetime,
        "level": random.choice(["INFO", "DEBUG", "WARNING", "ERROR"]),
        "module": random.choice(
            ["user_authentication", "payment_processing", "data_sync"]
        ),
        "message": random.choice(
            ["User login successful", "Session started", "Authentication complete"]
        ),
        "user_id": f"USR{random.randint(100, 999)}",
        "session_id": f"SESS{random.randint(100000, 999999)}",
        "ip_address": f"192.168.{random.randint(0, 255)}.{random.randint(1, 254)}",
        "details": {
            "authentication_method": random.choice(["password", "token", "biometric"]),
            "device_type": random.choice(["desktop", "mobile", "tablet"]),
            "browser": random.choice(["Chrome", "Firefox", "Safari", "Edge"]),
            "browser_version": f"{random.randint(70, 120)}.{random.randint(0, 9)}.{random.randint(1000, 9999)}.{random.randint(1, 99)}",
            "os": random.choice(["Windows 10", "Windows 11", "macOS", "Linux"]),
            "location": {
                "country": random.choice(["US", "UK", "CA", "AU", "DE"]),
                "city": random.choice(
                    ["New York", "London", "Toronto", "Sydney", "Berlin"]
                ),
                "latitude": round(random.uniform(-90, 90), 4),
                "longitude": round(random.uniform(-180, 180), 4),
            },
        },
        "status": random.choice(["success", "failed", "pending"]),
        "duration_ms": random.randint(50, 5000),
        "correlation_id": f"CORR{random.randint(100000000000, 999999999999)}",
    }


def generate_log_file(num_records, output_file):
    base_time = datetime.datetime(2023, 8, 15, 14, 30, 45, 123000)

    with open(output_file, "w") as f:
        f.write("[")
        for i in range(num_records):
            record = generate_log_record(i, base_time)
            json.dump(record, f)
            if i < num_records - 1:
                f.write(",")
            if i % 10000 == 0:
                print(f"Generated {i}/{num_records} records...", end="\r")
        f.write("]")

    print(f"\nSuccessfully generated {num_records} records in {output_file}")
    print(f"File size: {os.path.getsize(output_file) / (1024*1024):.2f} MB")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a JSON list of fake log records")
    parser.add_argument(
        "-n",
        "--num-records",
        type=int,
        default=1000000,
        help="number of records to generate (default: 1000000, 1 million)",
    )
    parser.add_argument(
        "-o",
        "--output",
        dest="output_file",
        default="million_logs.json",
        help='output file path (default: "million_logs.json")',
    )

    args = parser.parse_args()

    generate_log_file(num_records=args.num_records, output_file=args.output_file)
