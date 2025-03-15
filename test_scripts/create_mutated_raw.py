#!/usr/bin/env python3
# filepath: json_anomaly_generator.py

import argparse
import copy
import datetime
import json
import random
import string
import sys
import uuid
from typing import Any, Dict, List, Optional, Tuple, Union

import muty.string


def parse_arguments() -> argparse.Namespace:
    """
    parse command line arguments for the anomaly generator

    Args:
        None

    Returns:
        argparse.Namespace: parsed command line arguments
    """
    parser = argparse.ArgumentParser(
        description="generate json elements with mutations")
    parser.add_argument("--elements", type=int, default=2000,
                        help="number of elements to generate (default: 2000)")
    parser.add_argument("--output", type=str, default="./test_anomalies.json",
                        help="output file path (default: ./test_anomalies.json)")
    parser.add_argument("--field", type=str, default="destination.port",
                        help="field to mutate (default: destination.port)")
    parser.add_argument("--num_mutations", type=int, default=3,
                        help="number of elements to mutate (default: 3)")
    parser.add_argument("--start_timestamp", type=str,
                        default="2022-06-07T15:30:20+00:00",
                        help="starting timestamp in ISO-8601 format (default: 2022-06-07T15:30:20+00:00)")

    return parser.parse_args()


def generate_random_id(length: int = 24) -> str:
    """
    generate a random hexadecimal id

    Args:
        length: length of the id to generate

    Returns:
        str: random hexadecimal id
    """
    return ''.join(random.choice('0123456789abcdef') for _ in range(length))


def get_nested_field(data: Dict[str, Any], field_path: str) -> Tuple[Any, bool]:
    """
    get the value of a nested field in a dictionary using dot notation

    Args:
        data: dictionary to search in
        field_path: path to the field using dot notation

    Returns:
        Tuple[Any, bool]: the value of the field and a boolean indicating if field exists
    """
    # handle non-nested fields directly
    if field_path in data:
        return data[field_path], True

    # handle nested fields with dot notation
    keys = field_path.split('.')
    current = data

    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return None, False
        current = current[key]

    return current, True


def set_nested_field(data: Dict[str, Any], field_path: str, value: Any) -> None:
    """
    set the value of a nested field in a dictionary using dot notation

    Args:
        data: dictionary to modify
        field_path: path to the field using dot notation
        value: new value to set
    """
    # handle non-nested fields directly
    if field_path in data:
        data[field_path] = value
        return

    # handle nested fields with dot notation
    keys = field_path.split('.')
    current = data

    # navigate to the last parent
    for key in keys[:-1]:
        if key not in current:
            current[key] = {}
        current = current[key]

    # set the value
    current[keys[-1]] = value


def generate_mutation(original_value: Any) -> Any:
    """
    generate a mutation based on the type of the original value

    Args:
        original_value: value to mutate

    Returns:
        Any: mutated value of the same type as the original
    """
    # determine the type and generate an appropriate mutation
    if isinstance(original_value, int):
        # for integers, generate a different random integer
        # avoid the original value
        new_value = original_value
        while new_value == original_value:
            # common port range for network ports
            new_value = random.randint(1, 65535)
        return new_value

    elif isinstance(original_value, float):
        # for floats, generate a different random float
        return original_value * random.uniform(0.5, 1.5)

    elif isinstance(original_value, str):
        # for strings, modify by adding some random characters
        random_suffix = ''.join(random.choice(
            string.ascii_letters) for _ in range(5))
        return f"{original_value}_{random_suffix}"

    elif isinstance(original_value, bool):
        # for booleans, flip the value
        return not original_value

    else:
        # for other types, return the original value
        return original_value


def iso_to_nanoseconds(iso_timestamp: str) -> int:
    """
    convert iso-8601 timestamp to nanoseconds from unix epoch

    Args:
        iso_timestamp: timestamp in iso-8601 format

    Returns:
        int: timestamp in nanoseconds
    """
    # parse the iso timestamp
    dt = datetime.datetime.fromisoformat(iso_timestamp.replace('Z', '+00:00'))

    # convert to nanoseconds (seconds * 10^9)
    return int(dt.timestamp() * 1_000_000_000)


def generate_timestamps(start_timestamp: str, interval_seconds: int,
                        index: int) -> Tuple[str, int]:
    """
    generate consecutive timestamps spaced by an interval

    Args:
        start_timestamp: starting timestamp in iso-8601 format
        interval_seconds: seconds between consecutive timestamps
        index: index of the current element

    Returns:
        Tuple[str, int]: tuple containing the iso timestamp and nanosecond timestamp
    """
    # parse the starting timestamp
    dt = datetime.datetime.fromisoformat(
        start_timestamp.replace('Z', '+00:00'))

    # add the appropriate interval based on index
    new_dt = dt + datetime.timedelta(seconds=interval_seconds * index)

    # format as iso-8601
    iso_timestamp = new_dt.isoformat()

    # convert to nanoseconds
    nano_timestamp = iso_to_nanoseconds(iso_timestamp)

    return iso_timestamp, nano_timestamp


def main() -> None:
    """
    main function to generate json elements with targeted mutations

    Args:
        None

    Returns:
        None
    """
    # parse command line arguments
    args = parse_arguments()

    # base json template
    base_template = {
        "_id": "60e7b1b4b3f1b1b1b1b1b1b1",
        "@timestamp": "2022-06-07T15:30:20+00:00",
        "gulp.timestamp": 1654615820000000000,
        "gulp.operation_id": "test_operation",
        "gulp.context_id": "aa12e662778fe5cdad22371f9cd8d2a55484c513",
        "gulp.source_id": "949e4f4e0463d5b51ec9dac32b9aea806400e809",
        "log.file.path": "C:\\Users\\co29617\\Desktop\\Incidente 183651\\T4VDI1001R-017 winevt\\Security.evtx",
        "agent.type": "query_splunk",
        "event.original": "06/07/2022 05:30:20 PM\nLogName=Security\nEventCode=5156\nEventType=0\nComputerName=T4VDI1001R-017.ad04.eni.intranet\nSourceName=Microsoft Windows security auditing.\nType=Informazioni\nRecordNumber=1224403878\nKeywords=Controllo riuscito\nTaskCategory=Filtering Platform Connection\nOpCode=Informazioni\nMessage=Connessione consentita da Piattaforma filtro Windows.\r\n\r\nInformazioni sull'applicazione:\r\n\tID processo:\t\t18268\r\n\tNome applicazione:\t\\device\\harddiskvolume2\\program files\\intergraph smart licensing\\client\\islclient.exe\r\n\r\nInformazioni di rete:\r\n\tDirezione:\t\tIn uscita\r\n\tIndirizzo di origine:\t\t::1\r\n\tPorta di origine:\t\t61904\r\n\tIndirizzo di destinazione:\t::1\r\n\tPorta di destinazione:\t\t8088\r\n\tProtocollo:\t\t6\r\n\r\nInformazioni sul filtro:\r\n\tID run-time filtro:\t65692\r\n\tNome livello:\t\tConnetti\r\n\tID run-time livello:\t50\n",
        "event.sequence": 56403,
        "event.code": "WinEventLog:Security",
        "gulp.event_code": 6560703015527673681,
        "event.duration": 1,
        "winlog.computer_name": "T4VDI1001R-017.ad04.eni.intranet",
        "destination.port": 8088,
        "source.port": 61904,
        "gulp.unmapped.Protocollo": "6",
        "gulp.unmapped.RecordNumber": "1224403878",
        "gulp.unmapped.SourceName": "Microsoft Windows security auditing.",
        "gulp.unmapped.TaskCategory": "Filtering Platform Connection",
        "gulp.unmapped.Type": "Informazioni",
        "host.name": "test_host",
        "gulp.unmapped.index": "incidente_183651"
    }

    # check if the field exists in the template
    original_value, field_exists = get_nested_field(base_template, args.field)
    if not field_exists:
        print(f"error: field '{args.field}' not found in the template")
        sys.exit(1)

    # validate number of mutations
    if args.num_mutations > args.elements:
        print(
            f"error: number of mutations ({args.num_mutations}) cannot be greater than total elements ({args.elements})")
        sys.exit(1)

    # generate elements
    elements = []
    mutation_indices = random.sample(range(args.elements), args.num_mutations)
    mutated_elements = []

    for i in range(args.elements):
        # create a deep copy of the template
        element = copy.deepcopy(base_template)

        # set a random _id for each element
        element["_id"] = generate_random_id()

        # set consecutive timestamps, time interval is a random between 1 and 10 seconds
        time_interval = random.randint(1, 10)
        iso_timestamp, nano_timestamp = generate_timestamps(
            args.start_timestamp,
            time_interval,
            i
        )
        element["@timestamp"] = iso_timestamp
        element["gulp.timestamp"] = nano_timestamp

        # generate random event.original and update associated fields
        event_original = muty.string.generate_unique()
        element["event.original"] = event_original
        element["event.sequence"] = i

        # apply mutation if this element is selected for mutation
        if i in mutation_indices:
            # get the original value
            original_value, _ = get_nested_field(element, args.field)

            # generate a mutated value
            mutated_value = generate_mutation(original_value)

            # set the mutated value
            set_nested_field(element, args.field, mutated_value)

            # save the mutated element for later output
            mutated_element_info = {
                "index": i,
                "id": element["_id"],
                "timestamp": element["@timestamp"],
                "original_value": original_value,
                "mutated_value": mutated_value
            }
            mutated_elements.append(mutated_element_info)

        elements.append(element)

    # write to output file
    try:
        with open(args.output, 'w') as f:
            json.dump(elements, f, indent=2)
        print(f"successfully wrote {args.elements} elements to {args.output}")
    except Exception as e:
        print(f"error writing to file: {e}")
        sys.exit(1)

    # output summary information
    print(f"total elements: {args.elements}")
    print(f"mutated elements: {args.num_mutations}")
    print(f"start timestamp: {args.start_timestamp}")
    print(f"end timestamp: {elements[-1]['@timestamp']}")
    print("mutated elements details:")
    for elem in mutated_elements:
        print(
            f"  index: {elem['index']}, id: {elem['id']}, timestamp: {elem['timestamp']}, field: {args.field}, original: {elem['original_value']}, mutated: {elem['mutated_value']}")


if __name__ == "__main__":
    main()
