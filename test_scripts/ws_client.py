#!/usr/bin/env python3
# cy security.evtx
# total events count: processed=8096656, failed=0


import argparse
import asyncio
import json
import timeit

import websockets


async def connect_to_websocket(url: str, print_data: bool = False):
    async with websockets.connect(url, max_size=64 * 1024 * 1000) as websocket:
        start_time = timeit.default_timer()

        print("start time:", timeit.default_timer())
        await websocket.send(json.dumps({"token": "1234", "ws_id": "abc"}))
        try:
            count = 0
            events = 0

            while True:
                message = await websocket.recv()
                js: dict = json.loads(message)
                count += 1

                data = js["data"]
                t = js["type"]
                if t in [9, 6]:
                    # ingestion(9) or query(6) chunk, get events count
                    events += len(data["events"])
                    if print_data:
                        print(
                            "---------\n%s chunk: %s\n---------"
                            % (
                                "INGESTION" if t == 9 else "QUERY",
                                json.dumps(data, indent=2),
                            )
                        )
                elif t == 7:
                    # query done
                    print(
                        "QUERY DONE: %s, actual collected events=%d"
                        % (json.dumps(data, indent=2), events)
                    )

                if count % 100 == 0:
                    cur_time = timeit.default_timer()
                    execution_time = cur_time - start_time
                    print(
                        "CURRENT received messages count: %d, events=%d, %f sec. (%d hours, %d minutes)"
                        % (
                            count,
                            events,
                            execution_time,
                            execution_time // 3600,
                            (execution_time % 3600) // 60,
                        )
                    )

                # print(f"Incoming message: {message}")
        except KeyboardInterrupt:
            print("total count: %d" % (count))
        except websockets.exceptions.ConnectionClosedError as e:
            print("***connection closed: %s***" % (e))
        except Exception as e:
            print("***exception: %s***" % (e))
        finally:
            end_time = timeit.default_timer()
            execution_time = end_time - start_time
            print("end time:", end_time)
            print(
                "TOTAL received messages count: %d, events=%d, %f sec. (%d hours, %d minutes)"
                % (
                    count,
                    events,
                    execution_time,
                    execution_time // 3600,
                    (execution_time % 3600) // 60,
                )
            )


# get parameter "--ws" from command line, if None set to ws://localhost:8080/ws
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ws", help='websocket url, websocket name is fixed to "abc"')
    parser.add_argument("--print_data", help="print WS_DATA", action="store_true")
    args = parser.parse_args()
    ws_url = args.ws if args.ws else "ws://localhost:8080/ws"
    asyncio.run(connect_to_websocket(ws_url, args.print_data))
