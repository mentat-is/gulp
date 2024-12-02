#!/usr/bin/env python3
"""
Script to test gULP collaboration object API endpoints, focusing on notes
"""

import argparse
import json
import logging
import requests
from typing import Optional
from muty.log import MutyLogger
from gulp.api.collab.structs import GulpCollabFilter
from gulp.api.collab_api import TEST_CONTEXT_ID, TEST_OPERATION_ID, TEST_SOURCE_ID
from gulp.api.opensearch.structs import GulpBasicDocument


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Test gULP collaboration object endpoints."
    )
    parser.add_argument(
        "--host", default="localhost:8080", help="Gulp host", metavar="HOST:PORT"
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Reset gulp before running tests",
    )
    parser.add_argument("--username", default="admin", help="Username for login")
    parser.add_argument("--password", default="admin", help="Password for login")
    parser.add_argument("--ws_id", default="test_ws", help="websocket ID")
    parser.add_argument("--req_id", default="test_req", help="request ID")
    parser.add_argument(
        "--index",
        default="test_idx",
        help="index to reset (ignored if --reset is not set)",
    )
    return parser.parse_args()


def _make_url(host: str, endpoint: str) -> str:
    return f"http://{host}/{endpoint}"


class GulpCollabTester:
    def __init__(self, host: str):
        self.host = host
        self.logger = MutyLogger.get_instance("test_collab")

    def _log_request(self, method: str, url: str, params: dict):
        self.logger.debug(f"REQUEST {method} {url}")
        self.logger.debug(f"PARAMS: {json.dumps(params, indent=2)}")

    def _log_response(self, r: requests.Response):
        self.logger.debug(f"RESPONSE Status: {r.status_code}")
        self.logger.debug(f"RESPONSE Body: {json.dumps(r.json(), indent=2)}")

    async def _make_request(
        self,
        method: str,
        endpoint: str,
        params: dict,
        token: str = None,
        body: dict = None,
        expected_status: int = 200,
    ):
        """Make HTTP request and verify status"""
        url = _make_url(self.host, endpoint)
        headers = {"token": token} if token else None

        self._log_request(
            method, url, {"params": params, "body": body, "headers": headers}
        )

        if method in ["POST", "PATCH", "PUT"] and body:
            r = requests.request(method, url, headers=headers, params=params, json=body)
        else:
            r = requests.request(method, url, headers=headers, params=params)

        self._log_response(r)

        if r.status_code != expected_status:
            self.logger.error(f"Expected status {expected_status}, got {r.status_code}")
            raise Exception("Request failed!")

        return r.json().get("data") if r.status_code == 200 else None

    async def login(
        self, username: str, password: str, ws_id: str, req_id: str
    ) -> Optional[str]:
        """Login and return token"""
        self.logger.info(f"Logging in as {username}...")
        params = {
            "user_id": username,
            "password": password,
            "ws_id": ws_id,
            "req_id": req_id,
        }
        result = await self._make_request("PUT", "login", params=params)
        return result.get("token") if result else None

    async def create_note(
        self,
        token: str,
        ws_id: str,
        req_id: str,
        operation_id: str,
        context_id: str,
        source_id: str,
        text: str,
        time_pin: int = None,
        docs: list = None,
        name: str = None,
        tags: list[str] = None,
        color: str = None,
        private: bool = False,
        expected_status: int = 200,
    ) -> Optional[dict]:
        """Create a new note"""
        self.logger.info("Creating note...")
        params = {
            "operation_id": operation_id,
            "context_id": context_id,
            "source_id": source_id,
            "time_pin": time_pin,
            "name": name,
            "color": color,
            "private": private,
            "ws_id": ws_id,
            "req_id": req_id,
        }

        body = {
            "docs": docs,
            "text": text,
            "tags": tags,
        }

        return await self._make_request(
            "POST",
            "note_create",
            params=params,
            body=body,
            token=token,
            expected_status=expected_status,
        )

    async def update_note(
        self,
        token: str,
        ws_id: str,
        req_id: str,
        note_id: str,
        text: str = None,
        time_pin: int = None,
        docs: list = None,
        name: str = None,
        tags: list[str] = None,
        color: str = None,
        private: bool = None,
        expected_status: int = 200,
    ) -> Optional[dict]:
        """Update an existing note"""
        self.logger.info(f"Updating note {note_id}...")
        params = {
            "object_id": note_id,
            "time_pin": time_pin,
            "color": color,
            "private": private,
            "name": name,
            "ws_id": ws_id,
            "req_id": req_id,
        }

        body = {
            "docs": docs,
            "tags": tags,
            "text": text,
        }

        return await self._make_request(
            "PATCH",
            "note_update",
            params=params,
            body=body,
            token=token,
            expected_status=expected_status,
        )

    async def delete_note(
        self,
        token: str,
        ws_id: str,
        req_id: str,
        note_id: str,
        expected_status: int = 200,
    ) -> bool:
        """Delete a note"""
        self.logger.info(f"Deleting note {note_id}...")
        params = {"object_id": note_id, "ws_id": ws_id, "req_id": req_id}
        return (
            await self._make_request(
                "DELETE",
                "note_delete",
                params=params,
                token=token,
                expected_status=expected_status,
            )
            is not None
        )

    async def get_note(
        self, token: str, req_id: str, note_id: str, expected_status: int = 200
    ) -> Optional[dict]:
        """Get note details"""
        self.logger.info(f"Getting note {note_id}...")
        params = {"object_id": note_id, "req_id": req_id}
        return await self._make_request(
            "GET",
            "note_get_by_id",
            params=params,
            token=token,
            expected_status=expected_status,
        )

    async def list_notes(
        self,
        token: str,
        req_id: str,
        flt: GulpCollabFilter = None,
        expected_status: int = 200,
    ) -> Optional[list]:
        """List notes with optional filters"""
        self.logger.info("Listing notes...")
        return await self._make_request(
            "POST",
            "note_list",
            params={"req_id": req_id},
            body=flt.model_dump(
                by_alias=True, exclude_none=True, exclude_defaults=True
            ),
            token=token,
            expected_status=expected_status,
        )

    async def reset_gulp(self, args) -> bool:
        self.logger.info("Resetting gULP...")
        """Reset gULP"""

        # logging in as admin
        params = {
            "user_id": "admin",
            "password": "admin",
            "ws_id": args.ws_id,
            "req_id": args.req_id,
        }
        result = await self._make_request("PUT", "login", params=params)
        token = result.get("token")

        # reset
        await self._make_request(
            "POST", "gulp_reset", params={"index": args.index}, token=token
        )

    async def run_note_tests(self, args):
        """Run test sequence for notes"""
        try:
            user = args.username
            password = args.password
            ws_id = args.ws_id
            req_id = args.req_id

            self.logger.info(
                "Starting note API tests, user: %s, ws_id: %s, req_id: %s"
                % (user, ws_id, req_id)
            )

            # Login as admin
            the_token = await self.login(user, password, ws_id, req_id)
            assert the_token, "%s login failed" % (user)

            # test note creation
            note_data = await self.create_note(
                the_token,
                ws_id,
                req_id,
                operation_id=TEST_OPERATION_ID,
                context_id=TEST_CONTEXT_ID,
                source_id=TEST_SOURCE_ID,
                text="Test note",
                time_pin=1000000,
                name="Test Note",
                tags=["test"],
                color="blue",
            )
            assert note_data, "Note creation failed"
            note_1_id = note_data["id"]

            # Test get note
            note = await self.get_note(the_token, req_id, note_1_id)
            assert note, "Getting note failed"
            assert note["text"] == "Test note", "Note text mismatch"

            # Test update note
            updated = await self.update_note(
                the_token,
                ws_id,
                req_id,
                note_1_id,
                text="Updated note",
                tags=["test", "updated"],
            )
            assert updated["text"] == "Updated note", "Note update failed"

            updated = await self.update_note(
                the_token,
                ws_id,
                req_id,
                note_1_id,
                text="Updated note again",
                tags=["test", "updated", "again"],
            )
            assert updated["text"] == "Updated note again", "Note update failed"

            # Verify update
            updated_note = await self.get_note(the_token, req_id, note_1_id)
            assert (
                updated_note["text"] == "Updated note again"
            ), "Note update verification failed"
            assert len(updated_note["previous"]) == 2, "Edit history not updated"

            # Test list notes
            flt = GulpCollabFilter(
                operation_ids=[TEST_OPERATION_ID],
                context_ids=[TEST_CONTEXT_ID],
                source_ids=[TEST_SOURCE_ID],
            )
            notes = await self.list_notes(
                the_token,
                req_id,
                flt=flt,
            )
            assert notes and len(notes) >= 1, "Note listing failed"

            # update note again but setting docs
            # create an array of GulpBasicDocuments
            docs = [
                GulpBasicDocument(
                    id="test_doc",
                    timestamp="2019-01-01T00:00:00Z",
                    gulp_timestamp=1000000,
                    operation_id=TEST_OPERATION_ID,
                    context_id=TEST_CONTEXT_ID,
                    source_id=TEST_SOURCE_ID,
                ),
                GulpBasicDocument(
                    id="test_doc2",
                    timestamp="2019-01-01T00:00:01Z",
                    gulp_timestamp=1000001,
                    operation_id=TEST_OPERATION_ID,
                    context_id=TEST_CONTEXT_ID,
                    source_id=TEST_SOURCE_ID,
                ),
                GulpBasicDocument(
                    id="test_doc3",
                    timestamp="2019-01-01T00:00:03Z",
                    gulp_timestamp=1000002,
                    operation_id=TEST_OPERATION_ID,
                    context_id=TEST_CONTEXT_ID,
                    source_id=TEST_SOURCE_ID,
                ),
            ]
            updated = await self.update_note(
                the_token,
                ws_id,
                req_id,
                note_1_id,
                text="Updated note with docs",
                tags=["test", "updated", "again", "with_docs"],
                docs=[doc.model_dump(by_alias=True, exclude_none=True) for doc in docs],
            )
            assert len(updated["docs"]) == 3, "Note update (with docs) failed"

            # create another note with different docs
            docs = [
                GulpBasicDocument(
                    id="test_doc4",
                    timestamp="2019-01-01T01:00:00Z",
                    gulp_timestamp=1000008,
                    operation_id=TEST_OPERATION_ID,
                    context_id=TEST_CONTEXT_ID,
                    source_id=TEST_SOURCE_ID,
                ),
                GulpBasicDocument(
                    id="test_doc5",
                    timestamp="2019-01-01T02:00:01Z",
                    gulp_timestamp=1000009,
                    operation_id=TEST_OPERATION_ID,
                    context_id=TEST_CONTEXT_ID,
                    source_id=TEST_SOURCE_ID,
                ),
            ]
            note_data = await self.create_note(
                the_token,
                ws_id,
                req_id,
                operation_id=TEST_OPERATION_ID,
                context_id=TEST_CONTEXT_ID,
                source_id=TEST_SOURCE_ID,
                text="Test note",
                docs=[doc.model_dump(by_alias=True, exclude_none=True) for doc in docs],
                name="Test Note",
                tags=["test"],
                color="blue",
            )
            assert note_data, "Note 2 creation failed"
            note_2_id = note_data["id"]

            # test list note with filter on doc id (should match note 1)
            flt = GulpCollabFilter(
                doc_ids=["test_doc2"], operation_ids=[TEST_OPERATION_ID]
            )
            notes = await self.list_notes(
                the_token,
                req_id,
                flt=flt,
            )
            assert (
                notes and notes[0]["id"] == note_1_id
            ), "Note listing failed (doc filter 1)"

            # this should match note 2
            flt = GulpCollabFilter(
                doc_ids=["test_doc5"], operation_ids=[TEST_OPERATION_ID]
            )
            notes = await self.list_notes(
                the_token,
                req_id,
                flt=flt,
            )
            assert (
                notes and notes[0]["id"] == note_2_id
            ), "Note listing failed (doc filter 2)"

            # test list note with filter on note time (match both)
            flt = GulpCollabFilter(
                doc_time_range=(1000001, 1000101), operation_ids=[TEST_OPERATION_ID]
            )
            notes = await self.list_notes(
                the_token,
                req_id,
                flt=flt,
            )
            assert notes and len(notes) == 2, "Note listing failed (time filter)"

            # Test delete note
            assert await self.delete_note(
                the_token, ws_id, req_id, note_1_id
            ), "Note deletion failed"

            assert await self.delete_note(
                the_token, ws_id, req_id, note_2_id
            ), "Note deletion failed"

            # Verify deletion
            await self.get_note(the_token, req_id, note_1_id, expected_status=404)
            await self.get_note(the_token, req_id, note_2_id, expected_status=404)
            self.logger.info("Note tests completed successfully!")

        except Exception as ex:
            self.logger.error(f"Note test failed: {str(ex)}")
            raise


def main():
    MutyLogger.get_instance("test_collab", level=logging.DEBUG)
    args = _parse_args()
    tester = GulpCollabTester(args.host)
    import asyncio

    if args.reset:
        asyncio.run(tester.reset_gulp(args))

    asyncio.run(tester.run_note_tests(args))


if __name__ == "__main__":
    main()
