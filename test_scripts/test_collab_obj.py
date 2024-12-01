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


def _parse_args():
    parser = argparse.ArgumentParser(description="Test gULP collaboration object endpoints.")
    parser.add_argument(
        "--host", default="localhost:8080", help="Gulp host", metavar="HOST:PORT"
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Reset gulp before running tests",
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
        params: dict = None,
        body: dict = None,
        token: Optional[str] = None,
        expected_status: int = 200,
    ):
        """Make HTTP request and verify status"""
        url = _make_url(self.host, endpoint)
        headers = {"token": token} if token else {}
        params = params or {}
        params["req_id"] = "test_req"

        self._log_request(method, url, {"params": params, "body": body, "headers": headers})

        if method in ["POST", "PATCH", "PUT"] and body:
            r = requests.request(method, url, headers=headers, params=params, json=body)
        else:
            r = requests.request(method, url, headers=headers, params=params)

        self._log_response(r)

        if r.status_code != expected_status:
            self.logger.error(f"Expected status {expected_status}, got {r.status_code}")
            raise Exception("Request failed!")

        return r.json().get("data") if r.status_code == 200 else None

    async def login(self, username: str, password: str) -> Optional[str]:
        """Login and return token"""
        self.logger.info(f"Logging in as {username}...")
        params = {"user_id": username, "password": password, "ws_id": "test_ws"}
        result = await self._make_request("PUT", "login", params=params)
        return result.get("token") if result else None

    async def create_note(
        self,
        token: str,
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
            "ws_id": "test_ws",
        }
        
        body = {
            "text": text,
            "time_pin": time_pin,
            "docs": docs,
            "name": name,
            "tags": tags,
            "color": color,
            "private": private
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
            "note_id": note_id,
            "ws_id": "test_ws",
        }
        
        body = {
            "text": text,
            "time_pin": time_pin,
            "docs": docs,
            "name": name,
            "tags": tags,
            "color": color,
            "private": private
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
        self, token: str, note_id: str, expected_status: int = 200
    ) -> bool:
        """Delete a note"""
        self.logger.info(f"Deleting note {note_id}...")
        params = {
            "note_id": note_id,
            "ws_id": "test_ws"
        }
        return await self._make_request(
            "DELETE",
            "note_delete",
            params=params,
            token=token,
            expected_status=expected_status,
        ) is not None

    async def get_note(
        self, token: str, note_id: str, expected_status: int = 200
    ) -> Optional[dict]:
        """Get note details"""
        self.logger.info(f"Getting note {note_id}...")
        params = {"note_id": note_id}
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
        operation_id: str = None,
        context_id: str = None,
        source_id: str = None,
        expected_status: int = 200,
    ) -> Optional[list]:
        """List notes with optional filters"""
        self.logger.info("Listing notes...")
        params = {}
        if operation_id:
            params["operation_id"] = operation_id
        if context_id:
            params["context_id"] = context_id
        if source_id:
            params["source_id"] = source_id
            
        return await self._make_request(
            "POST",
            "note_list",
            params=params,
            token=token,
            expected_status=expected_status,
        )

    async def run_note_tests(self):
        """Run test sequence for notes"""
        try:
            self.logger.info("Starting note API tests...")

            # Login as admin
            admin_token = await self.login("admin", "admin")
            assert admin_token, "Admin login failed"

            # Test note creation
            note_data = await self.create_note(
                admin_token,
                operation_id="test_op",
                context_id="test_ctx",
                source_id="test_source",
                text="Test note",
                time_pin=1000000,
                name="Test Note",
                tags=["test"],
                color="blue"
            )
            assert note_data, "Note creation failed"
            note_id = note_data["id"]

            # Test get note
            note = await self.get_note(admin_token, note_id)
            assert note, "Getting note failed"
            assert note["text"] == "Test note", "Note text mismatch"

            # Test update note
            updated = await self.update_note(
                admin_token,
                note_id,
                text="Updated note",
                tags=["test", "updated"]
            )
            assert updated, "Note update failed"
            
            # Verify update
            updated_note = await self.get_note(admin_token, note_id)
            assert updated_note["text"] == "Updated note", "Note update verification failed"

            # Test list notes
            notes = await self.list_notes(admin_token, operation_id="test_op")
            assert notes and len(notes) >= 1, "Note listing failed"

            # Test delete note
            assert await self.delete_note(admin_token, note_id), "Note deletion failed"
            
            # Verify deletion
            await self.get_note(admin_token, note_id, expected_status=404)

            self.logger.info("Note tests completed successfully!")

        except Exception as ex:
            self.logger.error(f"Note test failed: {str(ex)}")
            raise

def main():
    MutyLogger.get_instance("test_collab", level=logging.DEBUG)
    args = _parse_args()
    tester = GulpCollabTester(args.host)
    import asyncio
    asyncio.run(tester.run_note_tests())

if __name__ == "__main__":
    main()