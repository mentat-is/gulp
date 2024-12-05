#!/usr/bin/env python3
"""
Script to test gULP collaboration object API endpoints, focusing on notes
"""

import argparse
import json
import logging
import pprint
import requests
from typing import Optional
from muty.log import MutyLogger
from gulp.api.collab.structs import GulpCollabFilter, GulpCollabType
from gulp.api.rest.test_values import (
    TEST_CONTEXT_ID,
    TEST_INDEX,
    TEST_OPERATION_ID,
    TEST_REQ_ID,
    TEST_SOURCE_ID,
    TEST_WS_ID,
)
from gulp.api.opensearch.structs import GulpBasicDocument


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Test gULP collaboration object endpoints."
    )
    parser.add_argument(
        "--host", default="localhost:8080", help="Gulp host", metavar="HOST:PORT"
    )
    parser.add_argument("--ws_id", default=TEST_WS_ID, help="websocket ID")
    parser.add_argument("--req_id", default=TEST_REQ_ID, help="request ID")
    parser.add_argument(
        "--index",
        default=TEST_INDEX,
        help="index to reset (ignored if --reset is not set)",
    )
    return parser.parse_args()


class GulpCollabTester:
    def __init__(self, host: str):
        self.host = host
        self.logger = MutyLogger.get_instance("test_collab")

    def _make_url(self, host: str, endpoint: str) -> str:
        return f"http://{host}/{endpoint}"

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
        url = self._make_url(self.host, endpoint)
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

    async def _logout(self, token: str) -> bool:
        self.logger.info(f"Logging out token {token}...")
        """Logout user"""
        params = {"ws_id": TEST_WS_ID}
        return (
            await self._make_request("PUT", "logout", params=params, token=token)
            is not None
        )

    async def _login(
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

    async def _create_note(
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

    async def _update_note(
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

    async def _delete_note(
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

    async def _get_note(
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

    async def _make_object_public_or_private(
        self,
        token: str,
        req_id: str,
        object_id: str,
        object_type: GulpCollabType,
        private: bool,
        expected_status: int = 200,
    ) -> Optional[dict]:
        """Make object public or private"""
        self.logger.info("Making object %s private: %r" % (object_id, private))
        if private:
            api = "object_make_private"
        else:
            api = "object_make_public"
        params = {"object_id": object_id, "type": object_type.value, "req_id": req_id}
        return await self._make_request(
            "PATCH",
            api,
            params=params,
            token=token,
            expected_status=expected_status,
        )

    async def _add_remove_user_grant(
        self,
        token: str,
        req_id: str,
        object_id: str,
        object_type: GulpCollabType,
        user_id: str,
        remove: bool,
        expected_status: int = 200,
    ) -> Optional[dict]:
        """Make object public or private"""
        self.logger.info("Adding grant on object %s to user %s" % (object_id, user_id))
        if remove:
            api = "object_remove_granted_user"
        else:
            api = "object_add_granted_user"
        params = {
            "object_id": object_id,
            "type": object_type.value,
            "user_id": user_id,
            "req_id": req_id,
        }
        return await self._make_request(
            "PATCH",
            api,
            params=params,
            token=token,
            expected_status=expected_status,
        )

    async def _add_remove_group_grant(
        self,
        token: str,
        req_id: str,
        object_id: str,
        object_type: GulpCollabType,
        group_id: str,
        remove: bool,
        expected_status: int = 200,
    ) -> Optional[dict]:
        """Make object public or private"""
        self.logger.info(
            "Adding grant on object %s to group %s" % (object_id, group_id)
        )
        if remove:
            api = "object_remove_granted_group"
        else:
            api = "object_add_granted_group"
        params = {
            "object_id": object_id,
            "type": object_type.value,
            "group_id": group_id,
            "req_id": req_id,
        }
        return await self._make_request(
            "PATCH",
            api,
            params=params,
            token=token,
            expected_status=expected_status,
        )

    async def _add_remove_user_to_group(
        self,
        token: str,
        req_id: str,
        user_id: str,
        group_id: str,
        remove: bool,
        expected_status: int = 200,
    ) -> Optional[dict]:
        """Make object public or private"""
        self.logger.info("Adding user_id %s to group %s" % (user_id, group_id))
        if remove:
            api = "user_group_remove_user"
        else:
            api = "user_group_add_user"
        params = {
            "group_id": group_id,
            "user_id": user_id,
            "req_id": req_id,
        }
        return await self._make_request(
            "PATCH",
            api,
            params=params,
            token=token,
            expected_status=expected_status,
        )

    async def _list_notes(
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

    async def _delete_user(
        self, token: str, username: str, expected_status: int = 200
    ) -> bool:
        """Delete user"""
        self.logger.info(f"Deleting user {username}...")
        params = {"user_id": username}
        return (
            await self._make_request(
                "DELETE",
                "user_delete",
                params=params,
                token=token,
                expected_status=expected_status,
            )
            is not None
        )

    async def _update_user(
        self,
        token: str,
        username: str,
        password: Optional[str] = None,
        permission: Optional[list[str]] = None,
        email: Optional[str] = None,
        expected_status: int = 200,
    ) -> Optional[dict]:
        """Update user"""
        body = None
        params = {"user_id": username}
        if password:
            params["password"] = password
        if permission:
            body = permission
        if email:
            params["email"] = email
        return await self._make_request(
            "PATCH",
            "user_update",
            params=params,
            body=body,
            token=token,
            expected_status=expected_status,
        )

    async def _list_users(
        self, token: str, expected_status: int = 200
    ) -> Optional[list]:
        self.logger.info("Listing users...")
        """List all users"""
        return await self._make_request(
            "GET", "user_list", params={}, token=token, expected_status=expected_status
        )

    async def _get_user(
        self, token: str, username: str, expected_status: int = 200
    ) -> Optional[dict]:
        self.logger.info(f"Getting user {username}...")
        """Get user details"""
        params = {"user_id": username}
        return await self._make_request(
            "GET",
            "user_get_by_id",
            params=params,
            token=token,
            expected_status=expected_status,
        )

    async def _create_user(
        self,
        token: str,
        username: str,
        password: str,
        permission: list[str],
        email: Optional[str] = None,
        expected_status: int = 200,
    ) -> Optional[dict]:
        self.logger.info(f"Creating user {username}...")
        """Create new user"""
        params = {"user_id": username, "password": password}
        body = permission
        if email:
            params["email"] = email
        return await self._make_request(
            "POST",
            "user_create",
            params=params,
            body=body,
            token=token,
            expected_status=expected_status,
        )

    async def _reset_gulp_collab(self, args) -> bool:
        self.logger.info("Resetting gULP...")
        """Reset gULP collab db"""

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
            "POST",
            "postgres_init",
            params={"restart_processes": False},
            token=token,
        )

    async def _verify_user_exists(
        self,
        token: str,
        username: str,
        expected_fields: dict = None,
        expected_status: int = 200,
    ) -> bool:
        self.logger.info(f"Verifying user {username} exists...")

        """Verify user exists and optionally check fields"""
        user = await self._get_user(token, username, expected_status=expected_status)
        if not user:
            return False

        if expected_fields:
            for k, v in expected_fields.items():
                if user.get(k) != v:
                    self.logger.error(
                        f"Field {k} mismatch: expected {v}, got {user.get(k)}"
                    )
                    return False
        return True

    async def run_acl_test(self, args):
        try:
            ws_id = args.ws_id
            req_id = args.req_id

            # reset first
            await self._reset_gulp_collab(args)

            # login editor, admin, guest, power
            admin_token = await self._login("admin", "admin", ws_id, req_id)
            assert admin_token, "admin login failed"

            # create another editor user
            editor2_pwd = "MyPassword1234!"
            editor2_user = await self._create_user(
                admin_token,
                "editor2",
                editor2_pwd,
                ["edit", "read"],
                email="editor2@localhost.com",
            )
            assert editor2_user["id"] == "editor2", "editor2 creation failed"
            power_token = await self._login("power", "power", ws_id, req_id)
            assert power_token, "power login failed"
            editor_token = await self._login("editor", "editor", ws_id, req_id)
            assert editor_token, "editor login failed"
            guest_token = await self._login("guest", "guest", ws_id, req_id)
            assert guest_token, "guest login failed"
            editor2_token = await self._login("editor2", editor2_pwd, ws_id, req_id)
            assert editor2_token, "editor2 login failed"

            # create a note by editor
            note_data = await self._create_note(
                editor_token,
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
            assert note_data["text"] == "Test note", "Note creation failed"

            # guest can see the note
            note = await self._get_note(
                guest_token, req_id, note_data["id"], expected_status=200
            )
            assert note["id"] == note_data["id"], "Guest cannot see note"

            # guest cannot edit the note
            updated = await self._update_note(
                guest_token,
                ws_id,
                req_id,
                note_data["id"],
                text="Updated note",
                tags=["test", "updated"],
                expected_status=401,
            )

            # editor2 cannot delete the note
            await self._delete_note(
                editor2_token, ws_id, req_id, note_data["id"], expected_status=401
            )

            # editor can edit the note
            updated = await self._update_note(
                editor_token,
                ws_id,
                req_id,
                note_data["id"],
                text="Updated note",
                tags=["test", "updated"],
            )
            assert updated["text"] == "Updated note", "Note update failed"

            # editor can delete the note
            assert await self._delete_note(
                editor_token, ws_id, req_id, note_data["id"]
            ), "Note deletion failed"

            # create couple of notes
            note_data = await self._create_note(
                editor_token,
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
            assert note_data["text"] == "Test note", "Note creation failed"

            docs = [
                GulpBasicDocument(
                    id="test_doc",
                    timestamp="2019-01-01T00:00:00Z",
                    gulp_timestamp=1000000,
                    operation_id=TEST_OPERATION_ID,
                    context_id=TEST_CONTEXT_ID,
                    source_id=TEST_SOURCE_ID,
                ).model_dump(by_alias=True, exclude_none=True),
                GulpBasicDocument(
                    id="test_doc2",
                    timestamp="2019-01-01T00:00:01Z",
                    gulp_timestamp=1000001,
                    operation_id=TEST_OPERATION_ID,
                    context_id=TEST_CONTEXT_ID,
                    source_id=TEST_SOURCE_ID,
                ).model_dump(by_alias=True, exclude_none=True),
                GulpBasicDocument(
                    id="test_doc3",
                    timestamp="2019-01-01T00:00:03Z",
                    gulp_timestamp=1000002,
                    operation_id=TEST_OPERATION_ID,
                    context_id=TEST_CONTEXT_ID,
                    source_id=TEST_SOURCE_ID,
                ).model_dump(by_alias=True, exclude_none=True),
            ]
            note_2_data = await self._create_note(
                editor_token,
                ws_id,
                req_id,
                operation_id=TEST_OPERATION_ID,
                context_id=TEST_CONTEXT_ID,
                source_id=TEST_SOURCE_ID,
                text="Test note 2",
                docs=docs,
                name="Test Note 2",
                tags=["test"],
                color="purple",
            )
            assert len(note_2_data["docs"]) == 3, "Note creation (with docs) failed"

            note_3_data = await self._create_note(
                editor_token,
                ws_id,
                req_id,
                operation_id=TEST_OPERATION_ID,
                context_id=TEST_CONTEXT_ID,
                source_id=TEST_SOURCE_ID,
                text="Test note 3",
                docs=[
                    GulpBasicDocument(
                        id="test_doc4",
                        timestamp="2019-01-01T01:00:00Z",
                        gulp_timestamp=1000008,
                        operation_id=TEST_OPERATION_ID,
                        context_id=TEST_CONTEXT_ID,
                        source_id=TEST_SOURCE_ID,
                    ).model_dump(by_alias=True, exclude_none=True),
                    GulpBasicDocument(
                        id="test_doc5",
                        timestamp="2019-01-01T02:00:01Z",
                        gulp_timestamp=1000009,
                        operation_id=TEST_OPERATION_ID,
                        context_id=TEST_CONTEXT_ID,
                        source_id=TEST_SOURCE_ID,
                    ).model_dump(by_alias=True, exclude_none=True),
                ],
                name="Test Note 3",
                tags=["test"],
                color="blue",
            )
            assert len(note_3_data["docs"]) == 2, "Note 3 creation (with docs) failed"

            note_4_data = await self._create_note(
                editor_token,
                ws_id,
                req_id,
                operation_id=TEST_OPERATION_ID,
                context_id=TEST_CONTEXT_ID,
                source_id=TEST_SOURCE_ID,
                text="Test note 4",
                time_pin=2000000,
                name="Test Note 4",
                tags=["test"],
                color="blue",
            )
            assert note_4_data["time_pin"] == 2000000, "Note 4 creation failed"

            # cannot create note with both data and pin
            n = await self._create_note(
                editor_token,
                ws_id,
                req_id,
                operation_id=TEST_OPERATION_ID,
                context_id=TEST_CONTEXT_ID,
                source_id=TEST_SOURCE_ID,
                text="Test note",
                time_pin=1000000,
                docs=docs,
                name="Test Note",
                tags=["test"],
                color="blue",
                expected_status=400,
            )
            assert not n, "Note creation (with docs and pin) failed"

            # cannot create note with no docs and no pin
            n = await self._create_note(
                editor_token,
                ws_id,
                req_id,
                operation_id=TEST_OPERATION_ID,
                context_id=TEST_CONTEXT_ID,
                source_id=TEST_SOURCE_ID,
                text="Test note",
                name="Test Note",
                tags=["test"],
                color="blue",
                expected_status=400,
            )
            assert not n, "Note creation (with no docs and no pin"

            # guest can see note
            n = await self._get_note(
                guest_token, req_id, note_data["id"], expected_status=200
            )
            assert n["id"] == note_data["id"], "Guest cannot see note"

            # guest can also list notes (by doc)
            flt = GulpCollabFilter(
                operation_ids=[TEST_OPERATION_ID],
                context_ids=[TEST_CONTEXT_ID],
                source_ids=[TEST_SOURCE_ID],
                doc_ids=["test_doc2"],
            )
            notes = await self._list_notes(
                guest_token,
                req_id,
                flt=flt,
            )
            assert (
                notes and len(notes) == 1 and notes[0]["id"] == note_2_data["id"]
            ), "Note listing failed (guest)"

            # or by time
            flt = GulpCollabFilter(
                operation_ids=[TEST_OPERATION_ID],
                context_ids=[TEST_CONTEXT_ID],
                source_ids=[TEST_SOURCE_ID],
                doc_time_range=(1000007, 1000009),
            )
            notes = await self._list_notes(
                guest_token,
                req_id,
                flt=flt,
            )
            assert (
                notes and len(notes) == 1 and notes[0]["id"] == note_3_data["id"]
            ), "Note listing failed (guest)"

            # editor2 cannot make note private (not owner)
            updated = await self._make_object_public_or_private(
                editor2_token,
                req_id,
                note_3_data["id"],
                GulpCollabType.NOTE,
                private=True,
                expected_status=401,
            )

            # editor can make note private
            updated = await self._make_object_public_or_private(
                editor_token,
                req_id,
                note_3_data["id"],
                GulpCollabType.NOTE,
                private=True,
            )

            # guest cannot see note 3 anymore, sees note 4 due to the timepin range
            flt = GulpCollabFilter(
                operation_ids=[TEST_OPERATION_ID],
                context_ids=[TEST_CONTEXT_ID],
                source_ids=[TEST_SOURCE_ID],
                doc_time_range=(1000007, 1000009),
            )
            notes = await self._list_notes(
                guest_token,
                req_id,
                flt=flt,
            )
            flt = GulpCollabFilter(
                operation_ids=[TEST_OPERATION_ID],
                context_ids=[TEST_CONTEXT_ID],
                source_ids=[TEST_SOURCE_ID],
                time_pin_range=(1900000, 2000009),
            )
            nn = await self._list_notes(
                guest_token,
                req_id,
                flt=flt,
            )
            notes.extend(nn)
            pprint.pprint(notes)
            assert (
                notes and len(notes) == 1 and notes[0]["id"] == note_4_data["id"]
            ), "guest shouldn't see private note 3 and should see note 4"

            # editor can add guest to object grants
            updated = await self._add_remove_user_grant(
                editor_token,
                req_id,
                note_3_data["id"],
                GulpCollabType.NOTE,
                "guest",
                remove=False,
            )

            # guest can see note 3 again
            flt = GulpCollabFilter(
                operation_ids=[TEST_OPERATION_ID],
                context_ids=[TEST_CONTEXT_ID],
                source_ids=[TEST_SOURCE_ID],
                doc_time_range=(1000007, 1000009),
            )
            notes = await self._list_notes(
                guest_token,
                req_id,
                flt=flt,
            )
            assert (
                notes and len(notes) == 1 and notes[0]["id"] == note_3_data["id"]
            ), "guest should see note 3 again"

            # editor can remove guest from object grants
            updated = await self._add_remove_user_grant(
                editor_token,
                req_id,
                note_3_data["id"],
                GulpCollabType.NOTE,
                "guest",
                remove=True,
            )

            # guest can't see the note again
            notes = await self._list_notes(
                guest_token,
                req_id,
                flt=flt,
            )
            assert not notes, "guest shouldn't see private note 3 (grant removed)"

            # guest cannot add itself to admin group
            updated = await self._add_remove_user_to_group(
                guest_token,
                req_id,
                "guest",
                "administrators",
                remove=False,
                expected_status=401,
            )

            # admin can add guest to admin group
            updated = await self._add_remove_user_to_group(
                admin_token,
                req_id,
                "guest",
                "administrators",
                remove=False,
            )

            # guest can now see note 3 again (he is admin)
            notes = await self._list_notes(
                guest_token,
                req_id,
                flt=flt,
            )
            assert (
                notes and len(notes) == 1 and notes[0]["id"] == note_3_data["id"]
            ), "guest should see private note 3 (admin group)"

            # verify guest is an admin
            flt = GulpCollabFilter(
                ids=["guest"],
            )
            # this should not fail
            users = await self._list_users(guest_token)
            guest_user = None
            for user in users:
                if user["id"] == "guest":
                    guest_user = user
                    break
            assert guest_user, "guest not found in user list"

            groups = guest_user.get("groups", [])
            assert groups, "guest has no groups"

            for g in groups:
                if g["id"] == "administrators":
                    MutyLogger.get_instance().info("all NOTES/ACL tests succeeded!")
                    return
            raise Exception("test failed!")

        except Exception as ex:
            self.logger.error(f"NOTES/ACL test failed: {str(ex)}")
            raise

    async def run_users_test(self, args):
        try:
            ws_id = args.ws_id
            req_id = args.req_id

            # reset first
            await self._reset_gulp_collab(args)

            # login editor, admin, guest, power
            admin_token = await self._login("admin", "admin", ws_id, req_id)
            assert admin_token, "admin login failed"

            guest_token = await self._login("guest", "guest", ws_id, req_id)
            assert guest_token, "guest login failed"

            # Test user creation
            test_user = "test_user"
            test_password = "Test123!"
            user_data = await self._create_user(
                admin_token, test_user, test_password, ["read"], "test@example.com"
            )
            assert user_data, "User creation failed"
            assert await self._verify_user_exists(
                admin_token,
                test_user,
                {"email": "test@example.com", "permission": ["read"]},
            ), "User verification failed"

            # test user listing
            users = await self._list_users(admin_token)
            assert users and len(users) >= 1, "User listing failed"

            # test user update
            updated = await self._update_user(
                admin_token,
                test_user,
                permission=["read", "edit"],
                email="updated@example.com",
            )
            assert updated, "User update failed"
            assert await self._verify_user_exists(
                admin_token,
                test_user,
                {"email": "updated@example.com", "permission": ["read", "edit"]},
            ), "Update verification failed"

            # test user deletion
            assert await self._delete_user(
                admin_token, test_user
            ), "User deletion failed"
            assert not await self._verify_user_exists(
                admin_token, test_user, expected_status=404
            ), "User still exists after deletion"

            # logout admin
            await self._logout(admin_token)
            self.logger.info("Admin tests completed successfully!")

            # admin should be logget out
            await self._list_users(admin_token, expected_status=404)

            # guest tests now!

            # guest cannot create, list, delete users
            await self._create_user(
                guest_token, "new_user", "Password#1234!", ["read"], expected_status=401
            )
            await self._list_users(guest_token, expected_status=401)
            await self._delete_user(guest_token, "admin", expected_status=401)

            # guest should not be able to update its own permission
            await self._update_user(
                guest_token, "guest", permission=["read", "edit"], expected_status=401
            )

            # guest should be able to get their own details
            guest_data = await self._get_user(guest_token, "guest")
            assert guest_data, "guest should be able to get its own details"

            # guest should be able to update their own password or email
            updated = await self._update_user(
                guest_token,
                "guest",
                password="Password#1234!",
                email="mynewemail@email.com",
            )
            assert updated, "guest should be able to update its own details"

            # guest should not be able to update his own permission
            await self._update_user(
                guest_token, "guest", permission=["read", "edit"], expected_status=401
            )

            # guest should not be able to update other users
            await self._update_user(
                guest_token, "admin", password="Hacked#1234!", expected_status=401
            )

            MutyLogger.get_instance().info("all USERS tests succeeded!")
        except Exception as ex:
            self.logger.error(f"USERS test failed: {str(ex)}")
            raise

    async def run_guest_tests(self):
        """Run test sequence as guest"""
        try:
            self.logger.info("Starting guest user API tests...")

            # Login as guest
            guest_token = await self._login("guest", "guest")
            assert guest_token, "Guest login failed"

            # These operations should fail for guest
            await self._create_user(
                guest_token, "new_user", "Password#1234!", ["read"], expected_status=401
            )
            await self._list_users(guest_token, expected_status=401)
            await self._delete_user(guest_token, "admin", expected_status=401)

            # Guest should not be able to update its own permission
            await self._update_user(
                guest_token, "guest", permission=["read", "edit"], expected_status=401
            )

            # Guest should be able to get their own details
            guest_data = await self._get_user(guest_token, "guest")
            assert guest_data, "Getting own user details failed"

            # Guest should be able to update their own password or email
            updated = await self._update_user(
                guest_token,
                "guest",
                password="Password#1234!",
                email="mynewemail@email.com",
            )
            assert updated, "Details update failed"

            # Guest should not be able to update his own permission
            await self._update_user(
                guest_token, "guest", permission=["read", "edit"], expected_status=401
            )

            # Guest should not be able to update other users
            await self._update_user(
                guest_token, "admin", password="Hacked#1234!", expected_status=401
            )

            # Cleanup
            await self._logout(guest_token)
            self.logger.info("Guest tests completed successfully!")

        except Exception as ex:
            self.logger.error(f"Guest test failed: {str(ex)}")
            raise


def main():
    MutyLogger.get_instance("test_collab", level=logging.DEBUG)
    args = _parse_args()
    tester = GulpCollabTester(args.host)
    import asyncio

    # users
    asyncio.run(tester.run_users_test(args))

    # acl/notes
    asyncio.run(tester.run_acl_test(args))

    # still miss other collab object tests, but these are enough to begin with ...
    # (testing notes, users, acl covers already most of the paths)


if __name__ == "__main__":
    main()
