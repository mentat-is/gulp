#!/usr/bin/env python3
"""
Script to test gULP user management API endpoints
"""

import argparse
import json
import logging
import os
import requests
from typing import Optional
from muty.log import MutyLogger


def _parse_args():
    parser = argparse.ArgumentParser(description="Test gULP user management endpoints.")
    parser.add_argument(
        "--host", default="localhost:8080", help="Gulp host", metavar="HOST:PORT"
    )
    parser.add_argument(
        "--mode",
        choices=["admin", "guest"],
        default="admin",
        help="Test mode - admin or guest user",
    )
    return parser.parse_args()


def _make_url(host: str, endpoint: str) -> str:
    return f"http://{host}/{endpoint}"


class GulpUserTester:
    def __init__(self, host: str):
        self.host = host
        self.logger = MutyLogger.get_instance("test_user")

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
        body: dict | list[str] = None,
        token: Optional[str] = None,
        expected_status: int = 200,
    ):
        """Make HTTP request and verify status"""
        url = _make_url(self.host, endpoint)

        # Setup headers if token provided
        headers = {"token": token} if token else {}

        # Add req_id to query params
        params = params or {}
        params["req_id"] = "test_req"

        self._log_request(
            method, url, {"params": params, "body": body, "headers": headers}
        )

        # Handle request based on method and params
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

    async def logout(self, token: str) -> bool:
        self.logger.info(f"Logging out token {token}...")
        """Logout user"""
        params = {"ws_id": "test_ws"}
        return (
            await self._make_request("PUT", "logout", params=params, token=token)
            is not None
        )

    async def create_user(
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

    async def delete_user(
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

    async def update_user(
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

    async def list_users(
        self, token: str, expected_status: int = 200
    ) -> Optional[list]:
        self.logger.info("Listing users...")
        """List all users"""
        return await self._make_request(
            "GET", "user_list", params={}, token=token, expected_status=expected_status
        )

    async def get_user(
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

    async def verify_user_exists(
        self,
        token: str,
        username: str,
        expected_fields: dict = None,
        expected_status: int = 200,
    ) -> bool:
        self.logger.info(f"Verifying user {username} exists...")

        """Verify user exists and optionally check fields"""
        user = await self.get_user(token, username, expected_status=expected_status)
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

    async def run_admin_tests(self):
        """Run test sequence as admin"""
        try:
            self.logger.info("Starting admin user API tests...")

            # Login as admin
            admin_token = await self.login("admin", "admin")
            assert admin_token, "Admin login failed"

            # Test user creation
            test_user = "test_user"
            test_password = "Test123!"
            user_data = await self.create_user(
                admin_token, test_user, test_password, ["read"], "test@example.com"
            )
            assert user_data, "User creation failed"
            assert await self.verify_user_exists(
                admin_token,
                test_user,
                {"email": "test@example.com", "permission": ["read"]},
            ), "User verification failed"

            # Test user listing
            users = await self.list_users(admin_token)
            assert users and len(users) >= 1, "User listing failed"

            # Test user update
            updated = await self.update_user(
                admin_token,
                test_user,
                permission=["read", "edit"],
                email="updated@example.com",
            )
            assert updated, "User update failed"
            assert await self.verify_user_exists(
                admin_token,
                test_user,
                {"email": "updated@example.com", "permission": ["read", "edit"]},
            ), "Update verification failed"

            # Test user deletion
            assert await self.delete_user(
                admin_token, test_user
            ), "User deletion failed"
            assert not await self.verify_user_exists(
                admin_token, test_user, expected_status=404
            ), "User still exists after deletion"

            # Cleanup
            await self.logout(admin_token)
            self.logger.info("Admin tests completed successfully!")

        except Exception as ex:
            self.logger.error(f"Admin test failed: {str(ex)}")
            raise

    async def run_guest_tests(self):
        """Run test sequence as guest"""
        try:
            self.logger.info("Starting guest user API tests...")

            # Login as guest
            guest_token = await self.login("guest", "guest")
            assert guest_token, "Guest login failed"

            # These operations should fail for guest
            await self.create_user(
                guest_token, "new_user", "Password#1234!", ["read"], expected_status=401
            )
            await self.list_users(guest_token, expected_status=401)
            await self.delete_user(guest_token, "admin", expected_status=401)

            # Guest should not be able to update its own permission
            await self.update_user(
                guest_token, "guest", permission=["read", "edit"], expected_status=401
            )

            # Guest should be able to get their own details
            guest_data = await self.get_user(guest_token, "guest")
            assert guest_data, "Getting own user details failed"

            # Guest should be able to update their own password or email
            updated = await self.update_user(
                guest_token,
                "guest",
                password="Password#1234!",
                email="mynewemail@email.com",
            )
            assert updated, "Details update failed"

            # Guest should not be able to update other users
            await self.update_user(
                guest_token, "admin", password="Hacked#1234!", expected_status=401
            )

            # Cleanup
            await self.logout(guest_token)
            self.logger.info("Guest tests completed successfully!")

        except Exception as ex:
            self.logger.error(f"Guest test failed: {str(ex)}")
            raise


def main():
    MutyLogger.get_instance("test_user", level=logging.DEBUG)
    args = _parse_args()

    tester = GulpUserTester(args.host)

    import asyncio

    if args.mode == "admin":
        asyncio.run(tester.run_admin_tests())
    else:
        asyncio.run(tester.run_guest_tests())


if __name__ == "__main__":
    main()
