"""
Server utility functions for the GULP REST API.

This module provides utility functions for the GULP server, including:
- Parameter dumping for debugging
- Email sending functionality
- Handling of chunked file uploads with resume capability

These utilities support the server-side operations of the GULP API by providing
common functionality needed across different endpoints and request handlers.
"""

import inspect
import json
import os
import re
import ssl
from email.message import EmailMessage
from typing import Tuple

import aiofiles
import aiosmtplib
import muty.crypto
import muty.file
from fastapi import Request
from muty.log import MutyLogger
from requests_toolbelt.multipart import decoder

from gulp.api.collab.context import GulpContext
from gulp.api.rest.structs import GulpUploadResponse
from gulp.config import GulpConfig


class ServerUtils:
    @staticmethod
    def dump_params(params: dict) -> str:
        """
        Dumps the parameters dictionary as a string.

        Args:
            params (dict): The parameters dictionary.

        Returns:
            str: The string representation of the parameters.
        """
        caller_frame = inspect.currentframe().f_back
        caller_name = caller_frame.f_code.co_name
        MutyLogger.get_instance().debug(
            "---> %s() params: %s" % (caller_name, json.dumps(params, indent=2))
        )

    @staticmethod
    async def send_mail(
        smtp_server: str,
        subject: str,
        content: str,
        sender: str,
        to: list[str],
        username: str = None,
        password: str = None,
        use_ssl: bool = False,
    ) -> None:
        """
        Sends an email using the specified SMTP server.

        Args:
            smtp_server (str): The SMTP server address as host:port.
            subject (str): The subject of the email.
            content (str): The content of the email.
            sender (str): The email address of the sender.
            to (list[str]): The email addresses of the recipients: to[0]=TO, optionally to[1...n]=CC.
            username (str, optional): The username for authentication. Defaults to None.
            password (str, optional): The password for authentication. Defaults to None.
            use_ssl (bool, optional): Whether to use SSL/TLS for the connection. Defaults to False.

        Returns:
            None: This function does not return anything.
        """

        splitted = smtp_server.split(":")
        server = splitted[0]
        port = int(splitted[1])
        to_email = to[0]
        cc_list = None
        if len(to) > 1:
            cc_list = to[1:]

        m = EmailMessage()
        MutyLogger.get_instance().info(
            "sending mail using %s:%d, from %s to %s, cc=%s, subject=%s"
            % (server, port, sender, to_email, cc_list, subject)
        )
        m["From"] = sender
        m["To"] = to_email
        m["Subject"] = subject
        if cc_list is not None:
            m["cc"] = cc_list
        m.set_content(content)
        ssl_ctx = None
        if use_ssl is not None:
            ssl_ctx = ssl.create_default_context()
        await aiosmtplib.send(
            m,
            hostname=server,
            port=port,
            username=username,
            password=password,
            tls_context=ssl_ctx,
            validate_certs=False,
        )

    @staticmethod
    async def handle_multipart_chunked_upload(
        r: Request, operation_id: str, context_name: str
    ) -> Tuple[str, dict, GulpUploadResponse]:
        """
        Handles a chunked upload request with multipart content (file and json), with resume support.

        1. Parse the request headers to get the "continue_offset" and "size" (the TOTAL file size), used to check the upload status.
        2. Decode the multipart data and parses the JSON payload, if any.
        3. Extract the "filename" from the Content-Disposition header.
        4. Writes the file chunk to the cache directory using operation_id, context_name, and the original filename to build a unique filename.
        5. Verify the upload status.
        6. Return the cache file path and the upload response object.

        Args:
            r (Request): The FastAPI request object.
            operation_id (str): The operation ID.
            context_name (str): The context name.
            req_id (str): The request ID, to allow resuming a previously interrupted upload.

        Returns:
            Tuple[str, dict, GulpUploadResponse]: A tuple containing:
                - the downloaded file path
                - the parsed JSON payload, if any
                - the upload response object to be returned to the client.
        """

        async def _parse_payload(content: bytes) -> dict:
            """Parse JSON payload from multipart content."""
            try:
                # validate the uploaded content
                payload = content.decode("utf-8")
                payload_dict = json.loads(payload)
                return payload_dict
            except Exception:
                MutyLogger.get_instance().error(f"invalid payload: {content}")
                return None

        def _extract_filename(content_disposition: str) -> str:
            """extract filename from Content-Disposition header."""
            if not content_disposition:
                raise ValueError("Empty Content-Disposition header")

            # normalize to lowercase and remove extra whitespace
            content_disposition = content_disposition.lower().strip()

            # find filename parameter
            filename_match = re.search(r"filename\s*=\s*([^;\s]+)", content_disposition)
            if not filename_match:
                raise ValueError('No "filename" found in Content-Disposition header')

            # Extract and clean filename
            filename = filename_match.group(1)
            filename = filename.strip("\"'")  # remove quotes
            filename = filename.strip()  # remove any remaining whitespace

            if not filename:
                raise ValueError("Empty filename in Content-Disposition header")

            return filename

        MutyLogger.get_instance().debug("headers=%s" % (r.headers))

        # parse request headers, continue_offset=0 (first chunk) is assumed if missing
        continue_offset = int(r.headers.get("continue_offset", 0))
        total_file_size = int(r.headers["size"])
        if total_file_size == 0:
            return (
                None,
                None,
                GulpUploadResponse(
                    done=True, continue_offset=None, error="file size is 0"
                ),
            )
        # decode multipart data
        data = decoder.MultipartDecoder(await r.body(), r.headers["content-type"])
        json_part, file_part = data.parts[0], data.parts[1]

        # parse JSON payload
        payload_dict = await _parse_payload(json_part.content) or {}

        # extract filename and prepare path
        MutyLogger.get_instance().debug("file_part.headers=%s" % (file_part.headers))
        filename = _extract_filename(
            file_part.headers[b"Content-Disposition"].decode("utf-8")
        )
        cache_dir = GulpConfig.get_instance().upload_tmp_dir()

        # build a unique filename
        unique_filename = "%s-%s" % (
            GulpContext.make_context_id_key(operation_id, context_name),
            filename,
        )
        cache_file_path = muty.file.safe_path_join(cache_dir, unique_filename)

        # Check if file is already complete
        current_size = await muty.file.get_size(cache_file_path)
        MutyLogger.get_instance().debug(
            "cache_file_path=%s, continue_offset=%d, current_size=%d, total_file_size=%d, filename=%s, cache_file_path=%s, payload_dict=%s"
            % (
                cache_file_path,
                continue_offset,
                current_size,
                total_file_size,
                filename,
                cache_file_path,
                payload_dict,
            )
        )

        if current_size < total_file_size:
            if continue_offset != current_size:
                # continue_offset must be equal to the current file size
                return (
                    cache_file_path,
                    payload_dict,
                    GulpUploadResponse(done=False, continue_offset=current_size),
                )

            # write file chunk at the specified offset
            async with aiofiles.open(cache_file_path, "ab+") as f:
                await f.seek(continue_offset, os.SEEK_SET)
                await f.truncate()
                await f.write(file_part.content)
                await f.flush()

        # verify upload status
        current_written_size = await muty.file.get_size(cache_file_path)
        current_hash = await muty.crypto.hash_sha1_file(cache_file_path)
        if current_written_size >= total_file_size:
            if "file_sha1" in payload_dict:
                if current_hash != payload_dict["file_sha1"]:
                    # delete uploaded file
                    muty.file.delete_file_or_dir(cache_file_path)
                    raise ValueError(
                        f"file is complete but file hash/file size mismatch: current_file_size={current_written_size}, expected={
                            total_file_size}, current_sha1={current_hash}, expected={payload_dict['file_sha1']}"
                    )

        # notify back upload status
        is_complete = current_written_size == total_file_size
        result = GulpUploadResponse(
            done=is_complete,
            continue_offset=None if is_complete else current_written_size,
        )
        MutyLogger.get_instance().debug(
            "file_path=%s,\npayload=%s,\nresult=%s"
            % (cache_file_path, json.dumps(payload_dict, indent=2), result)
        )

        return (cache_file_path, payload_dict, result)
