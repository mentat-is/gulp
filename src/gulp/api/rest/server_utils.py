import json
import os
import re
import ssl
from email.message import EmailMessage
from typing import Optional, Tuple

import aiofiles
import aiosmtplib
import muty.crypto
import muty.file
from fastapi import Request
from muty.log import MutyLogger
from pydantic import BaseModel, Field
from requests_toolbelt.multipart import decoder

from gulp.config import GulpConfig


class GulpUploadResponse(BaseModel):
    """
    the ingest API may respond with this object to indicate the status of an unfinished upload.
    """

    done: bool = Field(..., description="Indicates whether the upload is complete.")
    continue_offset: Optional[int] = Field(
        0, description="The offset of the next chunk to be uploaded, to resume."
    )


class ServerUtils:
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
        r: Request, operation_id: str, context_id: str, req_id: str
    ) -> Tuple[str, dict, GulpUploadResponse]:
        """
        Handles a chunked upload request with multipart content (file and json), with resume support.

        1. Parse the request headers to get the "continue_offset" and "total_file_size", used to check the upload status.
        2. Decode the multipart data and parses the JSON payload, if any.
        3. Extract the "filename" from the Content-Disposition header.
        4. Writes the file chunk to the cache directory using operation_id, context_id, req_id and the original filename to build a unique filename.
        5. Verify the upload status.
        6. Return the cache file path and the upload response object.

        Args:
            r (Request): The FastAPI request object.
            operation_id (str): The operation ID.
            context_id (str): The context ID.
            req_id (str): The request ID, to allow resuming a previously interrupted upload.

        Returns:
            Tuple[str, dict, GulpUploadResponse]: A tuple containing:
                - the file path
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
                MutyLogger.get_instance().error(f"invalid payload: {payload}")
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
            filename = filename.strip("\"'")  # Remove quotes
            filename = filename.strip()  # Remove any remaining whitespace

            if not filename:
                raise ValueError("Empty filename in Content-Disposition header")

            return filename

        # Parse request headers, continue_offset=0 (first chunk) is assumed if missing
        continue_offset = int(r.headers.get("continue_offset", 0))
        total_file_size = int(r.headers["size"])

        # Decode multipart data
        data = decoder.MultipartDecoder(await r.body(), r.headers["content-type"])
        json_part, file_part = data.parts[0], data.parts[1]

        # Parse JSON payload
        payload_dict = await _parse_payload(json_part.content) or {}

        # Extract filename and prepare path
        filename = _extract_filename(
            file_part.headers[b"Content-Disposition"].decode("utf-8")
        )
        cache_dir = GulpConfig.get_instance().upload_tmp_dir()

        # build a unique filename based on the operation_id, context_id, req_id and the original filename
        h = "%s-%s" % (muty.crypto.hash_xxh128(f"{operation_id}-{context_id}-{req_id}"), filename)
        cache_file_path = muty.file.safe_path_join(
            cache_dir, h)

        # Check if file is already complete
        current_size = await muty.file.get_size(cache_file_path)
        if current_size == total_file_size:
            # Upload is already complete !
            MutyLogger.get_instance().info(
                "file size matches, upload is already complete!"
            )
            return cache_file_path, payload_dict, GulpUploadResponse(done=True)

        # Write file chunk at the specified offset
        async with aiofiles.open(cache_file_path, "ab+") as f:
            await f.seek(continue_offset, os.SEEK_SET)
            await f.truncate()
            await f.write(file_part.content)
            await f.flush()

        # Verify upload status
        current_written_size = await muty.file.get_size(cache_file_path)
        is_complete = current_written_size == total_file_size

        return (
            cache_file_path,
            payload_dict,
            GulpUploadResponse(
                done=is_complete,
                continue_offset=None if is_complete else current_written_size,
            ),
        )
