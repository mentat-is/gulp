import inspect
import json
import os
import re
import ssl
from email.message import EmailMessage
from typing import Annotated, Optional, Tuple

import aiofiles
import aiosmtplib
import muty.crypto
import muty.file
import muty.string
from fastapi import Body, Header, Query, Request
from muty.log import MutyLogger
from pydantic import AfterValidator, BaseModel, Field
from requests_toolbelt.multipart import decoder
from fastapi import Depends
from gulp.api.collab.structs import T, GulpCollabFilter, GulpUserPermission
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch.filters import GulpIngestionFilter, GulpQueryFilter
from gulp.config import GulpConfig
from gulp.api.rest import defs as api_defs
from gulp.structs import GulpPluginParameters


class GulpUploadResponse(BaseModel):
    """
    the ingest API may respond with this object to indicate the status of an unfinished upload.
    """

    done: bool = Field(..., description="Indicates whether the upload is complete.")
    continue_offset: Optional[int] = Field(
        0, description="The offset of the next chunk to be uploaded, to resume."
    )


class APIDependencies:
    """
    A class containing static methods to be used as dependencies in FastAPI API endpoints.
    """

    @staticmethod
    def _strip_or_none(value: Optional[str]) -> Optional[str]:
        """
        Strips a string, or returns None if the string is empty.

        Args:
            value (Optional[str]): The string to strip.

        Returns:
            Optional[str]: The stripped string, or None.
        """
        if value:
            v = value.strip()
        else:
            v = None
        return v if v else None

    @staticmethod
    def _pwd_regex_validator(value: str) -> str:
        """
        Validates a password against the password regex.

        Args:
            value (str): The password to validate.

        Returns:
            str: The password if it is valid.
        """
        if GulpConfig.get_instance().debug_allow_insecure_passwords():
            return value

        if not re.match(api_defs.REGEX_CHECK_PASSWORD, value):
            raise ValueError(
                "Password must:\n"
                "- Be 8-64 characters long\n"
                "- Contain at least one uppercase letter\n"
                "- Contain at least one lowercase letter\n"
                "- Contain at least one number\n"
                "- Contain at least one special character (!@#$%^&*()_+-)"
            )
        return value

    @staticmethod
    def _email_regex_validator(value: Optional[str]) -> Optional[str]:
        """
        Validates an email against the email regex.

        Args:
            value (Optional[str]): The email to validate.

        Returns:
            Optional[str]: The email if it is valid.
        """
        if value is None:
            return None

        if not bool(re.match(api_defs.REGEX_CHECK_EMAIL, value)):
            raise ValueError(f"invalid email format: {value}")
        return value

    @staticmethod
    def param_private_optional(
        private: Annotated[
            Optional[bool],
            Body(description=api_defs.API_DESC_PRIVATE, example=False),
        ] = None
    ) -> bool:
        """
        used with fastapi Depends to provide API parameter

        Args:
            private (bool, optional, Body): Whether the object is private. Defaults to None.

        Returns:
            bool: The private flag.
        """
        if private is not None:
            return private
        return None

    @staticmethod
    def param_description_optional(
        description: Annotated[
            Optional[str],
            Body(
                description="the object description.", example="this is a description"
            ),
        ] = None
    ) -> str:
        """
        used with fastapi Depends to provide API parameter

        Args:
            description (str, optional, Body): The description. Defaults to None.

        Returns:
            str: The description.
        """
        return APIDependencies._strip_or_none(description)

    @staticmethod
    def param_display_name_optional(
        name: Annotated[
            Optional[str],
            Query(description="the object display name.", example="object name"),
        ] = None
    ) -> str:
        """
        used with fastapi Depends to provide API parameter

        Args:
            name (str, optional, Query): The display name. Defaults to None.

        Returns:
            str: The name.
        """
        return APIDependencies._strip_or_none(name)

    @staticmethod
    def param_tags_optional(
        tags: Annotated[
            Optional[list[str]],
            Body(
                description="tags to be assigned to the object.",
                example='["tag1","tag2"]',
            ),
        ] = None
    ) -> list[str]:
        """
        used with fastapi Depends to provide API parameter

        Args:
            tags (list[str], optional, Body): The tags. Defaults to None.

        Returns:
            list[str]: The tags.
        """
        if tags:
            # strip each tag, remove empty tags
            tags = [tag.strip() for tag in tags if tag and tag.strip()]

        return tags or []

    @staticmethod
    def param_color_optional(
        color: Annotated[
            Optional[str],
            Query(
                description="the color in #rrggbb or css-name format.", example="yellow"
            ),
        ] = None
    ) -> str:
        """
        used with fastapi Depends to provide API parameter

        Args:
            color (str, optional, Query): The color. Defaults to None.

        Returns:
            str: The color.
        """
        return APIDependencies._strip_or_none(color)

    @staticmethod
    def param_password(
        password: Annotated[
            str,
            Query(description="the user password.", example="Password1!"),
            AfterValidator(_pwd_regex_validator),
        ]
    ) -> str:
        """
        used with fastapi Depends to provide API parameter

        Args:
            password (str, Query): The password.

        Returns:
            str: The password.
        """
        return APIDependencies._strip_or_none(password)

    @staticmethod
    def param_password_optional(
        password: Annotated[
            Optional[str],
            Query(description="the user password.", example="Password1!"),
            AfterValidator(_pwd_regex_validator),
        ] = None
    ) -> str:
        """
        used with fastapi Depends to provide API parameter

        Args:
            password (str, optional, Query): The password. Defaults to None.

        Returns:
            str: The password.
        """
        return APIDependencies._strip_or_none(password)

    def param_permission_optional(
        permission: Annotated[
            Optional[list[GulpUserPermission]],
            Body(description="the user permission.", example='["read","edit"]'),
        ] = None
    ) -> list[GulpUserPermission]:
        """
        used with fastapi Depends to provide API parameter

        Args:
            permission (list[GulpUserPermission], optional, Body): The permission. Defaults to None.

        Returns:
            list[GulpUserPermission]: The permission.
        """
        return permission

    def param_permission(
        permission: Annotated[
            list[GulpUserPermission],
            Body(description="the user permission.", example='["read","edit"]'),
        ] = None
    ) -> list[GulpUserPermission]:
        """
        used with fastapi Depends to provide API parameter

        Args:
            permission (list[GulpUserPermission], optional, Body): The permission. Defaults to None.

        Returns:
            list[GulpUserPermission]: The permission.
        """
        return permission

    def param_email_optional(
        email: Annotated[
            Optional[str],
            Query(description="the user email.", example="user@mail.com"),
            AfterValidator(_email_regex_validator),
        ] = None
    ) -> str:
        """
        used with fastapi Depends to provide API parameter

        Args:
            email (str, optional, Query): The email. Defaults to None.

        Returns:
            str: The email.
        """
        return APIDependencies._strip_or_none(email)

    @staticmethod
    def param_object_id(
        object_id: Annotated[
            str,
            Query(description=api_defs.API_DESC_OBJECT_ID, example="the_id"),
        ]
    ) -> str:
        """
        used with fastapi Depends to provide API parameter

        Args:
            object_id (str, Query): The object ID.

        Returns:
            str: The object ID.
        """
        return APIDependencies._strip_or_none(object_id)

    @staticmethod
    def param_token(
        token: Annotated[
            str,
            Header(description=api_defs.API_DESC_TOKEN, example=api_defs.EXAMPLE_TOKEN),
        ]
    ) -> str:
        """
        used with fastapi Depends to provide API parameter

        Args:
            token (str, Header): The token.

        Returns:
            str: The token.
        """
        return APIDependencies._strip_or_none(token)

    @staticmethod
    def param_glyph_id_optional(
        glyph_id: Annotated[
            Optional[str],
            Query(
                description=api_defs.API_DESC_GLYPH_ID,
                example=api_defs.EXAMPLE_GLYPH_ID,
            ),
        ] = None
    ) -> str:
        """
        used with fastapi Depends to provide API parameter

        Args:
            glyph_id (str, optional, Query): The glyph ID. Defaults to None

        Returns:
            str: The glyph ID.
        """
        return APIDependencies._strip_or_none(glyph_id)

    @staticmethod
    def param_user_id(
        user_id: Annotated[
            str,
            Query(description="the user id.", example="admin"),
        ]
    ) -> str:
        """
        used with fastapi Depends to provide API parameter

        Args:
            user_id (str, Query): The user ID.

        Returns:
            str: The user ID.
        """
        return APIDependencies._strip_or_none(user_id)

    @staticmethod
    def param_user_id_optional(
        user_id: Annotated[
            Optional[str],
            Query(description="the user id.", example="admin"),
        ] = None
    ) -> str:
        """
        used with fastapi Depends to provide API parameter

        Args:
            user_id (str, optional, Query): The user ID. Defaults to None

        Returns:
            str: The user ID.
        """
        return APIDependencies._strip_or_none(user_id)

    @staticmethod
    def param_source_id(
        source_id: Annotated[
            str,
            Query(
                description=api_defs.API_DESC_SOURCE_ID,
                example=api_defs.EXAMPLE_SOURCE_ID,
            ),
        ]
    ) -> str:
        """
        used with fastapi Depends to provide API parameter

        Args:
            source_id (str, Query): The source ID.

        Returns:
            str: The source ID.
        """
        return APIDependencies._strip_or_none(source_id)

    @staticmethod
    def param_source_id_optional(
        source: Annotated[
            Optional[str],
            Query(
                description=api_defs.API_DESC_SOURCE_ID,
                example=api_defs.EXAMPLE_SOURCE_ID,
            ),
        ] = None
    ) -> str:
        """
        used with fastapi Depends to provide API parameter

        Args:
            source (str, optional, Query): The source. Defaults to None.

        Returns:
            str: The source.
        """
        return APIDependencies._strip_or_none(source)

    def param_index(
        index: Annotated[
            str,
            Query(description=api_defs.API_DESC_INDEX, example=api_defs.EXAMPLE_INDEX),
        ]
    ) -> int:
        """
        used with fastapi Depends to provide API parameter

        Args:
            index (str, Query): The opensearch index.

        Returns:
            int: The index.
        """
        return APIDependencies._strip_or_none(index)

    @staticmethod
    def param_operation_id(
        operation_id: Annotated[
            str,
            Query(
                description=api_defs.API_DESC_OPERATION_ID,
                example=api_defs.EXAMPLE_OPERATION_ID,
            ),
        ]
    ) -> str:
        """
        used with fastapi Depends to provide API parameter

        Args:
            operation_id (str, Query): The operation ID.

        Returns:
            str: The operation ID.
        """
        return APIDependencies._strip_or_none(operation_id)

    @staticmethod
    def param_context_id(
        context_id: Annotated[
            str,
            Query(
                description=api_defs.API_DESC_CONTEXT_ID,
                example=api_defs.EXAMPLE_CONTEXT_ID,
            ),
        ]
    ) -> str:
        """
        used with fastapi Depends to provide API parameter

        Args:
            context_id (str, Query): The context ID.

        Returns:
            str: The context ID.
        """
        return APIDependencies._strip_or_none(context_id)

    @staticmethod
    def param_plugin_optional(
        plugin: Annotated[
            Optional[str],
            Query(
                description=api_defs.API_DESC_PLUGIN, example=api_defs.EXAMPLE_PLUGIN
            ),
        ] = None
    ) -> str:
        """
        used with fastapi Depends to provide API parameter

        Args:
            plugin (str, optional, Query): The plugin. Defaults to None.

        Returns:
            str: The plugin.
        """
        return APIDependencies._strip_or_none(plugin)

    @staticmethod
    def param_plugin(
        plugin: Annotated[
            str,
            Query(
                description=api_defs.API_DESC_PLUGIN, example=api_defs.EXAMPLE_PLUGIN
            ),
        ]
    ) -> str:
        """
        used with fastapi Depends to provide API parameter

        Args:
            plugin (str, Query): The plugin.

        Returns:
            str: The plugin.
        """
        return APIDependencies._strip_or_none(plugin)

    @staticmethod
    def param_ws_id(
        ws_id: Annotated[
            str,
            Query(description=api_defs.API_DESC_WS_ID, example=api_defs.EXAMPLE_WS_ID),
        ]
    ) -> str:
        """
        used with fastapi Depends to provide API parameter

        Args:
            ws_id (str, Query): The WS ID.

        Returns:
            str: The WS ID.
        """
        return APIDependencies._strip_or_none(ws_id)

    def param_ingestion_flt_optional(
        flt: Annotated[
            Optional[GulpIngestionFilter],
            Body(
                description=api_defs.API_DESC_INGESTION_FILTER,
            ),
        ] = None
    ) -> GulpIngestionFilter:
        """
        used with fastapi Depends to provide API parameter

        Args:
            flt (GulpIngestionFilter, optional, Body): The ingestion filter. Defaults to empty(no) filter.

        Returns:
            GulpIngestionFilter: The ingestion filter.
        """
        return flt or GulpIngestionFilter()

    def param_plugin_params_optional(
        plugin_params: Annotated[
            Optional[GulpPluginParameters],
            Body(
                description=api_defs.API_DESC_PLUGIN_PARAMETERS,
            ),
        ] = None
    ) -> GulpPluginParameters:
        """
        used with fastapi Depends to provide API parameter

        Args:
            plugin_params (GulpPluginParameters, optional, Body): The plugin parameters. Defaults to default parameters.

        Returns:
            GulpPluginParameters: The plugin parameters.
        """
        return plugin_params or GulpPluginParameters()

    def param_query_flt_optional(
        flt: Annotated[
            Optional[GulpQueryFilter],
            Body(
                description=api_defs.API_DESC_QUERY_FILTER,
            ),
        ] = None
    ) -> GulpQueryFilter:
        """
        used with fastapi Depends to provide API parameter

        Args:
            flt (GulpQueryFilter, optional, Body): The query filter. Defaults to empty(no) filter.

        Returns:
            GulpQueryFilter: The query filter.
        """
        return flt or GulpQueryFilter()

    def param_collab_flt_optional(
        flt: Annotated[
            Optional[GulpCollabFilter],
            Body(
                description="the collab filter.",
            ),
        ] = None
    ) -> GulpCollabFilter:
        """
        used with fastapi Depends to provide API parameter

        Args:
            flt (GulpCollabFilter, optional, Body): The collab filter. Defaults to empty(no) filter.

        Returns:
            GulpCollabFilter: The collab filter.
        """
        return flt or GulpCollabFilter()

    @staticmethod
    def ensure_req_id(
        req_id: Annotated[
            str,
            Query(
                description=api_defs.API_DESC_REQ_ID, example=api_defs.EXAMPLE_REQ_ID
            ),
        ] = None
    ) -> str:
        """
        Ensures a request ID is set, either generates it.

        Args:
            req_id (str, optional): The request ID. Defaults to None.

        Returns:
            str: The request ID.
        """
        if not req_id:
            return muty.string.generate_unique()
        return req_id


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
            filename = filename.strip("\"'")  # Remove quotes
            filename = filename.strip()  # Remove any remaining whitespace

            if not filename:
                raise ValueError("Empty filename in Content-Disposition header")

            return filename

        MutyLogger.get_instance().debug("headers=%s" % (r.headers))

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
        h = "%s-%s" % (
            muty.crypto.hash_xxh128(f"{operation_id}-{context_id}-{req_id}"),
            filename,
        )
        cache_file_path = muty.file.safe_path_join(cache_dir, h)

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

        result = GulpUploadResponse(
            done=is_complete,
            continue_offset=None if is_complete else current_written_size,
        )
        MutyLogger.get_instance().debug(
            "file_path=%s,\npayload=%s,\nresult=%s"
            % (cache_file_path, json.dumps(payload_dict, indent=2), result)
        )

        return (cache_file_path, payload_dict, result)
