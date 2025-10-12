"""
This module defines the API structures and dependencies for the Gulp REST API.

It includes:
- Response models for API endpoints
- Regex patterns for validation
- API dependency helpers for FastAPI
- Parameter validation and processing functions

The APIDependencies class provides static methods that can be used with FastAPI's
Depends to process and validate common API parameters like tokens, IDs, filters,
and user credentials. These methods ensure consistent parameter handling across
the API endpoints.
"""

from os import name
import re
from typing import Annotated, Optional

from llvmlite.tests.test_ir import flt
import muty.string
from fastapi import Body, Header, Query, UploadFile, File
from pydantic import AfterValidator, BaseModel, ConfigDict, Field

from gulp.api.collab.structs import GulpCollabFilter, GulpUserPermission
from gulp.api.collab.user_group import ADMINISTRATORS_GROUP_ID
from gulp.api.opensearch.filters import GulpIngestionFilter, GulpQueryFilter
from gulp.api.opensearch.structs import GulpQueryParameters
from gulp.config import GulpConfig
from gulp.structs import GulpPluginParameters

TASK_TYPE_INGEST: str = "ingest"
TASK_TYPE_INGEST_RAW: str = "ingest_raw"
TASK_TYPE_QUERY: str = "query"

# 5-16 characters length, only letters, numbers, underscore, dot, @, dash allowed
REGEX_CHECK_USERNAME = "^([a-zA-Z0-9_.@-]).{4,16}$"

# 8-64 characters length, at least one uppercase, one lowercase, one digit, one special char
REGEX_CHECK_PASSWORD = (
    r"^(?=.*[A-Z])"
    r"(?=.*[a-z])"
    r"(?=.*[0-9])"
    r"(?=.*[!@#$%^&*()_+\-])"
    r"[A-Za-z0-9!@#$%^&*()_+\-]{8,64}"
    r"$"
)

# regex for checking email
REGEX_CHECK_EMAIL = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"


class GulpUploadResponse(BaseModel):
    """
    the ingest API may respond with this object to indicate the status of an unfinished upload.
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "done": True,
                    "continue_offset": 0,
                },
            ]
        },
    )
    done: Annotated[
        bool, Field(description="Indicates whether the upload is complete.")
    ] = False
    continue_offset: Annotated[
        int,
        Field(description="The offset of the next chunk to be uploaded, to resume."),
    ] = 0


class APIDependencies:
    """
    A class containing static methods to be used as dependencies in FastAPI API endpoints.
    """

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
        if not value:
            return None

        if not re.match(REGEX_CHECK_PASSWORD, value):
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
        if not value:
            return None

        if not bool(re.match(REGEX_CHECK_EMAIL, value)):
            raise ValueError(f"invalid email format: {value}")
        return value

    @staticmethod
    def param_operation_id(
        operation_id: Annotated[
            str,
            Query(
                description="id of a `GulpOperation` object in the collab database.",
                example="test_operation",
            ),
        ],
    ) -> str:
        return operation_id.lower().strip()

    @staticmethod
    def param_token(
        token: Annotated[
            str,
            Header(
                description="""
an authentication token obtained through `login`.

if `GULP_INTEGRATION_TEST` is set, the following tokens are valid if the corresponding user is logged in:

- `token_admin`: a token with admin permissions.
- `token_editor`: a token with read/edit permissions.
- `token_guest`: a token with just read permission
- `token_ingest`: a token with read/edit/ingest permission.
- `token_power`: a token with read/edit/delete permission.
""",
                example="token_admin",
            ),
        ],
    ) -> str:
        return token.strip()

    @staticmethod
    def param_ws_id(
        ws_id: Annotated[
            str,
            Query(
                description="""
id of the websocket to use during a request.
""",
                example="test_ws",
            ),
        ],
    ) -> str:
        return ws_id.strip()

    @staticmethod
    def param_group_id(
        group_id: Annotated[
            str,
            Query(
                description="id of a usergroup in the collab database.",
                example=ADMINISTRATORS_GROUP_ID,
            ),
        ],
    ) -> str:
        return group_id.strip()

    @staticmethod
    def param_obj_id(
        obj_id: Annotated[
            str,
            Query(description="the object id", example="obj_id"),
        ],
    ) -> str:
        return obj_id.lower().strip()

    @staticmethod
    def _param_user_id(
        user_id: Annotated[
            str,
            Query(
                description="id of an user in the collab database.",
                example="admin",
            ),
        ] = None,
    ) -> str:
        return user_id.lower().strip() if user_id else None

    @staticmethod
    def param_user_id() -> str:
        user_id: str = (APIDependencies._param_user_id(),)
        if not user_id:
            raise ValueError("user_id is required")
        return user_id

    @staticmethod
    def param_user_id_optional() -> str:
        return APIDependencies._param_user_id()

    @staticmethod
    def _param_password(
        password: Annotated[
            str,
            Query(
                description="""
the user password.

- 8-64 characters, at least one uppercase, one lowercase, one number, one special character.
""",
                example="Password1!",
            ),
            AfterValidator(_pwd_regex_validator),
        ] = None,
    ) -> str:
        """
        NOTE: use config `debug_allow_insecure_passwords` to disable validation (not recommended).
        """
        return password.strip() if password else None

    @staticmethod
    def param_password() -> str:
        password: str = APIDependencies._param_password()
        if not password:
            raise ValueError("password is required")
        return password

    @staticmethod
    def param_password_optional() -> str | None:
        return APIDependencies._param_password()

    @staticmethod
    def _param_permission(
        permission: Annotated[
            list[GulpUserPermission],
            Body(
                description="""
one or more user/group permission.

- read: read the object.
- edit: edit the object.
- delete: delete the object.
- ingest: ingest data.
- **admin: every permission.**
""",
                example='["read","edit"]',
            ),
        ] = [],
    ) -> list[GulpUserPermission]:
        return permission

    @staticmethod
    def param_permission() -> list[GulpUserPermission]:
        permission = APIDependencies._param_permission()
        if not permission:
            raise ValueError("permission is required")
        return permission

    @staticmethod
    def param_permission_optional() -> list[GulpUserPermission]:
        return APIDependencies._param_permission()

    @staticmethod
    def _param_email(
        email: Annotated[
            str,
            Query(description="the user email.", example="user@mail.com"),
            AfterValidator(_email_regex_validator),
        ] = None,
    ) -> str:
        return email.strip() if email else None

    @staticmethod
    def param_email() -> str:
        email: str = APIDependencies._param_email()
        if not email:
            raise ValueError("email is required")
        return email

    @staticmethod
    def param_email_optional() -> str:
        return APIDependencies._param_email()

    @staticmethod
    def param_private(
        private: Annotated[
            bool,
            Query(
                description="sets the object as private, so only the *owner* `user_id` and administrators can access it.",
            ),
        ] = False,
    ) -> bool:
        return private

    @staticmethod
    def param_description_optional(
        description: Annotated[
            str,
            Body(
                description="the object description.",
                examples=["this is a description"],
            ),
        ] = None,
    ) -> str:
        return description.strip() if description else None

    @staticmethod
    def ensure_req_id_optional(
        req_id: Annotated[
            str,
            Query(
                description="""
id of a request, will be replicated in the response `req_id`.

- leave empty to autogenerate.
""",
                example="test_req",
            ),
        ] = None,
    ) -> str:
        return req_id.lower().strip() if req_id else muty.string.generate_unique()

    @staticmethod
    def _param_name(
        name: Annotated[
            str,
            Query(description="the object name", example="my object"),
        ] = None,
    ) -> str:
        return name.strip() if name else None

    @staticmethod
    def param_name_optional() -> str:
        return APIDependencies._param_name()

    @staticmethod
    def param_name() -> str:
        name = APIDependencies._param_name()
        if not name:
            raise ValueError("name is required")
        return name

    @staticmethod
    def param_collab_flt_optional(
        flt: Annotated[
            GulpCollabFilter,
            Body(
                description="to filter objects on the collab database.",
            ),
        ] = None,
    ) -> GulpCollabFilter:
        return flt or GulpCollabFilter()

    @staticmethod
    def param_ingestion_flt_optional(
        flt: Annotated[
            GulpIngestionFilter,
            Body(
                description="""
to filter documents by `time_range` during ingestion.
""",
            ),
        ] = None,
    ) -> GulpIngestionFilter:
        return flt or GulpIngestionFilter()

    @staticmethod
    def _param_q_flt(
        flt: Annotated[
            GulpQueryFilter,
            Body(
                description="""
the query filter, to filter for common fields, including:

- `operation_id`, `context_id`, `source_id` to filter for specific objects.
- `event_original` to search into the original event.
- `time_range` to filter by time.
- to filter for custom keys, just add them in `flt` as `key: value` or `key: [values]` for `OR` match.
""",
            ),
        ] = None,
    ) -> GulpQueryFilter:
        return flt or GulpQueryFilter()

    @staticmethod
    def param_q_flt_optional() -> GulpQueryFilter:
        return APIDependencies._param_q_flt()

    @staticmethod
    def param_q_flt() -> GulpQueryFilter:
        flt = APIDependencies._param_q_flt()
        if not flt:
            raise ValueError("filter is required")
        return flt

    @staticmethod
    def param_q_options_optional(
        q_options: Annotated[
            GulpQueryParameters,
            Body(
                description="""
additional parameters for querying, including:

- `limit`, `offset`, `search_after` for pagination.
- `fields` to restrict returned fields.
- `sort` for sorting
""",
            ),
        ] = None,
    ) -> GulpQueryParameters:
        return q_options or GulpQueryParameters()

    @staticmethod
    def param_context_id(
        context_id: Annotated[
            str,
            Query(
                description="""
id of a `GulpContext` object on the collab database.
""",
                example="66d98ed55d92b6b7382ffc77df70eda37a6efaa1",
            ),
        ],
    ) -> str:
        return context_id.lower().strip()

    @staticmethod
    def param_source_id(
        source_id: Annotated[
            str,
            Query(
                description="""
id of a `GulpSource` object on the collab database.
""",
                example="fa144510fd16cf5ffbaeec79d68b593f3ba7e7e0",
            ),
        ],
    ) -> str:
        return source_id.lower().strip()

    @staticmethod
    def param_plugin(
        plugin: Annotated[
            str,
            Query(
                description="internal name (filename without extension) of the plugin to use.",
                example="win_evtx",
            ),
        ],
    ) -> str:
        return plugin.strip()

    @staticmethod
    def param_plugin_params(
        plugin_params: Annotated[
            GulpPluginParameters,
            Body(
                description="""
to customize `mapping` and specific `plugin` parameters.
"""
            ),
        ] = None,
    ) -> GulpPluginParameters:
        return plugin_params or GulpPluginParameters()

    @staticmethod
    def _param_tags(
        tags: Annotated[
            list[str],
            Body(
                description="tags to be assigned to the object.",
                example='["tag1","tag2"]',
            ),
        ] = [],
    ) -> list[str]:
        if tags:
            # strip each tag, remove empty tags
            tags = [tag.strip().lower() for tag in tags if tag and tag.strip()]

        return tags or []

    @staticmethod
    def param_tags() -> list[str]:
        tags = APIDependencies._param_tags()
        if not tags:
            raise ValueError("tags is required")
        return tags

    @staticmethod
    def param_tags_optional() -> list[str]:
        return APIDependencies._param_tags()

    @staticmethod
    def param_glyph_id_optional(
        glyph_id: Annotated[
            str,
            Query(
                description="id of a `glyph` in the collab database.",
            ),
        ] = None,
    ) -> str:
        return glyph_id.strip() if glyph_id else None

    @staticmethod
    def param_color_optional(
        color: Annotated[
            str,
            Query(
                description="the color in #rrggbb or css-name format.",
                example="#ff0000",
            ),
        ] = None,
    ) -> str:
        """
        color to set on the object

        Args:
            color (str, Query): The color. Defaults to None.

        Returns:
            str: The color.
        """
        return color.strip() if color else None
