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

import re
from typing import Annotated, Optional

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
    def _strip_or_none(value: Optional[str], lower: bool = True) -> Optional[str]:
        """
        Strips a string, or returns None if the string is empty.

        Args:
            value (Optional[str]): The string to strip.
            lower (bool, optional): Whether to convert the string to lowercase. Defaults to True.

        Returns:
            Optional[str]: The stripped string, or None.
        """
        if value:
            v = value.strip()
            if lower:
                v = v.lower()
        else:
            v = None
        return v

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
        if value is None:
            return None

        if not bool(re.match(REGEX_CHECK_EMAIL, value)):
            raise ValueError(f"invalid email format: {value}")
        return value

    @staticmethod
    def param_obj_id(
        obj_id: Annotated[
            str,
            Query(description="the object id", example="obj_id"),
        ],
    ) -> str:
        """
        an object id, stripped and lowercased.

        Args:
            obj_id (str, Query): The object ID.

        Returns:
            str: The object ID.
        """
        return obj_id.lower().strip()
        # return APIDependencies._strip_or_none(obj_id)

    @staticmethod
    def param_private(
        private: Annotated[
            bool,
            Query(
                description="sets the object as private, so only the *owner* `user_id` and administrators can access it.",
            ),
        ] = False,
    ) -> bool:
        """
        to set the object as private.

        Args:
            private (bool, Body): Whether the object is private. Defaults to False (public).

        Returns:
            bool: The private flag.
        """
        return private

    @staticmethod
    def param_description(
        description: Annotated[
            str,
            Body(
                description="the object description.",
                examples=["this is a description"],
            ),
        ] = None,
    ) -> str:
        """
        to set the object description.

        Args:
            description (str, Body): The description. Defaults to None.

        Returns:
            str: The description.
        """
        return description.strip() if description else None

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
        """
        the authentication token.

        Args:
            token (str, Header): The token.

        Returns:
            str: The token.
        """
        return token.strip()

    @staticmethod
    def ensure_req_id(
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
        """
        Ensures a request ID is set, either generates it.

        Args:
            req_id (str, optional): The request ID. Defaults to None.

        Returns:
            str: The request ID.
        """
        return req_id.lower().strip() if req_id else muty.string.generate_unique()

    @staticmethod
    def param_name(
        name: Annotated[
            str,
            Query(description="the object name", example="my object"),
        ] = None,
    ) -> str:
        """
        to set the object name

        Args:
            name (str, Query): the object name

        Returns:
            str: The name.
        """
        return name.strip() if name else None

    @staticmethod
    def param_ws_id(
        ws_id: Annotated[
            str,
            Query(
                description="""
id of the websocket to send progress and results during the processing of a request.
""",
                example="test_ws",
            ),
        ],
    ) -> str:
        """
        the websocket id.

        Args:
            ws_id (str, Query): The WS ID.

        Returns:
            str: The WS ID.
        """
        return ws_id.strip()

    @staticmethod
    def param_collab_flt(
        flt: Annotated[
            GulpCollabFilter,
            Body(
                description="the collab filter.",
            ),
        ] = None,
    ) -> GulpCollabFilter:
        """
        to filter collab objects.

        Args:
            flt (GulpCollabFilter, Body): The collab filter. Defaults to empty filter.

        Returns:
            GulpCollabFilter: The collab filter.
        """
        return flt or GulpCollabFilter()

    @staticmethod
    def param_q_flt(
        flt: Annotated[
            GulpQueryFilter,
            Body(
                description="""
the query filter, to filter for common fields, including:

- `operation_id`, `context_id`, `source_id` to filter for specific objects.
- `event_original` to search into the original event.
- `time_range` to filter by time.
- to filter for custom keys, just add them in `flt` as `key: value` or `key: [values]` for `OR` match.
"""
            ),
        ] = None,
    ) -> GulpQueryFilter:
        """
        to filter documents during query.

        Args:
            flt (GulpQueryFilter, Body): The query filter. Defaults to empty(no) filter.

        Returns:
            GulpQueryFilter: The query filter.
        """
        return flt or GulpQueryFilter()

    @staticmethod
    def param_q_options(
        q_options: Annotated[
            GulpQueryParameters,
            Body(
                description="""
additional parameters for querying, including:

- `limit`, `offset`, `search_after` for pagination.
- `fields` to restrict returned fields.
- `sort` for sorting
"""
            ),
        ] = None,
    ) -> dict:
        """
        to customize query

        Args:
            options (GulpQueryParameters, Body): The query options. Defaults to empty(no) options.

        Returns:
            GulpQueryParameters: The query options.
        """
        return q_options or GulpQueryParameters()

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
        """
        used with fastapi Depends to provide API parameter

        Args:
            operation_id (str, Query): The operation ID.

        Returns:
            str: The operation ID.
        """
        return operation_id.strip()

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
        """
        used with fastapi Depends to provide API parameter

        Args:
            context_id (str, Query): The context ID.

        Returns:
            str: The context ID.
        """
        return context_id.strip()

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
        """
        used with fastapi Depends to provide API parameter


        Args:
            source_id (str, Query): The source ID.

        Returns:
            str: The source ID.
        """
        return source_id.strip()

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
        """
        used with fastapi Depends to provide API parameter

        Args:
            plugin (str, Query): The plugin.

        Returns:
            str: The plugin.
        """
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
        """
        used with fastapi Depends to provide API parameter

        Args:
            plugin_params (GulpPluginParameters, Body): The plugin parameters

        Returns:
            GulpPluginParameters: The plugin parameters or None if empty
        """
        return plugin_params or GulpPluginParameters()

    ############################

    _DESC_OBJ_DISPLAY_NAME = "the object display name."
    _EXAMPLE_OBJ_DISPLAY_NAME = "object name"

    @staticmethod
    def param_display_name_optional(
        name: Annotated[
            Optional[str],
            Query(
                description=_DESC_OBJ_DISPLAY_NAME, example=_EXAMPLE_OBJ_DISPLAY_NAME
            ),
        ] = None,
    ) -> str:
        """
        used with fastapi Depends to provide API parameter

        Args:
            name (str, optional, Query): The display name. Defaults to None.

        Returns:
            str: The name.
        """
        return APIDependencies._strip_or_none(name, lower=False)

    @staticmethod
    def param_tags_optional(
        tags: Annotated[
            Optional[list[str]],
            Body(
                description="tags to be assigned to the object.",
                example='["tag1","tag2"]',
            ),
        ] = None,
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
            tags = [tag.strip().lower() for tag in tags if tag and tag.strip()]

        return tags or []

    @staticmethod
    def param_color_optional(
        color: Annotated[
            Optional[str],
            Query(
                description="the color in #rrggbb or css-name format.", example="yellow"
            ),
        ] = None,
    ) -> str:
        """
        used with fastapi Depends to provide API parameter

        Args:
            color (str, optional, Query): The color. Defaults to None.

        Returns:
            str: The color.
        """
        return APIDependencies._strip_or_none(color)

    _DESC_PASSWORD = """
the user password.

- 8-64 characters, at least one uppercase, one lowercase, one number, one special character.
"""
    _EXAMPLE_PASSWORD = "Password1!"

    @staticmethod
    def param_password(
        password: Annotated[
            str,
            Query(description=_DESC_PASSWORD, example=_EXAMPLE_PASSWORD),
            AfterValidator(_pwd_regex_validator),
        ],
    ) -> str:
        """
        used with fastapi Depends to provide API parameter

        Args:
            password (str, Query): The password.

        Returns:
            str: The password.
        """
        return APIDependencies._strip_or_none(password, lower=False)

    @staticmethod
    def param_password_optional(
        password: Annotated[
            Optional[str],
            Query(description=_DESC_PASSWORD, example=_EXAMPLE_PASSWORD),
            AfterValidator(_pwd_regex_validator),
        ] = None,
    ) -> str:
        """
        used with fastapi Depends to provide API parameter

        Args:
            password (str, optional, Query): The password. Defaults to None.

        Returns:
            str: The password.
        """
        return APIDependencies._strip_or_none(password, lower=False)

    _DESC_PERMISSION = """
one or more user permission.

- read: read the object.
- edit: edit the object.
- delete: delete the object.
- ingest: ingest data.
- **admin: every permission.**
"""
    _EXAMPLE_PERMISSION = '["read","edit"]'

    @staticmethod
    def param_permission_optional(
        permission: Annotated[
            Optional[list[GulpUserPermission]],
            Body(description=_DESC_PERMISSION, example=_EXAMPLE_PERMISSION),
        ] = None,
    ) -> list[GulpUserPermission]:
        """
        used with fastapi Depends to provide API parameter

        Args:
            permission (list[GulpUserPermission], optional, Body): The permission. Defaults to None.

        Returns:
            list[GulpUserPermission]: The permission.
        """
        return permission

    @staticmethod
    def param_permission(
        permission: Annotated[
            list[GulpUserPermission],
            Body(description=_DESC_PERMISSION, example=_EXAMPLE_PERMISSION),
        ] = None,
    ) -> list[GulpUserPermission]:
        """
        used with fastapi Depends to provide API parameter

        Args:
            permission (list[GulpUserPermission], optional, Body): The permission. Defaults to None.

        Returns:
            list[GulpUserPermission]: The permission.
        """
        return permission

    @staticmethod
    def param_email_optional(
        email: Annotated[
            Optional[str],
            Query(description="the user email.", example="user@mail.com"),
            AfterValidator(_email_regex_validator),
        ] = None,
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
    def param_glyph_id_optional(
        glyph_id: Annotated[
            Optional[str],
            Query(
                description="id of a `glyph` in the collab database.",
            ),
        ] = None,
    ) -> str:
        """
        used with fastapi Depends to provide API parameter

        Args:
            glyph_id (str, optional, Query): The glyph ID. Defaults to None

        Returns:
            str: The glyph ID.
        """
        return APIDependencies._strip_or_none(glyph_id)

    _DESC_OBJ_ID = "id of an object in the collab database."
    _EXAMPLE_OBJ_ID = "obj_id"

    @staticmethod
    def param_group_id(
        group_id: Annotated[
            str,
            Query(description="the usergroup ID", example=ADMINISTRATORS_GROUP_ID),
        ],
    ) -> str:
        """
        used with fastapi Depends to provide API parameter

        Args:
            group_id (str, Query): The group ID.

        Returns:
            str: The object ID.
        """
        return APIDependencies._strip_or_none(group_id)

    @staticmethod
    def param_object_id_optional(
        obj_id: Annotated[
            Optional[str],
            Query(description=_DESC_OBJ_ID, example=_EXAMPLE_OBJ_ID),
        ] = None,
    ) -> str:
        """
        used with fastapi Depends to provide API parameter

        Args:
            obj_id (str, optional, Query): The object ID. Defaults to None.

        Returns:
            str: The object ID.
        """
        return APIDependencies._strip_or_none(obj_id)

    @staticmethod
    def param_user_id(
        user_id: Annotated[
            str,
            Query(
                description="id of an user in the collab database.",
                example="admin",
            ),
        ],
    ) -> str:
        """
        used with fastapi Depends to provide API parameter

        Args:
            user_id (str, Query): The user ID.

        Returns:
            str: The user ID.
        """
        return APIDependencies._strip_or_none(user_id)

    _DESC_INDEX = "the gulp's opensearch index/datastream name."
    _EXAMPLE_INDEX = "test_operation"

    @staticmethod
    def param_index(
        index: Annotated[
            str,
            Query(description=_DESC_INDEX, example=_EXAMPLE_INDEX),
        ],
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
    def param_index_optional(
        index: Annotated[
            Optional[str],
            Query(description=_DESC_INDEX, example=_EXAMPLE_INDEX),
        ] = None,
    ) -> int:
        """
        used with fastapi Depends to provide API parameter

        Args:
            index (str, optional, Query): The opensearch index. Defaults to None.

        Returns:
            int: The index.
        """
        return APIDependencies._strip_or_none(index)

    _DESC_PLUGIN = """
the plugin to process the request with.

it must be the `bare filename` of the plugin (`.py`,`.pyc` extension may be omitted).
"""
    _EXAMPLE_PLUGIN = "win_evtx"

    @staticmethod
    def param_plugin_optional(
        plugin: Annotated[
            Optional[str],
            Query(description=_DESC_PLUGIN, example=_EXAMPLE_PLUGIN),
        ] = None,
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
    def param_ingestion_flt_optional(
        flt: Annotated[
            Optional[GulpIngestionFilter],
            Body(
                description="""
to filter documents by `time_range` during `ingestion`.
"""
            ),
        ] = None,
    ) -> GulpIngestionFilter:
        """
        used with fastapi Depends to provide API parameter

        Args:
            flt (GulpIngestionFilter, optional, Body): The ingestion filter. Defaults to empty(no) filter.

        Returns:
            GulpIngestionFilter: The ingestion filter.
        """
        return flt or GulpIngestionFilter()

    @staticmethod
    def param_plugin_params_optional(
        plugin_params: Annotated[
            Optional[GulpPluginParameters],
            Body(
                description="""
to customize `mapping` and specific `plugin` parameters.
"""
            ),
        ] = None,
    ) -> GulpPluginParameters:
        """
        used with fastapi Depends to provide API parameter

        Args:
            plugin_params (GulpPluginParameters, optional, Body): The plugin parameters. Defaults to default parameters.

        Returns:
            GulpPluginParameters: The plugin parameters or None if empty
        """
        return plugin_params or GulpPluginParameters()
