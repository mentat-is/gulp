"""
Module that defines the GulpUser class, representing users in the Gulp collaboration system.

This module provides functionality for user management, including:
- User creation, update, and authentication
- Permission management and access control
- Session management for login/logout operations
- Group membership and hierarchical permissions

GulpUser is a foundational class in the collab system, allowing for fine-grained
access control to resources based on user permissions, group memberships,
and ownership relationships.

"""

from typing import TYPE_CHECKING, Any, Optional, override

import muty.crypto
import muty.time
from muty.log import MutyLogger
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import ARRAY, BIGINT, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.mutable import MutableDict, MutableList
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.types import Enum as SQLEnum

from gulp.api.collab.structs import (
    COLLABTYPE_USER,
    GulpCollabBase,
    GulpUserPermission,
    MissingPermission,
    T,
    WrongUsernameOrPassword,
)
from gulp.api.collab.user_group import GulpUserGroupAssociation
from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch.filters import GulpQueryFilter
from gulp.api.opensearch.query import GulpQueryParameters
from gulp.api.ws_api import (
    WSDATA_USER_LOGIN,
    WSDATA_USER_LOGOUT,
    GulpUserLoginLogoutPacket,
    GulpWsSharedQueue,
)
from gulp.config import GulpConfig
from gulp.plugin import GulpUserInfoInternalEvent
from gulp.structs import GulpPluginParameters
from muty.pydantic import autogenerate_model_example_by_class

if TYPE_CHECKING:
    from gulp.api.collab.user_group import GulpUserGroup
    from gulp.api.collab.user_session import GulpUserSession


class GulpUserDataQueryHistoryEntry(BaseModel):
    """
    Represents a single entry in the user's query history.
    """

    query: Any = Field(
        ...,
        description="The query that was performed, may be a string or a dict depending on the query type and target.",
    )
    external: bool = Field(False, description="if the query is an external query.")
    query_options: Optional[GulpQueryParameters] = Field(
        default_factory=list, description="Additional options for the query."
    )
    flt: Optional[GulpQueryFilter] = Field(
        default_factory=list, description="Filter applied to the query."
    )
    plugin: Optional[str] = Field(None, description="Only set for external queries.")
    plugin_params: Optional[GulpPluginParameters] = Field(
        None,
        description="Only set for external queries, the parameters for the external query plugin.",
    )
    sigma: Optional[str] = Field(
        None, description="Sigma rule YML if the query is converted from a sigma rule."
    )
    timestamp_msec: int = Field(
        ..., description="Timestamp of the query in milliseconds from the unix epoch."
    )

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "query": {"query": {"match_all": {}}},
                    "external": False,
                    "query_options": autogenerate_model_example_by_class(
                        GulpQueryParameters
                    ),
                    "flt": autogenerate_model_example_by_class(GulpQueryFilter),
                    "plugin": None,
                    "plugin_params": autogenerate_model_example_by_class(
                        GulpPluginParameters
                    ),
                    "sigma": None,
                    "timestamp_msec": muty.time.now_msec(),
                }
            ]
        }
    )


class GulpUser(GulpCollabBase, type=COLLABTYPE_USER):
    """
    Represents a user in the system.
    """

    pwd_hash: Mapped[str] = mapped_column(
        String, doc="The hashed password of the user."
    )
    email: Mapped[Optional[str]] = mapped_column(
        String, doc="The email of the user.", unique=True, default=None
    )
    session: Mapped[Optional["GulpUserSession"]] = relationship(
        "GulpUserSession",
        back_populates="user",
        cascade="all,delete-orphan",
        uselist=False,
        lazy="joined",
        default=None,
        foreign_keys="[GulpUserSession.session_user_id]",
    )
    groups: Mapped[list["GulpUserGroup"]] = relationship(
        "GulpUserGroup",
        secondary=GulpUserGroupAssociation.table,
        back_populates="users",
        lazy="selectin",
        uselist=True,
        default_factory=list,
    )
    permission: Mapped[list[GulpUserPermission]] = mapped_column(
        MutableList.as_mutable(ARRAY(SQLEnum(GulpUserPermission))),
        default_factory=lambda: [GulpUserPermission.READ],
        doc="One or more permissions of the user.",
    )
    time_last_login: Mapped[Optional[int]] = mapped_column(
        BIGINT,
        default=0,
        doc="The time of the last login, in milliseconds from the unix epoch.",
    )

    user_data: Mapped[Optional[dict]] = mapped_column(
        MutableDict.as_mutable(JSONB), default_factory=dict, doc="Arbitrary user data."
    )

    @override
    @classmethod
    def example(cls) -> dict:
        from gulp.api.collab.user_group import GulpUserGroup
        from gulp.api.collab.user_session import GulpUserSession

        d = super().example()
        d.update(
            {
                "pwd_hash": "hashed_password",
                "groups": [GulpUserGroup.example()],
                "permission": ["READ"],
                "email": "user@mail.com",
                "time_last_login": 1234567890,
                "session": GulpUserSession.example(),
                "user_data": {"key": "value", "key2": 1234},
            }
        )
        return d

    @staticmethod
    async def add_query_history_entry(
        user_id: str,
        q: Any,
        q_options: GulpQueryParameters = None,
        flt: GulpQueryFilter = None,
        sigma: str = None,
        external: bool = False,
        plugin: str = None,
        plugin_params: GulpPluginParameters = None,
    ) -> None:
        """
        Adds a query history entry for the user.

        Args:
            user_id (str): The ID of the user.
            q (Any): the query to perform
            q_options (GulpQueryParameters, optional): Additional query options. Defaults to None.
            flt (GulpQueryFilter, optional): Filter applied to the query. Defaults to None.
            external (bool, optional): Whether the query is external. Defaults to False.
            plugin (str, optional): The plugin used for the query. Defaults to None.
            plugin_params (GulpPluginParameters, optional): Parameters for the plugin used in the query. Defaults to None.
            sigma (str, optional): Sigma rule applied to the query. Defaults to None.
        """
        async with GulpCollab.get_instance().session() as sess:
            u: GulpUser = await GulpUser.get_by_id(sess, user_id)
            await u._add_query_history_entry(
                sess, q, q_options, flt, sigma, external, plugin, plugin_params
            )

    async def _add_query_history_entry(
        self,
        sess: AsyncSession,
        q: Any,
        q_options: GulpQueryParameters = None,
        flt: GulpQueryFilter = None,
        sigma: str = None,
        external: bool = False,
        plugin: str = None,
        plugin_params: GulpPluginParameters = None,
    ) -> None:
        """
        Adds a query history entry for the user.

        Args:
            sess (AsyncSession): The database session, will be committed after the entry is added.
            q (Any): The query to perform
            q_options (GulpQueryParameters, optional): Additional query options. Defaults to None.
            flt (GulpQueryFilter, optional): Filter applied to the query. Defaults to None.
            external (bool, optional): Whether the query is external. Defaults to False.
            plugin (str, optional): The plugin used for the query. Defaults to None.
            plugin_params (GulpPluginParameters, optional): Parameters for the plugin used in the query. Defaults to None.
            sigma (str, optional): Sigma rule applied to the query. Defaults to None.
        """
        if self.user_data is None:
            self.user_data = {}

        if "query_history" not in self.user_data:
            self.user_data["query_history"] = []

        q_history = self.user_data["query_history"]
        entry: GulpUserDataQueryHistoryEntry = GulpUserDataQueryHistoryEntry(
            query=q,
            external=external,
            query_options=q_options,
            flt=flt,
            plugin=plugin,
            plugin_params=plugin_params,
            sigma=sigma,
            timestamp_msec=muty.time.now_msec(),
        )
        q_history.append(entry.model_dump(exclude_none=True))

        # trim if the history is too long
        MutyLogger.get_instance().debug(
            "current user query histor size=%d" % (len(q_history))
        )
        max_history_size: int = GulpConfig.get_instance().query_history_max_size()
        if len(q_history) > max_history_size:
            # keep only the last `max_history_size` entries
            MutyLogger.get_instance().warning(
                "trimming query history for user %s, size=%d, max_size=%d"
                % (self.id, len(q_history), max_history_size)
            )
            q_history = q_history[-max_history_size:]

        self.user_data["query_history"] = q_history
        await sess.commit()

    async def _get_query_history(self) -> list[dict]:
        """
        Retrieves the query history for the user.

        Args:
            sess (AsyncSession): The database session.

        Returns:
            list[dict]: The query history entries.
        """
        if "query_history" not in self.user_data:
            return []

        return self.user_data["query_history"]

    @staticmethod
    async def get_query_history(user_id: str) -> list[dict]:
        """
        Retrieves the query history for the user.

        Args:
            user_id (str): The ID of the user.

        Returns:
            list[dict]: The query history entries.
        """
        async with GulpCollab.get_instance().session() as sess:
            u: GulpUser = await GulpUser.get_by_id(sess, user_id)
            return await u._get_query_history()

    @classmethod
    @override
    async def create(cls, *args, **kwargs) -> T:
        raise TypeError("use 'create_user' method to create a user")

    @classmethod
    async def create_user(
        cls,
        sess: AsyncSession,
        user_id: str,
        password: str,
        permission: list[GulpUserPermission] = None,
        email: str = None,
        glyph_id: str = None,
    ) -> T:
        """
        Create a new user object on the collab database (can only be called by an admin).

        Args:
            sess (AsyncSession): The database session.
            user_id (str): The ID of the user to create.
            password (str): The password of the user to create.
            permission (list[GulpUserPermission], optional): permissions to be assigned to the user being created. Defaults to [GulpUserPermission.READ] if not specified.
            email (str, optional): The email of the user to create. Defaults to None.
            glyph_id (str, optional): The glyph ID of the user to create. Defaults to None.

        Returns:
            The created user object.
        """
        if not permission:
            # default permission
            permission = [GulpUserPermission.READ]

        if GulpUserPermission.READ not in permission:
            # ensure that all users have read permission
            permission.append(GulpUserPermission.READ)

        # set user_id to username (user owns itself)
        u: GulpUser = await GulpUser.create_internal(
            sess,
            obj_id=user_id,
            user_id=user_id,
            glyph_id=glyph_id,
            pwd_hash=muty.crypto.hash_sha256(password) if password else "-",
            permission=permission,
            email=email,
            user_data={},
        )

        # # if the default administrators group exists, and the user is administrator, add
        # # the user to the default administrators group
        # if u.is_admin():
        #     from gulp.api.collab.user_group import (
        #         ADMINISTRATORS_GROUP_ID,
        #         GulpUserGroup,
        #     )

        #     g: GulpUserGroup = await GulpUserGroup.get_by_id(
        #         sess, ADMINISTRATORS_GROUP_ID
        #     )
        #     MutyLogger.get_instance().debug(
        #         "adding user %s to the 'administrators' group" % (u.id)
        #     )
        #     await g.add_user(sess, u.id)
        return u

    def is_admin(self) -> bool:
        """
        Check if the user has admin permission (or is in an admin group).

        Returns:
            bool: True if the user has admin permission, False otherwise.
        """
        admin = GulpUserPermission.ADMIN in self.permission
        if admin:
            return admin

        if self.groups:
            # also check if the user is in an admin group
            for group in self.groups:
                if group.is_admin():
                    return True

        return False

    def logged_in(self) -> bool:
        """
        check if the user is logged in

        Returns:
            bool: True if the user has an active session (logged in)
        """
        return self.session is not None

    @classmethod
    async def login(
        cls,
        sess: AsyncSession,
        user_id: str,
        password: str,
        ws_id: str,
        req_id: str,
        skip_password_check: bool = False,
        user_ip: str = None,
    ) -> "GulpUserSession":
        """
        Asynchronously logs in a user and creates a session (=obtain token).
        Args:
            sess (AsyncSession): The session to use.
            user (str): The username of the user to log in.
            password (str): The password of the user to log in.
            ws_id (str): The websocket ID to send WSDATA_USER_LOGIN notification.
            req_id (str): The request ID.
            skip_password_check (bool, optional): Whether to skip the password check, internal usage only. Defaults to False.
            user_ip (str, optional): The IP address of the user, for logging purposes. Defaults to None.
        Returns:
            GulpUserSession: The created session object.
        """
        from gulp.api.collab.user_session import GulpUserSession

        # ensure atomicity of login
        try:
            await GulpUser.acquire_advisory_lock(sess, user_id)
            u: GulpUser = await GulpUser.get_by_id(sess, user_id)

            if not skip_password_check:
                # check password
                if u.pwd_hash != muty.crypto.hash_sha256(password):
                    raise WrongUsernameOrPassword(
                        "wrong password for user_id=%s" % (user_id)
                    )

            if u.session:
                # session already exist, update expiration time
                MutyLogger.get_instance().warning(
                    "user %s was already logged in, resetting and renewing token..."
                    % (user_id)
                )

                # update expiration
                await u.session.update_expiration_time(
                    sess, is_admin=u.is_admin(), update_id=True
                )
                await sess.commit()
                return u.session

            # create new session
            p = GulpUserLoginLogoutPacket(user_id=u.id, login=True, ip=user_ip)
            time_expire = GulpConfig.get_instance().token_expiration_time(u.is_admin())
            if GulpConfig.get_instance().is_integration_test():
                # for integration tests, this api will return a fixed token based on the user_id
                # (the user must anyway log in first)
                token_id: str = "token_" + user_id
                MutyLogger.get_instance().warning(
                    "using fixed token %s for integration test" % (token_id)
                )
            else:
                # autogenerated
                token_id: str = None

            # pylint: disable=protected-access
            new_session: GulpUserSession = await GulpUserSession.create_internal(
                sess,
                u.id,
                obj_id=token_id,
                ws_id=ws_id,
                ws_data_type=WSDATA_USER_LOGIN,
                ws_data=p.model_dump(),
                req_id=req_id,
                time_expire=time_expire,
                session_user_id=u.id,
            )

            # update user with new session and write the new session object itself
            u.session = new_session
            u.time_last_login = muty.time.now_msec()
            MutyLogger.get_instance().info(
                "user %s logged in, token=%s, expire=%d, admin=%r"
                % (u.id, new_session.id, new_session.time_expire, u.is_admin())
            )
            await sess.commit()

            # also broadcast to registered plugins
            from gulp.plugin import GulpInternalEventsManager

            await GulpInternalEventsManager.get_instance().broadcast_event(
                GulpInternalEventsManager.EVENT_LOGIN,
                GulpUserInfoInternalEvent(user_id=u.id, ip=user_ip).model_dump(),
            )

            return new_session
        except Exception as e:
            await sess.rollback()
            raise e

    @classmethod
    async def logout(
        cls,
        sess: AsyncSession,
        s: "GulpUserSession",
        ws_id: str,
        req_id: str,
        user_ip: str = None,
    ) -> None:
        """
        Logs out the specified user by deleting the session.

        Args:
            sess (AsyncSession): The session to use.
            s: the GulpUserSession to log out
            ws_id (str): The websocket ID to send WSDATA_USER_LOGOUT notification.
            req_id (str): The request ID.
            user_ip (str, optional): The IP address of the user, for logging purposes. Defaults to None.
        Returns:
            None
        """
        MutyLogger.get_instance().info(
            "logging out token=%s, user=%s" % (s.id, s.user.id)
        )
        p = GulpUserLoginLogoutPacket(user_id=s.user.id, login=False)
        await s.delete(
            sess=sess,
            user_id=s.user.id,
            ws_id=ws_id,
            req_id=req_id,
            ws_data_type=WSDATA_USER_LOGOUT,
            ws_data=p.model_dump(),
        )

        # also broadcast to registered plugins
        from gulp.plugin import GulpInternalEventsManager

        await GulpInternalEventsManager.get_instance().broadcast_event(
            GulpInternalEventsManager.EVENT_LOGOUT,
            GulpUserInfoInternalEvent(user_id=s.user.id, ip=user_ip).model_dump(),
        )

    def has_permission(self, permission: list[GulpUserPermission]) -> bool:
        """
        Check if the user has the specified permission

        Args:
            permission (list[GulpUserPermission]): The permission(s) to check.

        Returns:
            bool: True if the user has the specified permissions, False otherwise.
        """
        if self.is_admin():
            return True

        # check if user has all the required permissions using a generator expression
        has_permission = all(p in self.permission for p in permission)
        if has_permission:
            return True
        if self.groups:
            for group in self.groups:
                if group.has_permission(permission):
                    return True
        return False

    def check_object_access(
        self,
        obj: GulpCollabBase,
        enforce_owner: bool = False,
        throw_on_no_permission: bool = False,
    ) -> bool:
        """
        Check if the user has READ permission to access the specified object.

        the user has permission to access the object if:

        - no granted users or groups are set (public, everyone has access)
        - the user is an admin
        - the user is the owner of the object
        - the user is in the granted groups of the object
        - the user is in the granted users of the object (i.e. the object is private to an user)

        Args:
            obj (GulpCollabBase): The object to check against.
            enforce_owner (bool, optional): Whether to enforce that the user is the owner of the object (or administrator). Defaults to False.
            throw_on_no_permission (bool, optional): Whether to throw an exception if the user does not have permission. Defaults to False.
        Returns:
            bool: True if the user has permission to access the object, False otherwise.
        """
        if self.is_admin():
            # admin is always granted
            # MutyLogger.get_instance().debug("allowing access to admin")
            return True

        # check if the user is the owner of the object
        if obj.is_owner(self.id):
            # MutyLogger.get_instance().debug("allowing access to object owner")
            return True

        if enforce_owner:
            if throw_on_no_permission:
                raise MissingPermission(
                    f"User {self.id} is not the owner of the object {obj.id}."
                )
            return False

        if not obj.granted_user_group_ids and not obj.granted_user_ids:
            # public object (both granted_user_group_ids and granted_user_ids are empty)
            MutyLogger.get_instance().debug(
                "allowing access to public object, user=%s" % (self.id)
            )
            return True

        # the object is private or have granted users/groups, check if the user is in the granted groups or users

        # check if the user is in the granted groups
        if obj.granted_user_group_ids:
            for group in self.groups:
                if group.id in obj.granted_user_group_ids:
                    MutyLogger.get_instance().debug(
                        "allowing access to granted group %s" % (group.id)
                    )
                    return True

        # check if the user is in the granted users
        if obj.granted_user_ids and self.id in obj.granted_user_ids:
            MutyLogger.get_instance().debug(
                "allowing access to granted user %s" % (self.id)
            )
            return True

        if throw_on_no_permission:
            raise MissingPermission(
                f"User {self.id} does not have the required permissions to access the object {obj.id}."
            )
        MutyLogger.get_instance().debug(
            f"User {self.id} does not have the required permissions to access the object {obj.id}, granted_user_ids={obj.granted_user_ids}, granted_group_ids={obj.granted_user_group_ids}, requestor_user_id={self.id}"
        )
        return False
