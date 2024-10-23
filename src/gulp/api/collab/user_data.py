from sqlalchemy import ForeignKey, String, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio.engine import AsyncEngine
from sqlalchemy.orm import Mapped, mapped_column

from gulp.api.collab.base import CollabBase, GulpCollabFilter
from gulp.api.collab.user import User
from gulp.api.collab.structs import COLLAB_MAX_NAME_LENGTH
from gulp.defs import ObjectAlreadyExists, ObjectNotFound
from gulp.utils import logger

class UserData(CollabBase):
    """
    defines data associated with an user, per operation (e.g. preferences, etc.)
    """

    __tablename__ = "user_data"
    id: Mapped[str] = mapped_column(String(COLLAB_MAX_NAME_LENGTH), primary_key=True, unique=True)
    user: Mapped[str] = mapped_column(ForeignKey("user.name", ondelete="CASCADE"))
    data: Mapped[dict] = mapped_column(JSONB)

    def to_dict(self) -> dict:
        return {
            "user": self.user_id,
            "name": self.id,
            "data": self.data if self.data is not None else {},
        }

    @staticmethod
    async def get(
        engine: AsyncEngine, flt: GulpCollabFilter = None
    ) -> list["UserData"]:
        """
        Retrieve user data based on the provided filter.

        Args:
            engine (AsyncEngine): The database engine.
            flt (GulpCollabFilter, optional): The filter to apply (id, user_id, name, operation_id, limit, offset). Defaults to None.

        Returns:
            list["UserData"]: A list of user data objects that match the filter.

        Raises:
            ObjectNotFound: If no user data is found.
        """
        logger().debug("---> get: flt=%s" % (flt))

        # make a select() query depending on the filter
        q = select(UserData)
        if flt is not None:
            # check each part of flt and build the query
            if flt.id is not None:
                q = q.where(UserData.id.in_(flt.id))
            if flt.owner_id is not None:
                q = q.where(UserData.user_id.in_(flt.owner_id))
            if flt.name is not None:
                q = q.where(UserData.id.in_(flt.name))
            if flt.operation_id is not None:
                q = q.where(UserData.operation_id.in_(flt.operation_id))

            if flt.limit is not None:
                q = q.limit(flt.limit)
            if flt.offset is not None:
                q = q.offset(flt.offset)

        async with AsyncSession(engine) as sess:
            res = await sess.execute(q)
            ud = res.scalars().all()
            if len(ud) == 0:
                logger().warning("no UserData found (flt=%s)" % (flt))
                return []

            logger().info("user_data retrieved: %s ..." % (ud))
            return ud

    @staticmethod
    async def create(
        engine: AsyncEngine,
        token: str,
        name: str,
        operation_id: int,
        data: dict,
        user_id: int = None,
    ) -> "UserData":
        """
        Create a new UserData object in the "user_data" table.

        Args:
            engine (AsyncEngine): The database engine.
            token (str): The user's authentication token.
            name (str): The name of the user data.
            operation_id (int): The operation ID associated with the user data.
            data (dict): The data to be stored in the user data.
            user_id (int, optional): The ID of the user (if set, if not equal to token's user id, the token must be an ADMIN token). Defaults to None.

        Returns:
            UserData: The created UserData object.

        Raises:
            ObjectAlreadyExists: If a user data with the same name already exists.

        """
        logger().debug(
            "---> create: token=%s, name=%s, operation_id=%d, data=%s, user_id=%s"
            % (token, name, operation_id, data, user_id)
        )

        # check permission
        user = await User.check_token_owner(engine, token, user_id)

        async with AsyncSession(engine, expire_on_commit=False) as sess:
            # check if user data already exists
            q = select(UserData).where(UserData.id == name)
            res = await sess.execute(q)
            if res.scalar_one_or_none() is not None:
                raise ObjectAlreadyExists("user_data %s already exists" % (name))

            # create
            ud = UserData(
                user_id=user_id if user_id is not None else user.id,
                name=name,
                operation_id=operation_id,
                data=data,
            )
            sess.add(ud)
            await sess.flush()

            # also update user
            if ud.id not in user.user_data:
                logger().error(
                    "appending %d to %s" % (ud.id, user.user_data)
                )
                user.user_data.append(ud.id)
                await user.update_user_data(sess)
                sess.add(user)
                await sess.flush()

            await sess.commit()
            logger().info("user_data created: %s, user=%s ..." % (ud, user))
            return ud

    @staticmethod
    async def delete(
        engine: AsyncEngine, token: str, user_data_id: int, user_id: int = None
    ) -> None:
        """
        Delete user data.

        Args:
            engine (AsyncEngine): The database engine.
            token (str): The user's authentication token.
            user_data_id (int): The ID of the user data to delete.
            user_id (int, optional): The ID of the user (if set, if not equal to token's user id, the token must be an ADMIN token). Defaults to None.

        Raises:
            ObjectNotFound: If the user data with the given ID does not exist.

        Returns:
            None
        """
        logger().debug(
            "---> delete: token=%s, user_data_id=%d, user_id=%s"
            % (token, user_data_id, user_id)
        )

        # check if token has permission
        user = await User.check_token_owner(engine, token, user_id)
        async with AsyncSession(engine) as sess:
            # get user_data
            q = select(UserData).where(UserData.id == user_data_id)
            res = await sess.execute(q)
            user_data = res.scalar_one_or_none()
            if user_data is None:
                raise ObjectNotFound("user_data id %d does not exist!" % (user_data_id))

            await sess.delete(user_data)
            logger().info("user_data deleted: %s ..." % (user_data_id))

            if user_data_id in user.user_data:
                # also update user_data in User
                user.user_data.remove(user_data_id)
                await user.update_user_data(sess)
                sess.add(user)

            # commit session
            await sess.commit()

    @staticmethod
    async def update(
        engine: AsyncEngine,
        token: str,
        user_data_id: int,
        name: str = None,
        data: dict = None,
        user_id: int = None,
    ) -> "UserData":
        """
        Update user data with the specified parameters.

        Args:
            engine (AsyncEngine): The database engine.
            token (str): The authentication token.
            user_data_id (int): The ID of the user data to update.
            name (str, optional): The new name for the user data. Defaults to None.
            data (dict, optional): The new data for the user data. Defaults to None.
            user_id (int, optional): The ID of the user (if set, if not equal to token's user id, the token must be an ADMIN token). Defaults to None.

        Returns:
            UserData: The updated user data.

        Raises:
            ObjectNotFound: If the user data with the specified ID is not found.
        """
        logger().debug(
            "---> update: token=%s, user_data_id=%d, user_id=%s"
            % (token, user_data_id, user_id)
        )

        # check if token has permission
        await User.check_token_owner(engine, token, user_id)
        async with AsyncSession(engine, expire_on_commit=False) as sess:
            q = select(UserData).where(UserData.id == user_data_id).with_for_update()
            res = await sess.execute(q)
            user_data = res.scalar_one_or_none()
            if user_data is None:
                raise ObjectNotFound("user_data %d not found!" % (user_data_id))

            if name is not None:
                user_data.id = name
            if data is not None:
                user_data.data = data

            sess.add(user_data)
            await sess.commit()
            logger().info("user_data updated: %s ..." % (user_data))
            return user_data
