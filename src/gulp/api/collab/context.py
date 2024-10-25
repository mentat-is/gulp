from typing import Optional, override

from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column

from gulp.api.collab.structs import GulpCollabBase, GulpCollabType
from gulp.utils import logger


class GulpContext(GulpCollabBase):
    """
    Represents a context object: in gulp terms, a context is used to group a set of data coming from the same host.

    Attributes:
        id (int): The unique identifier of the context.
        name (str): The name of the context.
        color (str): A color hex string (0xffffff, #ffffff)
    """

    __tablename__ = "context"
    color: Mapped[Optional[str]] = mapped_column(
        String(32), default="#ffffff", doc="The color of the context."
    )

    __mapper_args__ = {
        "polymorphic_identity": "context",
    }

    @override
    def __init__(self, id: str, user: str, color: str = None) -> None:
        """
        Initialize a GulpContext instance.
        Args:
            id (str): The unique identifier for the context.
            user (str): The object's owner.
            color (str, optional): The color associated with the context. Defaults to None.
        """

        super().__init__(id, GulpCollabType.CONTEXT, user)
        self.color = color
        logger().debug("---> GulpContext: id=%s, color=%s" % (id, color))
