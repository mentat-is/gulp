from typing import Optional, override

from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column

from gulp.api.collab.structs import GulpCollabBase, GulpCollabType
from gulp.utils import logger


class GulpOperation(GulpCollabBase):
    """
    Represents an operation in the gulp system.
    """

    __tablename__ = GulpCollabType.OPERATION
    index: Mapped[Optional[str]] = mapped_column(
        String(),
        default=None,
        doc="The opensearch index to associate the operation with.",
    )
    description: Mapped[Optional[str]] = mapped_column(
        String(), default=None, doc="The description of the operation."
    )

    __mapper_args__ = {
        f"polymorphic_identity": {GulpCollabType.OPERATION},
    }

    @override
    def _init(
        self, id: str, user: str, index: str = None, description: str = None, **kwargs
    ) -> None:
        """
        Initialize a GulpOperation instance.
        Args:
            id (str): The unique identifier for the operation.
            index (str, optional): The opensearch index to associate the operation with.
            description (str, optional): The description of the operation. Defaults to None.
            **kwargs: Additional keyword arguments.
        """
        super().__init__(id, GulpCollabType.OPERATION, user)
        self.index = index
        self.description = description
        logger().debug(
            "---> GulpOperation: index=%s, description=%s" % (index, description)
        )
