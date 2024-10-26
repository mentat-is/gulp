from typing import Optional, override
from sqlalchemy import BIGINT, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column
from gulp.api.collab.structs import (
    GulpCollabObject,
    GulpCollabType,
)
from gulp.utils import logger


class GulpHighlight(GulpCollabObject):
    """
    an highlight in the gulp collaboration system
    """

    __tablename__ = GulpCollabType.HIGHLIGHT

    time_start: Mapped[int] = mapped_column(BIGINT)
    time_end: Mapped[int] = mapped_column(BIGINT)
    log_file_path: Mapped[Optional[str]] = mapped_column(String, default=None)

    __mapper_args__ = {
        f"polymorphic_identity": {GulpCollabType.HIGHLIGHT},
    }

    @override
    def _init(
        self,
        id: str,
        user: str,
        operation: str,
        time_start: int,
        time_end: int,
        log_file_path: str = None,
        **kwargs,
    ) -> None:
        """
        Initialize a GulpHighlight instance.
        Args:
            id (str): The identifier for the highlight.
            user (str): The user associated with the highlight.
            operation (str): The operation associated with the highlight.
            time_start (int): The start time for the highlight.
            time_end (int): The end time for the highlight.
            log_file_path (str, optional): The path to the log file. Defaults to None.
            **kwargs: Additional keyword arguments passed to the GulpCollabObject initializer.
        Returns:
            None
        """
        super().__init__(id, GulpCollabType.HIGHLIGHT, user, operation, **kwargs)
        self.time_start = time_start
        self.time_end = time_end
        self.log_file_path = log_file_path
        logger().debug(
            "---> GulpHighlight: time_start=%s, time_end=%s, log_file_path=%s"
            % (time_start, time_end, log_file_path)
        )
