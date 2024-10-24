#!/usr/bin/env python
import asyncio
import sys
import os
from gulp.api import collab_api

# from gulp.api.collab import context as collab_context
import muty.json
from gulp import config
from typing import Optional, TypeVar
from gulp.utils import logger, configure_logger
from pydantic import BaseModel, Field
from sqlalchemy_mixins.serialize import SerializeMixin
from sqlalchemy.orm import MappedAsDataclass, DeclarativeBase, Mapped, mapped_column
from sqlalchemy import BIGINT, String
from sqlalchemy.ext.asyncio import AsyncAttrs


class TestClass(MappedAsDataclass, AsyncAttrs, DeclarativeBase, SerializeMixin):
    field2: str = Field(None, description="test field1")
    field1: int = Field(None, description="test field2")


class TestOrm(MappedAsDataclass, AsyncAttrs, DeclarativeBase, SerializeMixin):
    """
    base for everything on the collab database
    """

    T = TypeVar("T", bound="TestOrm")

    __tablename__ = "testorm"

    id: Mapped[str] = mapped_column(
        String, primary_key=True, unique=True, doc="The id of the object."
    )
    type: Mapped[int] = mapped_column(String, doc="The type of the object.")
    time_created: Mapped[Optional[int]] = mapped_column(
        BIGINT,
        default=0,
        doc="The time the object was created, in milliseconds from unix epoch.",
    )
    time_updated: Mapped[Optional[int]] = mapped_column(
        BIGINT,
        default=0,
        doc="The time the object was last updated, in milliseconds from unix epoch.",
    )


async def main():
    configure_logger()
    config.init()

    # connect postgre
    # await collab_api.setup(force_recreate=True)

    # t = TestClass
    # print("field1" in t.__annotations__)
    # print(hasattr(t(), "field1"))
    t = TestOrm
    print("field1" in t.__annotations__)
    print("time_created" in t.__annotations__)


if __name__ == "__main__":
    asyncio.run(main())
