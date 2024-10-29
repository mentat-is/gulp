#!/usr/bin/env python
import asyncio
import sys
import os
from gulp.api import collab_api

# from gulp.api.collab import context as collab_context
import muty.json
from gulp import config
from typing import Optional, TypeVar
from gulp.api.collab.structs import GulpCollabType
from gulp.utils import logger, configure_logger
from pydantic import BaseModel, Field
from sqlalchemy_mixins.serialize import SerializeMixin
from sqlalchemy.orm import MappedAsDataclass, DeclarativeBase, Mapped, mapped_column
from sqlalchemy import BIGINT, ForeignKey, String
from sqlalchemy.ext.asyncio import AsyncAttrs


async def testbed():
    class TestPydanticClass(BaseModel):
        class Config:
            extra = "allow"

        field2: str = Field(None, description="test field1")
        field1: int = Field(None, description="test field2")

    class TestPydanticDerivedClass(TestPydanticClass):
        field3: str = Field(None, description="test field3")

    class TestOrmBase(MappedAsDataclass, AsyncAttrs, DeclarativeBase, SerializeMixin):
        """
        base for everything on the collab database
        """

        T = TypeVar("T", bound="TestOrm")

        id: Mapped[str] = mapped_column(
            String, primary_key=True, unique=True, doc="The id of the object."
        )
        type: Mapped[int] = mapped_column(String, doc="The type of the object.")
        time_created: Mapped[Optional[int]] = mapped_column(
            BIGINT,
            default=0,
            doc="The time the object was created, in milliseconds from unix epoch.",
        )
        __mapper_args__ = {
            "polymorphic_identity": "testorm_base",
            "polymorphic_on": "type",
        }

    class TestOrm(TestOrmBase):
        __tablename__ = "testorm"
        id: Mapped[int] = mapped_column(ForeignKey("testorm_base.id"), primary_key=True)
        time_updated: Mapped[Optional[int]] = mapped_column(
            BIGINT,
            default=0,
            doc="The time the object was last updated, in milliseconds from unix epoch.",
        )
        __mapper_args__ = {"polymorphic_identity": "testorm"}

        @classmethod
        def print_name(cls):
            print(cls)
            print(TestOrm)
            print(cls.__name__)

    # t = TestOrm()
    # t.to_dict()
    p = TestPydanticClass(field1=1, field2="test", missing_field="aaa")
    print(p)
    d = p.model_dump()
    print(d)
    pp = TestPydanticClass(**d)
    print(pp)
    dd = pp.model_dump()
    print(dd)
    return

    TestOrm.print_name()
    return
    print("field1" in t.columns)
    print("time_created" in t.columns)
    tt = TestPydanticDerivedClass
    print("field1" in tt.model_fields)
    print("field4" in tt.model_fields)


async def main():
    configure_logger()
    config.init()
    await testbed()
    # connect postgre
    # await collab_api.setup(force_recreate=True)

    # t = TestClass
    # print("field1" in t.__annotations__)
    # print(hasattr(t(), "field1"))


if __name__ == "__main__":
    asyncio.run(main())
