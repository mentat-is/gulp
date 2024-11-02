#!/usr/bin/env python
import asyncio
import sys
import os

from sqlalchemy.sql.base import _NoArg

# from gulp.api.collab import context as collab_context
import muty.json
from gulp import config
from typing import Optional, Type, TypeVar
from gulp.api.collab.structs import GulpCollabType
from gulp.api.elastic.structs import GulpIngestionFilter
from gulp.api import elastic_api
from gulp.api import collab_api
from gulp.api.collab import db
from gulp.utils import logger, configure_logger
from pydantic import BaseModel, Field
from sqlalchemy_mixins.serialize import SerializeMixin
from sqlalchemy.orm import MappedAsDataclass, DeclarativeBase, Mapped, mapped_column
from sqlalchemy import BIGINT, ForeignKey, String
from sqlalchemy.ext.asyncio import AsyncAttrs
from gulp.api.collab.structs import GulpCollabObject, GulpCollabType, GulpCollabBase
from gulp.api.collab.note import GulpNote
from dotwiz import DotWiz
from opensearchpy import AsyncOpenSearch
from gulp.plugin import PluginBase

_os: AsyncOpenSearch = None


async def testbed():
    class TestPydanticClass(BaseModel):
        class Config:
            extra = "allow"
        field_required: dict = Field(..., description="required field", min_length=1)
        field2: str = Field("default", description="test field1")
        field1: int = Field("default", description="test field2")
        
        def __init__(self, **data):
            super().__init__(**data)
            self.field2 = "changed"
            
    class TestPydanticDerivedClass(TestPydanticClass):
        field3: str = Field(None, description="test field3")

    class TestOrmBase(MappedAsDataclass, AsyncAttrs, DeclarativeBase, SerializeMixin):
        """
        base for everything on the collab database
        """
        def __init_subclass__(cls, type: str, **kwargs) -> None:                
            cls.__tablename__ = type
            cls.__mapper_args__ = {
                "polymorphic_identity": type,
                "polymorphic_on": "type",
            }
            print(cls.__name__, cls.__tablename__, cls.__mapper_args__)
            super().__init_subclass__(cls, **kwargs)
        
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
    class TestOrm(TestOrmBase, type='testorm'):        
        #__tablename__ = "testorm"
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

    buf=[]
    if buf:
        print('buf')
    else:
        print('not buf')
    return

    flt=GulpIngestionFilter(opt_storage_ignore_filter=True)
    flt.time_range = {"start": 0, "end": 1}
    #for i in range(10):
    test_fun(flt)
    print('original', flt)
    return

    p = TestPydanticClass(field1=1, another_field="aaa", field_required={"a": 1})
    print(p)
    return
    d = p.model_dump()
    print(d)
    d={}
    print('validating...')
    pp = TestPydanticClass.model_validate(d)
    print(pp)
    return
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

async def _init_gulptest(param1: str, param2: str, *args, **kwargs):
    print(param1, param2, args, kwargs)
    return
    configure_logger()
    logger().debug("---> init")
    config.init()
    
    # reinit collab
    await db.setup(force_recreate=True)

    # reinit elastic
    global _os
    _os = elastic_api.elastic()
    await elastic_api.datastream_create(_os, "testidx")


async def main():
    #configure_logger()
    #config.init()
    #await testbed()
    # connect postgre
    # await collab_api.setup(force_recreate=True)

    # t = TestClass
    # print("field1" in t.__annotations__)
    # print(hasattr(t(), "field1"))
    try:        
        await _init_gulptest('a','b')
        
    finally:
        await elastic_api.shutdown_client(_os)

if __name__ == "__main__":
    asyncio.run(main())
