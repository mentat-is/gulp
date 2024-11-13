#!/usr/bin/env python
import asyncio
import sys
import os

import pytest
import pytest_asyncio

# from gulp.api.collab import context as collab_context
from gulp.api import collab_api
import muty.json
from gulp.config import GulpConfig
from typing import Optional, TypeVar
from gulp.api.collab.structs import GulpCollabType
from gulp.utils import GulpLogger, configure_logger
from pydantic import BaseModel, Field
from sqlalchemy_mixins.serialize import SerializeMixin
from sqlalchemy.orm import MappedAsDataclass, DeclarativeBase, Mapped, mapped_column
from sqlalchemy import BIGINT, ForeignKey, String
from sqlalchemy.ext.asyncio import AsyncAttrs
from gulp.api.collab.context import GulpContext
from gulp.api.collab.db import setup, session


async def _init():
    configure_logger()
    GulpLogger.get_logger().debug("---> init")
    config.init()
    await setup(force_recreate=True)


@pytest_asyncio.fixture(scope="module")
async def init():
    await _init()


@pytest.mark.asyncio
class TestCollab:
    async def test_context(self, init):
        # c = GulpContext("test_context")

        return

    async def test_note(self, init):
        return

    async def test_base(self, init):
        return

    async def test_testbed(self, init):
        return


####################
# Test
####################
asyncio.run(_init())
