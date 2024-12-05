#!/usr/bin/env python
import asyncio
import os
import sys
from typing import Optional, TypeVar

import muty.json
import pytest
import pytest_asyncio
from muty.log import MutyLogger, configure_logger
from pydantic import BaseModel, Field
from sqlalchemy import BIGINT, ForeignKey, String
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, Mapped, MappedAsDataclass, mapped_column
from sqlalchemy_mixins.serialize import SerializeMixin

# from gulp.api.collab import context as collab_context
from gulp.api import collab_api
from gulp.api.collab.context import GulpContext
from gulp.api.collab.db import session, setup
from gulp.api.collab.structs import GulpCollabType
from gulp.config import GulpConfig


async def _init():
    configure_logger()
    MutyLogger.get_instance().debug("---> init")
    config.init()
    await setup(force_recreate=True)


@pytest_asyncio.fixture(scope="module")
async def init():
    await _init()


@pytest.mark.asyncio
class TestCollab:
    async def test_context(self, init):
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
