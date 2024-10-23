#!/usr/bin/env python
import asyncio
import sys
import os
from gulp.api import collab_api
from gulp.api.collab import context as collab_context
import muty.json
from gulp import config
from gulp.utils import logger, configure_logger

async def main():
    configure_logger()
    config.init()

    # connect postgre
    await collab_api.setup(force_recreate=True)

    

if __name__ == "__main__":
    asyncio.run(main())
