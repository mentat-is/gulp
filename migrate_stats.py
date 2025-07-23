#!/usr/bin/env python3
"""
usage:
    ./migrate.py --url postgresql+psycopg://postgres:Gulp1234!@localhost:5432/gulp

"""

import argparse
import asyncio
from typing import Optional

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text


async def migrate_stats_table(db_url: str) -> None:
    print("connecting to the database...")
    # create an async engine to connect to the database
    engine = create_async_engine(db_url, echo=False)

    # create a session factory
    async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    # start a session and a transaction
    async with async_session() as session:
        async with session.begin():
            print("starting migration transaction for 'stats' table...")

            try:
                # step 1: add the new 'req_type' column
                print("step 1/2: adding new column 'req_type' of type jsonb...")
                await session.execute(text("alter table request_stats add column req_type character varying;"))
                print("... 'req_type' column added.")

                # step 2: add "req_type": "ingest" to all existing records
                print("step 2/2: updating existing records with 'req_type'...")
                update_query = text(
                    """
                    update request_stats
                    set req_type = 'ingest'
                    where req_type is null;
                    """
                )
                result = await session.execute(update_query)
                print(f"... {result.rowcount} records updated.")

                print("\nmigration completed successfully!")

            except Exception as e:
                # if an error occurs, the transaction will be rolled back automatically
                print(f"\nan error occurred during migration: {e}")
                print("transaction has been rolled back. no changes were made.")
                raise

    # dispose of the engine
    await engine.dispose()


async def main() -> None:
    """
    main function to parse arguments and run the migration.
    """
    # set up argument parser
    parser = argparse.ArgumentParser(
        description="migrate the 'note' table in the database."
    )
    parser.add_argument(
        "--url",
        type=str,
        required=True,
        help='the postgresql database connection url (e.g., "postgresql+asyncpg://user:password@host:port/dbname")',
    )

    args = parser.parse_args()

    try:
        # run the migration
        await migrate_stats_table(args.url)
    except Exception as e:
        print(f"failed to complete migration: {e}")


if __name__ == "__main__":
    # run the main async function
    asyncio.run(main())
