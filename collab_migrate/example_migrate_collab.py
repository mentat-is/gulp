#!/usr/bin/env python3
"""
migrates gulp note table from old format with docs as a jsonb array to a new format with doc as a single jsonb object.

usage:
    ./migrate.py --url postgresql+psycopg://postgres:Gulp1234!@localhost:5432/gulp

"""

import argparse
import asyncio
from typing import Optional

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text


async def migrate_note_table(db_url: str) -> None:
    """
    performs the migration of the 'note' table in the database.

    This function connects to the database, renames the old 'docs' column to 'docs_old', also handling the data migration.

    Args:
        db_url (str): the database connection url.

    Returns:
        None
    """
    print("connecting to the database...")
    # create an async engine to connect to the database
    engine = create_async_engine(db_url, echo=False)

    # create a session factory
    async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    # start a session and a transaction
    async with async_session() as session:
        async with session.begin():
            print("starting migration transaction for 'note' table...")

            try:
                # step 1: rename the old 'docs' column to 'docs_old'
                print("step 1/4: renaming column 'docs' to 'docs_old'...")
                await session.execute(
                    text("alter table note rename column docs to docs_old;")
                )
                print("... 'docs' column renamed to 'docs_old'.")

                # step 2: add the new 'doc' column
                print("step 2/4: adding new column 'doc' of type jsonb...")
                await session.execute(text("alter table note add column doc jsonb;"))
                print("... 'doc' column added.")

                # step 3: migrate data from 'docs_old' to 'doc'
                # this takes the first element of the 'docs_old' json array
                print("step 3/4: migrating data from 'docs_old' to 'doc'...")
                update_query = text(
                    """
                    update note
                    set doc = docs_old[1]
                    where
                        docs_old is not null and
                        array_length(docs_old, 1) > 0;
                    """
                )
                result = await session.execute(update_query)
                print(f"... {result.rowcount} records updated.")

                # step 4: drop the old 'docs_old' column
                print("step 4/4: dropping old column 'docs_old'...")
                await session.execute(text("alter table note drop column docs_old;"))
                print("... 'docs_old' column dropped.")

                print("\nmigration completed successfully!")

            except Exception as e:
                # if an error occurs, the transaction will be rolled back automatically
                print(f"\nan error occurred during migration: {e}")
                print("transaction has been rolled back. no changes were made.")
                raise

    # dispose of the engine
    await engine.dispose()


async def create_task_table(db_url: str) -> None:
    """
    this is an example using a full script exported from adminer to create the 'task' table in the database.
    """
    print("connecting to the database...")
    # create an async engine to connect to the database
    engine = create_async_engine(db_url, echo=False)

    # create a session factory
    async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    # start a session and a transaction
    async with async_session() as session:
        async with session.begin():
            print("creating 'task' table ...\n")

            try:
                await session.execute(
                    text(
                        """
                    DROP TABLE IF EXISTS "task";
                    CREATE TABLE "public"."task" (
                        "ws_id" character varying NOT NULL,
                        "operation_id" character varying NOT NULL,
                        "req_id" character varying NOT NULL,
                        "task_type" character varying NOT NULL,
                        "params" jsonb NOT NULL,
                        "raw_data" bytea,
                        "id" character varying NOT NULL,
                        "type" character varying NOT NULL,
                        "owner_user_id" character varying NOT NULL,
                        "granted_user_ids" character varying[],
                        "granted_user_group_ids" character varying[],
                        "time_created" bigint,
                        "time_updated" bigint,
                        "glyph_id" character varying,
                        "name" character varying,
                        "description" character varying,
                        CONSTRAINT "task_pkey" PRIMARY KEY ("id")
                    )
                    WITH (oids = false);


                    ALTER TABLE ONLY "public"."task" ADD CONSTRAINT "task_glyph_id_fkey" FOREIGN KEY (glyph_id) REFERENCES glyph(id) ON DELETE SET NULL NOT DEFERRABLE;
                    ALTER TABLE ONLY "public"."task" ADD CONSTRAINT "task_operation_id_fkey" FOREIGN KEY (operation_id) REFERENCES operation(id) ON DELETE CASCADE NOT DEFERRABLE;
                    ALTER TABLE ONLY "public"."task" ADD CONSTRAINT "task_owner_user_id_fkey" FOREIGN KEY (owner_user_id) REFERENCES "user"(id) ON DELETE CASCADE NOT DEFERRABLE;                                           
                """
                    )
                )
                print("'task' table created!\n")
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
        await migrate_note_table(args.url)
    except Exception as e:
        print(f"failed to complete migration: {e}")


if __name__ == "__main__":
    # run the main async function
    asyncio.run(main())
