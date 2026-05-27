#!/usr/bin/env python3
"""
Migration script for shared_object table (pre-v1.7.400 -> v1.7.400).

This script will:
  - create the `shared_object` collab table if it does not exist
  - add missing foreign keys to `user`, `operation`, and `glyph`

Usage:
  python3 migrate.py --url postgresql+asyncpg://user:pass@host:port/dbname

Notes:
  - This migration is idempotent and safe to re-run.
"""

import argparse
import asyncio

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker


async def migrate_shared_object(db_url: str) -> None:
    print("connecting to the database...")
    engine = create_async_engine(db_url, echo=False)
    async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async with async_session() as session:
        async with session.begin():
            print("starting migration transaction for shared_object (1.7.400)...")
            try:
                print("step 1/2: creating 'shared_object' table if not exists...")
                await session.execute(text("""
                        CREATE TABLE IF NOT EXISTS shared_object (
                            id character varying PRIMARY KEY,
                            type character varying NOT NULL DEFAULT 'shared_object',
                            user_id character varying,
                            name character varying,
                            time_created bigint,
                            time_updated bigint,
                            operation_id character varying,
                            glyph_id character varying,
                            description character varying,
                            tags character varying[],
                            color character varying,
                            granted_user_ids character varying[],
                            granted_user_group_ids character varying[],
                            obj jsonb NOT NULL DEFAULT '{}'::jsonb,
                            obj_type character varying NOT NULL
                        );
                        """))

                print("step 2/2: ensuring foreign keys exist...")
                res = await session.execute(
                    text(
                        "SELECT 1 FROM pg_constraint WHERE conname = 'shared_object_user_id_fkey';"
                    )
                )
                if not res.fetchone():
                    await session.execute(
                        text(
                            'ALTER TABLE shared_object ADD CONSTRAINT shared_object_user_id_fkey FOREIGN KEY (user_id) REFERENCES "user"(id) ON DELETE CASCADE;'
                        )
                    )

                res = await session.execute(
                    text(
                        "SELECT 1 FROM pg_constraint WHERE conname = 'shared_object_operation_id_fkey';"
                    )
                )
                if not res.fetchone():
                    await session.execute(
                        text(
                            "ALTER TABLE shared_object ADD CONSTRAINT shared_object_operation_id_fkey FOREIGN KEY (operation_id) REFERENCES operation(id) ON DELETE CASCADE;"
                        )
                    )

                res = await session.execute(
                    text(
                        "SELECT 1 FROM pg_constraint WHERE conname = 'shared_object_glyph_id_fkey';"
                    )
                )
                if not res.fetchone():
                    await session.execute(
                        text(
                            "ALTER TABLE shared_object ADD CONSTRAINT shared_object_glyph_id_fkey FOREIGN KEY (glyph_id) REFERENCES glyph(id) ON DELETE SET NULL;"
                        )
                    )

                print("migration completed successfully!")
            except Exception as e:
                print(f"\nan error occurred during migration: {e}")
                print("transaction has been rolled back. no changes were made.")
                raise

    await engine.dispose()


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="migrate shared_object table (pre-v1.7.400 -> v1.7.400)",
    )
    parser.add_argument(
        "--url",
        type=str,
        required=True,
        help='the postgresql database connection url (e.g., "postgresql+asyncpg://user:password@host:port/dbname")',
    )

    args = parser.parse_args()

    try:
        await migrate_shared_object(args.url)
    except Exception as e:
        print(f"failed to complete migration: {e}")


if __name__ == "__main__":
    asyncio.run(main())
