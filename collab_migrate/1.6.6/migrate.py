#!/usr/bin/env python3
"""
migration script for enhance_document_map table (pre-v1.6.6 -> v1.6.6)

This script will:
  - create a proper `enhance_document_map` collab table (if not present)
  - ensure foreign keys for `user_id`, `glyph_id`, and `operation_id` exist

Usage:
  python3 migrate.py --url postgresql+asyncpg://user:pass@host:port/dbname

Notes:
  - This migration is idempotent and safe to re-run.
  - `enhance_document_map` entries are global and not tied to an operation,
    so `operation_id` remains nullable and can stay unset.
"""

import argparse
import asyncio

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker


async def migrate_enhance_document_map(db_url: str) -> None:
    print("connecting to the database...")
    engine = create_async_engine(db_url, echo=False)
    async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async with async_session() as session:
        async with session.begin():
            print("starting migration transaction for enhance_document_map...")
            try:
                print("step 1/2: creating 'enhance_document_map' table if not exists...")
                await session.execute(
                    text(
                        """
                        CREATE TABLE IF NOT EXISTS enhance_document_map (
                            id character varying PRIMARY KEY,
                            type character varying NOT NULL DEFAULT 'enhance_document_map',
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
                            gulp_event_code integer,
                            plugin character varying
                        );
                        """
                    )
                )

                print("step 2/2: ensuring foreign keys exist...")
                res = await session.execute(
                    text("SELECT 1 FROM pg_constraint WHERE conname = 'enhance_document_map_user_id_fkey';")
                )
                if not res.fetchone():
                    await session.execute(
                        text(
                            'ALTER TABLE enhance_document_map ADD CONSTRAINT enhance_document_map_user_id_fkey FOREIGN KEY (user_id) REFERENCES "user"(id) ON DELETE CASCADE;'
                        )
                    )

                res = await session.execute(
                    text("SELECT 1 FROM pg_constraint WHERE conname = 'enhance_document_map_glyph_id_fkey';")
                )
                if not res.fetchone():
                    await session.execute(
                        text(
                            "ALTER TABLE enhance_document_map ADD CONSTRAINT enhance_document_map_glyph_id_fkey FOREIGN KEY (glyph_id) REFERENCES glyph(id) ON DELETE SET NULL;"
                        )
                    )

                res = await session.execute(
                    text("SELECT 1 FROM pg_constraint WHERE conname = 'enhance_document_map_operation_id_fkey';")
                )
                if not res.fetchone():
                    await session.execute(
                        text(
                            "ALTER TABLE enhance_document_map ADD CONSTRAINT enhance_document_map_operation_id_fkey FOREIGN KEY (operation_id) REFERENCES operation(id) ON DELETE CASCADE;"
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
        description="migrate collab schema to include enhance_document_map table (pre-v1.6.6 -> v1.6.6)",
    )
    parser.add_argument(
        "--url",
        type=str,
        required=True,
        help='the postgresql database connection url (e.g., "postgresql+asyncpg://user:password@host:port/dbname")',
    )

    args = parser.parse_args()

    try:
        await migrate_enhance_document_map(args.url)
    except Exception as e:
        print(f"failed to complete migration: {e}")


if __name__ == "__main__":
    asyncio.run(main())
