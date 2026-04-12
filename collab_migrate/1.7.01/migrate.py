#!/usr/bin/env python3
"""
Migration script for enhance_document_map table (pre-v1.7.0 -> v1.7.0)

This script will:
  - drop the old ``gulp_event_code`` integer column
  - add the new ``match_criteria`` JSONB column (NOT NULL, default '{}')

Usage:
  python3 migrate.py --url postgresql+asyncpg://user:pass@host:port/dbname

Notes:
  - This migration is idempotent and safe to re-run.
  - Existing rows will have ``match_criteria`` set to ``{}`` after migration.
    Re-create the entries via the API with the desired criteria.
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
            print("starting migration transaction for enhance_document_map (1.7.0)...")
            try:
                print("step 1/2: dropping old 'gulp_event_code' column if present...")
                res = await session.execute(
                    text(
                        """
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name = 'enhance_document_map'
                          AND column_name = 'gulp_event_code';
                        """
                    )
                )
                if res.fetchone():
                    await session.execute(
                        text("ALTER TABLE enhance_document_map DROP COLUMN gulp_event_code;")
                    )
                    print("  dropped 'gulp_event_code'.")
                else:
                    print("  'gulp_event_code' not present, skipping.")

                print("step 2/2: adding 'match_criteria' JSONB column if not present...")
                res = await session.execute(
                    text(
                        """
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name = 'enhance_document_map'
                          AND column_name = 'match_criteria';
                        """
                    )
                )
                if not res.fetchone():
                    await session.execute(
                        text(
                            "ALTER TABLE enhance_document_map ADD COLUMN match_criteria jsonb NOT NULL DEFAULT '{}'::jsonb;"
                        )
                    )
                    print("  added 'match_criteria'.")
                else:
                    print("  'match_criteria' already present, skipping.")

                print("migration completed successfully!")
            except Exception as e:
                print(f"\nan error occurred during migration: {e}")
                print("transaction has been rolled back. no changes were made.")
                raise

    await engine.dispose()


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="migrate enhance_document_map table (pre-v1.7.0 -> v1.7.0)",
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
