#!/usr/bin/env python3
"""
migration script for mapping parameters and field types (pre-v1.5.1 -> v1.5.1)

This script will:
  - create a proper `mapping_parameters` collab table (if not present)
  - add a `mapping_parameters_id` column to `source`
  - migrate any existing `source.mapping_parameters` JSONB into deduplicated
    `mapping_parameters` rows (id = SHA1 of canonical JSON)
  - set `source.mapping_parameters_id` accordingly
  - drop the old `source.mapping_parameters` column

  - create a proper `field_types_entries` collab table (if not present)
  - add a `field_types_id` column to `source_fields`
  - migrate any existing `source_fields.field_types` JSONB into deduplicated
    `field_types_entries` rows (id = SHA1 of canonical JSON)
  - set `source_fields.field_types_id` accordingly
  - drop the old `source_fields.field_types` column

Usage:
  python3 migrate.py --url postgresql+asyncpg://user:pass@host:port/dbname

Notes:
  - This is idempotent and safe to re-run (INSERT ... ON CONFLICT DO NOTHING used).
  - It creates tables shaped like other collab objects so future ORM inserts using
    `create_internal` will work.
"""

import argparse
import asyncio
import json

import muty.crypto
import muty.time
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker


async def migrate_mapping_parameters(db_url: str) -> None:
    print("connecting to the database...")
    engine = create_async_engine(db_url, echo=False)
    async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async with async_session() as session:
        async with session.begin():
            print("starting migration transaction for mapping parameters...")
            try:
                # 1) create mapping_parameters table if not exists (include collab base columns)
                print("step 1/5: creating 'mapping_parameters' table if not exists...")
                await session.execute(
                    text(
                        """
                        CREATE TABLE IF NOT EXISTS mapping_parameters (
                            id character varying PRIMARY KEY,
                            type character varying NOT NULL DEFAULT 'mapping_parameters',
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
                            mapping jsonb
                        );
                        """
                    )
                )
                # add foreign keys if the referenced tables exist
                res = await session.execute(
                    text("SELECT 1 FROM pg_constraint WHERE conname = 'mapping_parameters_user_id_fkey';")
                )
                if not res.fetchone():
                    await session.execute(
                        text(
                            'ALTER TABLE mapping_parameters ADD CONSTRAINT mapping_parameters_user_id_fkey FOREIGN KEY (user_id) REFERENCES "user"(id) ON DELETE CASCADE;'
                        )
                    )
                res = await session.execute(
                    text("SELECT 1 FROM pg_constraint WHERE conname = 'mapping_parameters_glyph_id_fkey';")
                )
                if not res.fetchone():
                    await session.execute(
                        text(
                            "ALTER TABLE mapping_parameters ADD CONSTRAINT mapping_parameters_glyph_id_fkey FOREIGN KEY (glyph_id) REFERENCES glyph(id) ON DELETE SET NULL;"
                        )
                    )
                res = await session.execute(
                    text("SELECT 1 FROM pg_constraint WHERE conname = 'mapping_parameters_operation_id_fkey';")
                )
                if not res.fetchone():
                    await session.execute(
                        text(
                            "ALTER TABLE mapping_parameters ADD CONSTRAINT mapping_parameters_operation_id_fkey FOREIGN KEY (operation_id) REFERENCES operation(id) ON DELETE CASCADE;"
                        )
                    )
                print("... done.")

                # 2) add mapping_parameters_id column to source if not exists
                print("step 2/5: adding 'mapping_parameters_id' column to source table if not exists...")
                await session.execute(
                    text(
                        "ALTER TABLE source ADD COLUMN IF NOT EXISTS mapping_parameters_id character varying;"
                    )
                )
                # add foreign key
                res = await session.execute(
                    text("SELECT 1 FROM pg_constraint WHERE conname = 'source_mapping_parameters_id_fkey';")
                )
                if not res.fetchone():
                    await session.execute(
                        text(
                            "ALTER TABLE source ADD CONSTRAINT source_mapping_parameters_id_fkey FOREIGN KEY (mapping_parameters_id) REFERENCES mapping_parameters(id) ON DELETE SET NULL;"
                        )
                    )
                print("... done.")

                # 3) migrate existing mapping JSON into mapping_parameters table
                print("step 3/5: migrating existing source.mapping_parameters into mapping_parameters table...")
                # check whether the old `mapping_parameters` column exists before querying it
                res = await session.execute(
                    text(
                        "SELECT 1 FROM information_schema.columns WHERE table_name = 'source' AND column_name = 'mapping_parameters';"
                    )
                )
                if not res.fetchone():
                    print("no 'mapping_parameters' column found on 'source' table; skipping migration of mapping parameters")
                    rows = []
                else:
                    res = await session.execute(
                        text(
                            "select id, mapping_parameters from source where mapping_parameters is not null and mapping_parameters != '{}'::jsonb;"
                        )
                    )
                    rows = res.fetchall()
                    print(f"found {len(rows)} sources with mapping_parameters to migrate")

                for row in rows:
                    src_id = row[0]
                    mapping = row[1]
                    # canonicalize JSON and compute id
                    s = json.dumps(mapping, sort_keys=True, separators=(",", ":"))
                    mp_id = muty.crypto.hash_sha1(s)

                    # insert mapping entry if not exists (use minimal collab columns)
                    await session.execute(
                        text(
                            "INSERT INTO mapping_parameters (id, type, user_id, name, mapping, time_created) VALUES (:id, :type, :user_id, :name, CAST(:mapping AS jsonb), :time_created) ON CONFLICT (id) DO NOTHING;"
                        ),
                        {
                            "id": mp_id,
                            "type": "mapping_parameters",
                            "user_id": None,
                            "name": f"mapping_{mp_id}",
                            "mapping": json.dumps(mapping),
                            "time_created": int(muty.time.now_msec()),
                        },
                    )

                    # update source
                    await session.execute(
                        text(
                            "UPDATE source SET mapping_parameters_id = :mp_id WHERE id = :src_id"
                        ),
                        {"mp_id": mp_id, "src_id": src_id},
                    )

                print("... migration of mapping parameters completed for sources.")

                # 4) drop old mapping_parameters column
                print("step 4/5: dropping old 'mapping_parameters' column from source table...")
                await session.execute(text("ALTER TABLE source DROP COLUMN IF EXISTS mapping_parameters;"))
                print("... done.")

                # 5) final commit/check
                print("step 5/5: finalizing migration...")
                print("migration completed successfully!")
            except Exception as e:
                print(f"\nan error occurred during migration: {e}")
                print("transaction has been rolled back. no changes were made.")
                raise

    await engine.dispose()


async def migrate_field_types(db_url: str) -> None:
    """Migrate existing `source_fields.field_types` into `field_types_entries` table.

    This function mirrors the steps performed by the v1.5.2 migration but is
    included here so a single script can perform both collab migrations.
    """
    print("connecting to the database for field types migration...")
    engine = create_async_engine(db_url, echo=False)
    async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async with async_session() as session:
        async with session.begin():
            print("starting migration transaction for field types...")
            try:
                # 1) create field_types_entries table if not exists (include collab base columns)
                print("step 1/5: creating 'field_types_entries' table if not exists...")
                await session.execute(
                    text(
                        """
                        CREATE TABLE IF NOT EXISTS field_types_entries (
                            id character varying PRIMARY KEY,
                            type character varying NOT NULL DEFAULT 'field_types_entries',
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
                            field_types jsonb
                        );
                        """
                    )
                )
                # add foreign keys if the referenced tables exist
                res = await session.execute(
                    text("SELECT 1 FROM pg_constraint WHERE conname = 'field_types_entries_user_id_fkey';")
                )
                if not res.fetchone():
                    await session.execute(
                        text(
                            'ALTER TABLE field_types_entries ADD CONSTRAINT field_types_entries_user_id_fkey FOREIGN KEY (user_id) REFERENCES "user"(id) ON DELETE CASCADE;'
                        )
                    )
                res = await session.execute(
                    text("SELECT 1 FROM pg_constraint WHERE conname = 'field_types_entries_glyph_id_fkey';")
                )
                if not res.fetchone():
                    await session.execute(
                        text(
                            "ALTER TABLE field_types_entries ADD CONSTRAINT field_types_entries_glyph_id_fkey FOREIGN KEY (glyph_id) REFERENCES glyph(id) ON DELETE SET NULL;"
                        )
                    )
                res = await session.execute(
                    text("SELECT 1 FROM pg_constraint WHERE conname = 'field_types_entries_operation_id_fkey';")
                )
                if not res.fetchone():
                    await session.execute(
                        text(
                            "ALTER TABLE field_types_entries ADD CONSTRAINT field_types_entries_operation_id_fkey FOREIGN KEY (operation_id) REFERENCES operation(id) ON DELETE CASCADE;"
                        )
                    )
                print("... done.")

                # 2) add field_types_id column to source_fields if not exists
                print("step 2/5: adding 'field_types_id' column to source_fields table if not exists...")
                await session.execute(
                    text(
                        "ALTER TABLE source_fields ADD COLUMN IF NOT EXISTS field_types_id character varying;"
                    )
                )
                # add foreign key
                res = await session.execute(
                    text("SELECT 1 FROM pg_constraint WHERE conname = 'source_fields_field_types_id_fkey';")
                )
                if not res.fetchone():
                    await session.execute(
                        text(
                            "ALTER TABLE source_fields ADD CONSTRAINT source_fields_field_types_id_fkey FOREIGN KEY (field_types_id) REFERENCES field_types_entries(id) ON DELETE SET NULL;"
                        )
                    )
                print("... done.")

                # 3) migrate existing field_types JSON into field_types_entries table
                print("step 3/5: migrating existing source_fields.field_types into field_types_entries table...")
                # check whether the old `field_types` column exists before querying it
                res = await session.execute(
                    text(
                        "SELECT 1 FROM information_schema.columns WHERE table_name = 'source_fields' AND column_name = 'field_types';"
                    )
                )
                if not res.fetchone():
                    print("no 'field_types' column found on 'source_fields' table; skipping migration of field types")
                    rows = []
                else:
                    res = await session.execute(
                        text(
                            "select id, field_types from source_fields where field_types is not null and field_types != '{}'::jsonb;"
                        )
                    )
                    rows = res.fetchall()
                    print(f"found {len(rows)} source_fields with field_types to migrate")

                for row in rows:
                    src_id = row[0]
                    field_types = row[1]
                    # canonicalize JSON and compute id
                    s = json.dumps(field_types, sort_keys=True, separators=(",", ":"))
                    ft_id = muty.crypto.hash_sha1(s)

                    # insert field_types entry if not exists (use minimal collab columns)
                    await session.execute(
                        text(
                            "INSERT INTO field_types_entries (id, type, user_id, name, field_types, time_created) VALUES (:id, :type, :user_id, :name, CAST(:field_types AS jsonb), :time_created) ON CONFLICT (id) DO NOTHING;"
                        ),
                        {
                            "id": ft_id,
                            "type": "field_types_entries",
                            "user_id": None,
                            "name": f"field_types_{ft_id}",
                            "field_types": json.dumps(field_types),
                            "time_created": int(muty.time.now_msec()),
                        },
                    )

                    # update source_fields
                    await session.execute(
                        text(
                            "UPDATE source_fields SET field_types_id = :ft_id WHERE id = :src_id"
                        ),
                        {"ft_id": ft_id, "src_id": src_id},
                    )

                print("... migration of field types completed for source_fields.")

                # 4) drop old field_types column
                print("step 4/5: dropping old 'field_types' column from source_fields table...")
                await session.execute(text("ALTER TABLE source_fields DROP COLUMN IF EXISTS field_types;"))
                print("... done.")

                # 5) final commit/check
                print("step 5/5: finalizing field types migration...")
                print("field types migration completed successfully!")
            except Exception as e:
                print(f"\nAn error occurred during field types migration: {e}")
                print("transaction has been rolled back. no changes were made.")
                raise

    await engine.dispose()


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="migrate mapping parameters and field types to dedicated tables (pre-v1.5.1 -> v1.5.1)",
    )
    parser.add_argument(
        "--url",
        type=str,
        required=True,
        help='the postgresql database connection url (e.g., "postgresql+asyncpg://user:password@host:port/dbname")',
    )

    args = parser.parse_args()

    try:
        print("[.] migrating mapping parameters ...")
        await migrate_mapping_parameters(args.url)
        print("[.] migrating field types ...")
        await migrate_field_types(args.url)

    except Exception as e:
        print(f"failed to complete migration: {e}")


if __name__ == "__main__":
    asyncio.run(main())
