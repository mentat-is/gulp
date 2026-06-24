#!/usr/bin/env python3
"""
Migration script for glyph references (pre-v1.7.400 -> v1.7.601).

This script will:
  - drop every foreign key constraint that references the `glyph` table
  - delete all rows from the `glyph` table
  - enforce `glyph.img` as NOT NULL when the column exists
  - insert the default dashboard shared objects when `shared_object` exists

Usage:
  python3 migrate.py --url postgresql+psycopg://user:pass@host:port/dbname

Notes:
  - This migration is idempotent and safe to re-run.
  - `glyph_id` columns on collab tables are intentionally kept as plain strings.
"""

import argparse
import asyncio
import json

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker


DEFAULT_SHARED_OBJECTS: tuple[dict[str, object], ...] = (
    {
        "obj_type": "dashboard",
        "obj": {
            "id": "log_source_distribution",
            "name": "Log Source Distribution",
            "type": "fixed",
            "labels": {"value": "Logs", "legend": "Source"},
            "visible": True,
            "dashboard": "vertical_chart",
            "required_ecs": ["gulp.source_id"],
            "opensearch_query": {
                "aggs": {
                    "sources": {
                        "terms": {
                            "field": "gulp.source_id",
                            "order": {"_count": "desc"},
                        }
                    }
                }
            },
            "size": 0,
            "query": {},
        },
        "id": "dash_log_source_distribution",
        "type": "shared_object",
        "user_id": "admin",
        "name": "log_source_distribution",
        "time_created": 1781851775,
        "time_updated": 1781851775,
    },
    {
        "obj_type": "dashboard",
        "obj": {
            "id": "global_event_rate",
            "name": "Global Event Rate",
            "type": "fixed",
            "labels": {
                "x_axis": "Time",
                "y_axis": "Count",
                "tooltip": "Events",
            },
            "visible": True,
            "dashboard": "line_chart",
            "required_ecs": ["@timestamp"],
            "opensearch_query": {
                "aggs": {
                    "histogram": {
                        "date_histogram": {
                            "field": "@timestamp",
                            "min_doc_count": 0,
                            "fixed_interval": "15m",
                        }
                    }
                },
                "size": 0,
                "query": {},
            },
            "template_version": "1.0",
        },
        "id": "dash_global_event_rate",
        "type": "shared_object",
        "user_id": "admin",
        "name": "global_event_rate",
        "time_created": 1781851775,
        "time_updated": 1781851775,
    },
)


def quote_identifier(identifier: str) -> str:
    """Quote a PostgreSQL identifier for use in dynamic DDL."""
    return '"' + identifier.replace('"', '""') + '"'


async def migrate_glyph_references(db_url: str) -> None:
    """Drop glyph references and seed default shared dashboard objects."""
    print("connecting to the database...")
    engine = create_async_engine(db_url, echo=False)
    async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    try:
        async with async_session() as session:
            async with session.begin():
                print(
                    "starting migration transaction for glyph references (1.7.601)..."
                )
                try:
                    print("step 1/5: checking for 'glyph' table...")
                    res = await session.execute(text("""
                            SELECT 1
                            FROM information_schema.tables
                            WHERE table_schema = 'public'
                              AND table_name = 'glyph';
                            """))
                    glyph_exists = res.fetchone() is not None
                    if glyph_exists:
                        print("step 2/5: dropping foreign keys referencing 'glyph'...")
                        res = await session.execute(text("""
                                SELECT
                                    src_namespace.nspname AS table_schema,
                                    src_table.relname AS table_name,
                                    constraint_info.conname AS constraint_name
                                FROM pg_constraint AS constraint_info
                                JOIN pg_class AS src_table
                                    ON src_table.oid = constraint_info.conrelid
                                JOIN pg_namespace AS src_namespace
                                    ON src_namespace.oid = src_table.relnamespace
                                JOIN pg_class AS ref_table
                                    ON ref_table.oid = constraint_info.confrelid
                                JOIN pg_namespace AS ref_namespace
                                    ON ref_namespace.oid = ref_table.relnamespace
                                WHERE constraint_info.contype = 'f'
                                  AND ref_namespace.nspname = 'public'
                                  AND ref_table.relname = 'glyph'
                                ORDER BY src_namespace.nspname, src_table.relname, constraint_info.conname;
                                """))
                        constraints = res.fetchall()
                        if not constraints:
                            print(
                                "  no foreign keys referencing 'glyph' found, skipping."
                            )
                        else:
                            for table_schema, table_name, constraint_name in constraints:
                                qualified_table = (
                                    f"{quote_identifier(table_schema)}."
                                    f"{quote_identifier(table_name)}"
                                )
                                quoted_constraint = quote_identifier(constraint_name)
                                await session.execute(
                                    text(
                                        "ALTER TABLE "
                                        f"{qualified_table} DROP CONSTRAINT {quoted_constraint};"
                                    )
                                )
                                print(
                                    "  dropped "
                                    f"{constraint_name} on {table_schema}.{table_name}."
                                )

                        print("step 3/5: deleting all rows from 'glyph'...")
                        res = await session.execute(text("DELETE FROM glyph;"))
                        print(f"  deleted {res.rowcount} glyph row(s).")

                        print(
                            "step 4/5: enforcing 'glyph.img' NOT NULL when present..."
                        )
                        res = await session.execute(text("""
                                SELECT 1
                                FROM information_schema.columns
                                WHERE table_schema = 'public'
                                  AND table_name = 'glyph'
                                  AND column_name = 'img';
                                """))
                        if res.fetchone():
                            await session.execute(
                                text("ALTER TABLE glyph ALTER COLUMN img SET NOT NULL;")
                            )
                            print("  enforced 'glyph.img' NOT NULL.")
                        else:
                            print("  'glyph.img' column not present, skipping.")
                    else:
                        print("  'glyph' table not present, skipping glyph cleanup.")

                    print("step 5/5: inserting default dashboard shared objects...")
                    res = await session.execute(text("""
                            SELECT 1
                            FROM information_schema.tables
                            WHERE table_schema = 'public'
                              AND table_name = 'shared_object';
                            """))
                    if not res.fetchone():
                        print("  'shared_object' table not present, skipping.")
                    else:
                        stmt = text(
                            """
                            INSERT INTO shared_object (
                                obj_type,
                                obj,
                                id,
                                type,
                                user_id,
                                name,
                                time_created,
                                time_updated,
                                operation_id,
                                glyph_id,
                                description,
                                tags,
                                color,
                                granted_user_ids,
                                granted_user_group_ids
                            )
                            VALUES (
                                :obj_type,
                                CAST(:obj AS jsonb),
                                :id,
                                :type,
                                :user_id,
                                :name,
                                :time_created,
                                :time_updated,
                                NULL,
                                NULL,
                                NULL,
                                NULL,
                                NULL,
                                NULL,
                                NULL
                            )
                            ON CONFLICT (id) DO NOTHING;
                            """
                        )
                        for shared_object in DEFAULT_SHARED_OBJECTS:
                            res = await session.execute(
                                stmt,
                                {
                                    **shared_object,
                                    "obj": json.dumps(shared_object["obj"]),
                                },
                            )
                            action = (
                                "inserted"
                                if res.rowcount and res.rowcount > 0
                                else "already present"
                            )
                            print(f"  {action}: {shared_object['id']}.")

                    print("migration completed successfully!")
                except Exception as e:
                    print(f"\nan error occurred during migration: {e}")
                    print("transaction has been rolled back. no changes were made.")
                    raise
    finally:
        await engine.dispose()


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="migrate glyph references (pre-v1.7.601 -> v1.7.601)",
    )
    parser.add_argument(
        "--url",
        type=str,
        required=True,
        help='the postgresql database connection url (e.g., "postgresql+psycopg://user:password@host:port/dbname")',
    )

    args = parser.parse_args()

    try:
        await migrate_glyph_references(args.url)
    except Exception as e:
        print(f"failed to complete migration: {e}")


if __name__ == "__main__":
    asyncio.run(main())
