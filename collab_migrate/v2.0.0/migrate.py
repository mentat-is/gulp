#!/usr/bin/env python3
"""Add normalized note storage changes for gULP 2.0.0.

Usage:
  python3 migrate.py --url postgresql+psycopg://user:password@host:port/dbname
"""

import argparse
import asyncio

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker


async def migrate_note_text(db_url: str) -> None:
    """Create shared note text and attachment storage."""
    engine = create_async_engine(db_url, echo=False)
    async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    try:
        async with async_session() as session:
            async with session.begin():
                await session.execute(
                    text(
                        """
                        CREATE TABLE IF NOT EXISTS note_text (
                            id character varying PRIMARY KEY,
                            type character varying NOT NULL,
                            user_id character varying NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
                            name character varying NOT NULL,
                            time_created bigint NOT NULL,
                            time_updated bigint NOT NULL,
                            operation_id character varying REFERENCES operation(id) ON DELETE CASCADE,
                            glyph_id character varying,
                            description character varying,
                            tags character varying[],
                            color character varying,
                            granted_user_ids character varying[],
                            granted_user_group_ids character varying[],
                            text character varying NOT NULL
                        );
                        """
                    )
                )
                await session.execute(
                    text(
                        """
                        CREATE TABLE IF NOT EXISTS note_attachment (
                            id character varying PRIMARY KEY,
                            note_id character varying NOT NULL REFERENCES note(id) ON DELETE CASCADE,
                            user_id character varying NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
                            operation_id character varying NOT NULL REFERENCES operation(id) ON DELETE CASCADE,
                            title character varying NOT NULL,
                            description character varying NOT NULL DEFAULT '',
                            mime_type character varying NOT NULL
                        );
                        """
                    )
                )
                await session.execute(
                    text(
                        """
                        ALTER TABLE note
                            ADD COLUMN IF NOT EXISTS text_id character varying,
                            ADD COLUMN IF NOT EXISTS doc_id character varying;
                        """
                    )
                )
                await session.execute(
                    text(
                        """
                        DO $$ BEGIN
                            IF NOT EXISTS (
                                SELECT 1 FROM pg_constraint
                                WHERE conrelid = 'note_text'::regclass
                                  AND conname = 'note_text_user_id_fkey'
                            ) THEN
                                ALTER TABLE note_text ADD CONSTRAINT note_text_user_id_fkey
                                FOREIGN KEY (user_id) REFERENCES "user"(id) ON DELETE CASCADE;
                            END IF;
                            IF NOT EXISTS (
                                SELECT 1 FROM pg_constraint
                                WHERE conrelid = 'note_text'::regclass
                                  AND conname = 'note_text_operation_id_fkey'
                            ) THEN
                                ALTER TABLE note_text ADD CONSTRAINT note_text_operation_id_fkey
                                FOREIGN KEY (operation_id) REFERENCES operation(id) ON DELETE CASCADE;
                            END IF;
                            IF NOT EXISTS (
                                SELECT 1 FROM pg_constraint
                                WHERE conrelid = 'note'::regclass
                                  AND conname = 'note_text_id_fkey'
                            ) THEN
                                ALTER TABLE note ADD CONSTRAINT note_text_id_fkey
                                FOREIGN KEY (text_id) REFERENCES note_text(id) ON DELETE SET NULL;
                            END IF;
                            IF NOT EXISTS (
                                SELECT 1 FROM pg_constraint
                                WHERE conrelid = 'note_attachment'::regclass
                                  AND conname = 'note_attachment_note_id_fkey'
                            ) THEN
                                ALTER TABLE note_attachment ADD CONSTRAINT note_attachment_note_id_fkey
                                FOREIGN KEY (note_id) REFERENCES note(id) ON DELETE CASCADE;
                            END IF;
                            IF NOT EXISTS (
                                SELECT 1 FROM pg_constraint
                                WHERE conrelid = 'note_attachment'::regclass
                                  AND conname = 'note_attachment_user_id_fkey'
                            ) THEN
                                ALTER TABLE note_attachment ADD CONSTRAINT note_attachment_user_id_fkey
                                FOREIGN KEY (user_id) REFERENCES "user"(id) ON DELETE CASCADE;
                            END IF;
                            IF NOT EXISTS (
                                SELECT 1 FROM pg_constraint
                                WHERE conrelid = 'note_attachment'::regclass
                                  AND conname = 'note_attachment_operation_id_fkey'
                            ) THEN
                                ALTER TABLE note_attachment ADD CONSTRAINT note_attachment_operation_id_fkey
                                FOREIGN KEY (operation_id) REFERENCES operation(id) ON DELETE CASCADE;
                            END IF;
                        END $$;
                        """
                    )
                )
                await session.execute(
                    text(
                        "CREATE INDEX IF NOT EXISTS idx_note_text_operation ON note_text (operation_id);"
                    )
                )
                await session.execute(
                    text(
                        "CREATE INDEX IF NOT EXISTS idx_note_text_id ON note (text_id);"
                    )
                )
                await session.execute(
                    text(
                        "CREATE INDEX IF NOT EXISTS idx_note_context ON note (context_id);"
                    )
                )
                await session.execute(
                    text(
                        "CREATE INDEX IF NOT EXISTS idx_note_source ON note (source_id);"
                    )
                )
                await session.execute(
                    text("CREATE INDEX IF NOT EXISTS idx_note_doc ON note (doc_id);")
                )
                await session.execute(
                    text("CREATE INDEX IF NOT EXISTS idx_note_user ON note (user_id);")
                )
                await session.execute(
                    text(
                        "CREATE INDEX IF NOT EXISTS idx_note_attachment_note ON note_attachment (note_id);"
                    )
                )
                await session.execute(
                    text(
                        "CREATE INDEX IF NOT EXISTS idx_note_attachment_user ON note_attachment (user_id);"
                    )
                )
                await session.execute(
                    text(
                        "CREATE INDEX IF NOT EXISTS idx_note_attachment_operation ON note_attachment (operation_id);"
                    )
                )
    finally:
        await engine.dispose()


async def main() -> None:
    """Run the migration from command-line arguments."""
    parser = argparse.ArgumentParser(
        description="migrate normalized note storage (pre-v2.0.0 -> v2.0.0)",
    )
    parser.add_argument("--url", required=True, help="PostgreSQL connection URL")
    args = parser.parse_args()
    await migrate_note_text(args.url)


if __name__ == "__main__":
    asyncio.run(main())
