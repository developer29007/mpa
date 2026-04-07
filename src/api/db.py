import os
from typing import Generator

import psycopg
from fastapi import Request


def get_conn(request: Request) -> Generator[psycopg.Connection, None, None]:
    conn: psycopg.Connection = request.app.state.db
    try:
        yield conn
    finally:
        conn.rollback()


def open_connection() -> psycopg.Connection:
    dsn = os.environ["MPA_DSN"]
    return psycopg.connect(dsn, autocommit=False)
