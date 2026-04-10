"""
Database connection management for future_trade.
Provides connection pooling and context managers for safe PostgreSQL access.
"""
from contextlib import contextmanager
from typing import Generator
import psycopg2
from psycopg2 import pool
from psycopg2.extensions import connection
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from config.postgres_config import get_settings

_connection_pool: pool.ThreadedConnectionPool | None = None
_engine: Engine | None = None
_fcta_pool: pool.ThreadedConnectionPool | None = None


def _get_pool() -> pool.ThreadedConnectionPool:
    """Get or create the connection pool."""
    global _connection_pool
    if _connection_pool is None:
        settings = get_settings()
        _connection_pool = pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=10,
            host=settings.pg_host,
            port=settings.pg_port,
            database=settings.pg_database,
            user=settings.pg_user,
            password=settings.pg_password,
        )
    return _connection_pool


@contextmanager
def get_connection() -> Generator[connection, None, None]:
    """Get a database connection from the pool."""
    conn = None
    try:
        conn = _get_pool().getconn()
        yield conn
        conn.commit()
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            _get_pool().putconn(conn)


def get_engine() -> Engine:
    """Get a SQLAlchemy engine for pandas to_sql operations."""
    global _engine
    if _engine is None:
        settings = get_settings()
        _engine = create_engine(settings.database_url)
    return _engine


@contextmanager
def get_fcta_connection() -> Generator[connection, None, None]:
    """Get a connection to the fcta database (for equity data)."""
    global _fcta_pool
    if _fcta_pool is None:
        _fcta_pool = pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=10,
            host="127.0.0.1",
            port=5432,
            database="fcta",
            user="postgres",
            password="161514",
        )
    conn = None
    try:
        conn = _fcta_pool.getconn()
        yield conn
        conn.commit()
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            _fcta_pool.putconn(conn)


def close_pool() -> None:
    """Close the connection pool."""
    global _connection_pool, _engine, _fcta_pool
    if _connection_pool:
        _connection_pool.closeall()
        _connection_pool = None
    if _engine:
        _engine.dispose()
        _engine = None
    if _fcta_pool:
        _fcta_pool.closeall()
        _fcta_pool = None
