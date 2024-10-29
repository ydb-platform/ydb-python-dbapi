import pytest
import ydb_dbapi
import ydb_dbapi.cursors


@pytest.mark.asyncio
async def test_cursor_ddl(session_pool):
    cursor = ydb_dbapi.cursors.Cursor(session_pool=session_pool)

    yql = """
        CREATE TABLE table (
        id Int64 NOT NULL,
        val Int64,
        PRIMARY KEY(id)
        )
    """

    with pytest.raises(ydb_dbapi.Error):
        await cursor.execute(query=yql)

    yql = """
    DROP TABLE table
    """

    await cursor.execute(query=yql)

    assert await cursor.fetchone() is None


@pytest.mark.asyncio
async def test_cursor_dml(session_pool):
    cursor = ydb_dbapi.cursors.Cursor(session_pool=session_pool)
    yql_text = """
    INSERT INTO table (id, val) VALUES
    (1, 1),
    (2, 2),
    (3, 3)
    """

    await cursor.execute(query=yql_text)
    assert await cursor.fetchone() is None

    cursor = ydb_dbapi.cursors.Cursor(session_pool=session_pool)

    yql_text = """
    SELECT COUNT(*) FROM table as sum
    """

    await cursor.execute(query=yql_text)

    res = await cursor.fetchone()
    assert len(res) == 1
    assert res[0] == 3


@pytest.mark.asyncio
async def test_cursor_fetch_one(session_pool):
    cursor = ydb_dbapi.cursors.Cursor(session_pool=session_pool)
    yql_text = """
    INSERT INTO table (id, val) VALUES
    (1, 1),
    (2, 2)
    """

    await cursor.execute(query=yql_text)
    assert await cursor.fetchone() is None

    cursor = ydb_dbapi.cursors.Cursor(session_pool=session_pool)

    yql_text = """
    SELECT id, val FROM table
    """

    await cursor.execute(query=yql_text)

    res = await cursor.fetchone()
    assert res[0] == 1

    res = await cursor.fetchone()
    assert res[0] == 2

    assert await cursor.fetchone() is None


@pytest.mark.asyncio
async def test_cursor_fetch_many(session_pool):
    cursor = ydb_dbapi.cursors.Cursor(session_pool=session_pool)
    yql_text = """
    INSERT INTO table (id, val) VALUES
    (1, 1),
    (2, 2),
    (3, 3),
    (4, 4)
    """

    await cursor.execute(query=yql_text)
    assert await cursor.fetchone() is None

    cursor = ydb_dbapi.cursors.Cursor(session_pool=session_pool)

    yql_text = """
    SELECT id, val FROM table
    """

    await cursor.execute(query=yql_text)

    res = await cursor.fetchmany()
    assert len(res) == 1
    assert res[0][0] == 1

    res = await cursor.fetchmany(size=2)
    assert len(res) == 2
    assert res[0][0] == 2
    assert res[1][0] == 3

    res = await cursor.fetchmany(size=2)
    assert len(res) == 1
    assert res[0][0] == 4

    assert await cursor.fetchmany(size=2) is None


@pytest.mark.asyncio
async def test_cursor_fetch_all(session_pool):
    pass
