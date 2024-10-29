import os
import pytest
import time
import ydb


@pytest.fixture(scope="module")
def docker_compose_file(pytestconfig):
    return os.path.join(str(pytestconfig.rootdir), "docker-compose.yml")


def wait_container_ready(driver):
    driver.wait(timeout=30)

    with ydb.SessionPool(driver) as pool:
        started_at = time.time()
        while time.time() - started_at < 30:
            try:
                with pool.checkout() as session:
                    session.execute_scheme(
                        "create table `.sys_health/test_table` "
                        "(A int32, primary key(A));"
                    )

                return True

            except ydb.Error:
                time.sleep(1)

    raise RuntimeError("Container is not ready after timeout.")


@pytest.fixture(scope="module")
def endpoint(pytestconfig, module_scoped_container_getter):
    with ydb.Driver(endpoint="localhost:2136", database="/local") as driver:
        wait_container_ready(driver)
    yield "localhost:2136"


@pytest.fixture(scope="module")
def database():
    return "/local"


@pytest.fixture()
async def driver(endpoint, database, event_loop):
    driver_config = ydb.DriverConfig(
        endpoint,
        database,
    )

    driver = ydb.aio.Driver(driver_config=driver_config)
    await driver.wait(timeout=15)

    yield driver

    await driver.stop(timeout=10)
    del driver


@pytest.fixture
async def session_pool(driver: ydb.aio.Driver):
    session_pool = ydb.aio.QuerySessionPool(driver)
    async with session_pool:
        await session_pool.execute_with_retries(
            """DROP TABLE IF EXISTS table"""
        )
        await session_pool.execute_with_retries(
            """
            CREATE TABLE table (
            id Int64 NOT NULL,
            val Int64,
            PRIMARY KEY(id)
            )
            """
        )

        yield session_pool
