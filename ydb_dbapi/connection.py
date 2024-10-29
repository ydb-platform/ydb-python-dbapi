class Connection:
    def __init__(self):
        pass

    def cursor(self):
        pass

    async def commit(self):
        pass

    async def rollback(self):
        pass

    async def close(self):
        pass


async def connect() -> Connection:
    return Connection()
