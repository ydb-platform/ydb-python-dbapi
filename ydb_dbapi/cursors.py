class Cursor:
    def __init__(self):
        self.arraysize = 1

    async def execute(self):
        pass

    async def executemany(self):
        pass

    async def fetchone(self):
        pass

    async def fetchmany(self):
        pass

    async def fetchall(self):
        pass

    async def nextset(self):
        pass

    def setinputsizes(self):
        pass

    def setoutputsize(self):
        pass

    async def close(self):
        pass

    @property
    def description(self):
        pass

    @property
    def rowcount(self):
        pass
