from asyncpg import connect

from django.conf import settings


async def conn():
    con = await connect(user=settings.DATABASE_USER,
        password=settings.DATABASE_PASSWORD, database=settings.DATABASE_NAME,
        host=settings.DATABASE_HOST, statement_cache_size=2000)
    return con