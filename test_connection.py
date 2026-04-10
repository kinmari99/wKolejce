import pyodbc
from db import get_engine
from sqlalchemy import text

print("Sterowniki ODBC widziane przez pyodbc:")
print(pyodbc.drivers())

engine = get_engine()

with engine.connect() as conn:
    result = conn.execute(text("SELECT DB_NAME()"))
    print("Połączono z bazą:", result.scalar())