
from fastapi import FastAPI
from fastapi.responses import JSONResponse
import sqlite3

app = FastAPI()

@app.get("/obitos")
def get_obitos():
    conn = sqlite3.connect("afogamentos.db")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM obitos")
    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    conn.close()

    result = [dict(zip(columns, row)) for row in rows]
    return JSONResponse(content=result)
