# OUTDATED, EMBEDDER CALLED BY KAFKA NOW DIRECTLY FROM SERVICE.PY
from fastapi import FastAPI

from service import data_loader, build_index

app = FastAPI()

@app.post("/embed")  # стартует создание эмбеддингов при пинге, надо чтобы принимал данные из другого секрвиса
async def embed():
    data = await data_loader()
    index = await build_index(data)
    return index
