import asyncio
import httpx
import json
import faiss 
import numpy as np
from config import YANDEX_EMBEDDING_URL, headers, payload


client = httpx.AsyncClient()
semaphore = asyncio.Semaphore(8)


async def data_loader():  # вот это вот на бд заменить
    with open("synthetic_data.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    dataset_name = data["dataset_name"]
    language = data["language"]
    documents = data["documents"]

    doc_texts = [await enrich_text(doc) for doc in documents]

    return doc_texts


async def build_index(doc_texts):
    tasks = [get_yandex_embedding(text) for text in doc_texts]

    embeddings = await asyncio.gather(*tasks)
    embeddings_np = np.array(embeddings).astype('float32')
    dimension = embeddings_np.shape[1]
    
    index = faiss.IndexFlatL2(dimension)
    index.add(embeddings_np)

    index_path = "/faiss_data/index.faiss"
    faiss.write_index(index, index_path)

    return index_path


async def enrich_text(doc):  # объединяет все поля, возможны другие методы индексации
    return f"Заголовок: {doc.get('title', '')}. Департамент: {doc.get('department', '')}. Теги: {', '.join(doc.get('tags', []))}. Содержание: {doc.get('text', '')}"


async def get_yandex_embedding(text, retries=3):  # TODO посмотреть работает ли яндекс апи батчами
    async with semaphore:
        for attempt in range(retries):

            payload["text"] = text
            response = await client.post(YANDEX_EMBEDDING_URL, headers=headers, json=payload)
            data = response.json()

            if "embedding" in data:
                return data["embedding"]
            
            wait = 2 ** attempt
            await asyncio.sleep(wait)
        
