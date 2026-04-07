from aiokafka import AIOKafkaConsumer
import asyncio
import httpx
import json
import faiss 
import numpy as np
from config import YANDEX_EMBEDDING_URL, headers, payload


client = httpx.AsyncClient()
semaphore = asyncio.Semaphore(8)
KAFKA = "kafka:9092"
TOPIC = "documents-to-embed"


async def data_loader():
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA,
        group_id="embedder-group",
        auto_offset_reset="earliest",
        retry_backoff_ms=2000
    )
    await consumer.start()

    documents = []
    try:
        async for msg in consumer:
            doc = json.loads(msg.value)
            documents.append(doc)

            if consumer.assignment():
                end_offsets = await consumer.end_offsets(consumer.assignment())
                current = {tp: await consumer.position(tp) for tp in consumer.assignment()}
                if all(current[tp] >= end_offsets[tp] for tp in consumer.assignment()):
                    break
    finally:
        await consumer.stop()

    doc_texts = [await enrich_text(doc) for doc in documents]
    return doc_texts


async def build_index(doc_texts):
    tasks = [get_yandex_embedding(text) for text in doc_texts]

    embeddings = await asyncio.gather(*tasks)
    embeddings_np = np.array(embeddings).astype('float32')
    embeddings = [e for e in embeddings if e is not None]
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
        return None
        

async def main():
    documents = await data_loader()
    index_path = await build_index(documents)
    print(f"Index built at {index_path}")

if __name__ == "__main__":
    asyncio.run(main()) 