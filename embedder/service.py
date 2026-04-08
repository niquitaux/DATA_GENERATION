from aiokafka import AIOKafkaConsumer
import asyncio
import httpx
import json
import faiss 
import numpy as np
import boto3
from config import YANDEX_EMBEDDING_URL, headers, payload, KAFKA, TOPIC, SEMAPHORE_SIZE, MINIO_URL, MINIO_KEY_ID, MINIO_ACCESS_KEY


client = httpx.AsyncClient()
semaphore = asyncio.Semaphore(SEMAPHORE_SIZE)
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_URL,
    aws_access_key_id=MINIO_KEY_ID,
    aws_secret_access_key=MINIO_ACCESS_KEY,
)


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
                    break  # TODO нужен рефактор, чтобы контейнер не падал, но пока лениво думать про логику обработки потенциальных новых данных
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

    buf = faiss.serialize_index(index)
    s3.put_object(
        Bucket="faiss-index",
        Key="index.faiss",
        Body=buf.tobytes(),
    )

    return True


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
    print("Index uploaded to MinIO")
    

if __name__ == "__main__":
    asyncio.run(main()) 