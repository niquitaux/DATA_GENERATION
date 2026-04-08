import faiss
import json
import numpy as np
import httpx
import boto3
import asyncio
from config import YANDEX_EMBEDDING_URL, YANDEX_LLM_URL, headers, query_payload, data, MINIO_URL, MINIO_KEY_ID, MINIO_ACCESS_KEY


client = httpx.AsyncClient()
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_URL,
    aws_access_key_id=MINIO_KEY_ID,
    aws_secret_access_key=MINIO_ACCESS_KEY,
)

async def data_loader():  # TODO вот это вот на бд заменить
    with open("synthetic_data.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    dataset_name = data["dataset_name"]
    language = data["language"]
    documents = data["documents"]

    return documents


async def search_docs(query, k=3):
    resp = await asyncio.to_thread(
        s3.get_object, Bucket="faiss-index", Key="index.faiss"
    )
    buf = np.frombuffer(resp["Body"].read(), dtype=np.uint8)
    index = faiss.deserialize_index(buf)

    query_payload["text"] = query
    response = await client.post(YANDEX_EMBEDDING_URL, headers=headers, json=query_payload)

    query_emb = response.json()["embedding"]
    query_np = np.array([query_emb]).astype('float32')
    distances, indices = index.search(query_np, k)

    documents = await data_loader()

    results = []
    for i, idx in enumerate(indices[0]):
        results.append({
            "doc": documents[idx],
            "distance": float(distances[0][i])
        })

    return results


async def generate_llm_response(query, rag_results):
    context = []
    for i, doc in enumerate(rag_results):
        context.append(f"""[{i+1}] document_id: {doc['doc']['document_id']}
                       [text] {doc['doc']['text']}""")
    context = '\n\n'.join(context)

    system_prompt = (
        f"""Ты ассистент для работы с юридическими документами. Ответь на вопрос пользователя, используя следующий контекст:
        В конце ответа укажи номера источников в формате [1] [document_id], [2] [document_id] и т.д.\n\n
        Контекст:\n{context}"""
    )

    data['input'] = query
    data['instructions'] = system_prompt
    response = await client.post(YANDEX_LLM_URL, headers=headers, json=data)
    result = response.json()
    return result["output"][0]["content"][0]["text"]