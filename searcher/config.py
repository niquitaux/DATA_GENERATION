import os

YANDEX_API_KEY = os.getenv('YANDEX_API_KEY')
YANDEX_FOLDER_ID = os.getenv('YANDEX_FOLDER_ID')

YANDEX_EMBEDDING_URL = os.getenv('YANDEX_EMBEDDING_URL')
YANDEX_LLM_URL = os.getenv('YANDEX_LLM_URL')
YANDEX_LLM_MODEL = os.getenv('YANDEX_LLM_MODEL')

headers = {
    "Authorization": f"Api-Key {YANDEX_API_KEY}",
    "Content-Type": "application/json",
}

query_payload = {
        "modelUri": f"emb://{YANDEX_FOLDER_ID}/text-search-query/latest"
    }

data = {
        "model": f"gpt://{YANDEX_FOLDER_ID}/{YANDEX_LLM_MODEL}",
        "temperature": 0.5,  # TODO put params in yaml mb
        "max_output_tokens": 1500
    }

MINIO_URL = "http://minio:9000"
MINIO_KEY_ID = "minioadmin"  # TODO should be in .env file but nvm
MINIO_ACCESS_KEY = "minioadmin"
