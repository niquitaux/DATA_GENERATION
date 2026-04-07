import os

YANDEX_API_KEY = os.getenv('YANDEX_API_KEY')
YANDEX_FOLDER_ID = os.getenv('YANDEX_FOLDER_ID')
YANDEX_EMBEDDING_URL = os.getenv('YANDEX_EMBEDDING_URL')

headers = {
    "Authorization": f"Api-Key {YANDEX_API_KEY}",
    "Content-Type": "application/json",
}

payload = {
    "modelUri": f"emb://{YANDEX_FOLDER_ID}/text-search-doc/latest",
}