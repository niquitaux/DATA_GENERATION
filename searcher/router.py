from fastapi import FastAPI

from service import search_docs, generate_llm_response

app = FastAPI()


@app.get("/search")
async def search(query: str):
    rag_results = await search_docs(query)
    answer = await generate_llm_response(query, rag_results)

    return answer
