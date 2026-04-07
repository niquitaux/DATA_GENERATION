import streamlit as st
import httpx


SEARCHER_URL = "http://searcher:8000/search"


st.title("data_generation test_task")


if "messages" not in st.session_state:
    st.session_state.messages = []


for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])


if prompt := st.chat_input("What is up?"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    st.chat_message("user").markdown(prompt)

    try:
        response = httpx.get(SEARCHER_URL, params={"query": prompt}, timeout=60.0)
        response.raise_for_status()
        answer = response.json()
        response_text = answer.get("output", str(answer)) if isinstance(answer, dict) else str(answer)
    except Exception as e:
        response_text = f"Error: {e}"

    st.chat_message("assistant").markdown(response_text)
    st.session_state.messages.append({"role": "assistant", "content": response_text})