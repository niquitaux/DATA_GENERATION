FROM python:3.11-slim

WORKDIR /frontend

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["streamlit", "run", "main.py", "--server.port", "8002", "--server.address", "0.0.0.0"]
