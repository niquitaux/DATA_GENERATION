from aiokafka import AIOKafkaProducer
import json
import asyncio

KAFKA = "kafka:9092"
TOPIC = "documents-to-embed"


async def main():
    with open("synthetic_data.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    producer = AIOKafkaProducer(bootstrap_servers=KAFKA)
    await producer.start()
    try:
        for doc in data["documents"]:
            await producer.send_and_wait(TOPIC, json.dumps(doc).encode())
            print(f"Sent: {doc.get('document_id')}")
    finally:
        await producer.stop()

if __name__ == "__main__":
    print("producer producing")
    asyncio.run(main()) 