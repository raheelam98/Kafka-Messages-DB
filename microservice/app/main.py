# main.py
from fastapi import FastAPI
from contextlib import asynccontextmanager
from typing import AsyncGenerator
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import asyncio
import json

from typing import Optional
from sqlmodel import SQLModel , Field

### ============================================================================================================= ###

class Todo(SQLModel , table = True):
    id : Optional[int] = Field(default=None , primary_key=True)
    content : str

### ============================================================================================================= ###


async def consume_messages(topic, bootstrap_servers):
    # Create a consumer instance.
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id="my-todos-group",
        auto_offset_reset='earliest'
    )

    print("Creating Kafka Consumer...")

    # Start the consumer.
    await consumer.start()
    try:
        # Continuously listen for messages.
        async for message in consumer:
            print("Listening...")
            print(f"Received message: {message.value.decode()} on topic {message.topic}")
            # Here you can add code to process each message.
            # Example: parse the message, store it in a database, etc.
    finally:
        # Ensure to close the consumer when done.
        await consumer.stop()

### ============================================================================================================= ###

# The first part of the function, before the yield, will
# be executed before the application starts.
# https://fastapi.tiangolo.com/advanced/events/#lifespan-function
# loop = asyncio.get_event_loop()
@asynccontextmanager
async def lifespan(app: FastAPI)-> AsyncGenerator[None, None]:
    print("microservice ...lifespan..")
    task = asyncio.create_task(consume_messages('todos', 'broker:19092'))
    yield

### ============================================================================================================= ###

app = FastAPI(lifespan=lifespan, title="Hello World API with DB", 
    version="0.0.1",
    servers=[
        {
            "url": "http://127.0.0.1:8002", # ADD NGROK URL Here Before Creating GPT Action
            "description": "Development Server"
        },{
            "url": "http://127.0.0.1:8001", # ADD NGROK URL Here Before Creating GPT Action
            "description": "Development Server"
        }
        ])


app = FastAPI()

### ============================================================================================================= ###


# Kafka Producer as a dependency
async def get_kafka_producer():
    producer = AIOKafkaProducer(bootstrap_servers='broker:19092')
    await producer.start()
    try:
        yield producer
    finally:
        await producer.stop()
   

### ============================================================================================================= ###


@app.get('/')
def get_root():
    return 'kafka micro-service'

### ============================================================================================================= ###

        