
from fastapi import FastAPI, Depends, HTTPException, Body
from contextlib import asynccontextmanager
from typing import Union, Optional, Annotated
from app import settings
from sqlmodel import Field, Session, SQLModel, create_engine, select
from typing import AsyncGenerator

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import asyncio
import json

from datetime import datetime, timezone

### ============================================================================================================= ###

# class Todo(SQLModel, table=True):
#     id: Optional[int] = Field(default=None, primary_key=True)
#     content: str = Field(index=True)

class TodoBaseModel(SQLModel):
    order_date: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class TodoDetail(SQLModel):
    content: str = Field(index=True)

class TodoModel(TodoBaseModel, TodoDetail):
    pass   

class Todo(TodoModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)

### ============================================================================================================= ###

# only needed for psycopg 3 - replace postgresql
# with postgresql+psycopg in settings.DATABASE_URL
connection_string = str(settings.DATABASE_URL).replace(
    "postgresql", "postgresql+psycopg"
)

# recycle connections after 5 minutes
# to correspond with the compute scale down
engine = create_engine(
    connection_string, connect_args={}, pool_recycle=300
)

### ============================================================================================================= ###

def create_db_and_tables()->None:
    SQLModel.metadata.create_all(engine)

### ============================================================================================================= ###

async def consume_messages(topic, bootstrap_servers):
    # Create a consumer instance.
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id="my-group",
        auto_offset_reset='earliest',
        enable_auto_commit=False,  # Disable auto commit to handle offsets manually
        session_timeout_ms=6000,   # Increase session timeout to avoid frequent rebalances
        heartbeat_interval_ms=2000 # Adjust heartbeat interval
    )

    print(f'listing .....consume_messages....todos ....')
    # Start the consumer.
    await consumer.start()
    try:
        # Continuously listen for messages.
        async for message in consumer:
            print(f"Consumer Received message: {message.value.decode()} on topic {message.topic}")
            # Here you can add code to process each message.
            # Example: Phrase the message, store it in a database, etc.
            await consumer.commit()  # Manually commit the offset after processing
    finally:
        # Ensure that the consumer is properly closed when done.
        await consumer.stop()  

### ============================================================================================================= ###

# The first part of the function, before the yield, will
# be executed before the application starts.
# https://fastapi.tiangolo.com/advanced/events/#lifespan-function
@asynccontextmanager
async def lifespan(app: FastAPI)-> AsyncGenerator[None, None]:
    print("todo.. lifespan ...kafka messages ........")
    task = asyncio.create_task(consume_messages('todos', 'broker:19092'))
    create_db_and_tables()
    yield

### ============================================================================================================= ###

# app = FastAPI(lifespan=lifespan, title="API with DB")    

app = FastAPI(lifespan=lifespan, title="API with DB", 
    version="0.0.1",
    servers=[
        {
            "url": "http://127.0.0.1:8001", # ADD NGROK URL Here Before Creating GPT Action
            "description": "Development Server"
        }
        ])

def get_session():
    with Session(engine) as session:
        yield session

### ============================================================================================================= ###

@app.get("/")
def read_root():
    return {"Kafka Messages": "Postgres DB"}

### ============================================================================================================= ###

# Kafka Producer as a dependency
async def get_kafka_producer():
    producer = AIOKafkaProducer(bootstrap_servers='broker:19092')
    print(f'KAFKA_PRODUCER......')
    await producer.start()
    try:
        yield producer
    finally:
        await producer.stop()

### ============================================================================================================= ###


DB_SESSION = Annotated[Session, Depends(get_session)]
KAFKA_PRODUCER = Annotated[AIOKafkaProducer, Depends(get_kafka_producer)]   

### ============================================================================================================= ###

"""
  Custom function to handle datetime objects during JSON serialization
  """
    
def convert_datetime_to_str(obj):
  """
  Custom function to handle datetime objects during JSON serialization
  """
  if isinstance(obj, datetime):
    return obj.isoformat()
  raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')

### ============================================================================================================= ###

## kafka messages for producer and consumer 

@app.post("/kafka_messages/", response_model=Todo)
async def create_todo_messages(todo_form: TodoDetail, session: DB_SESSION, producer: KAFKA_PRODUCER) -> Todo:
    print('Todos : KAFKA_PRODUCER ...')
    todo = Todo(**todo_form.model_dump())   
    todo_dict = {field: getattr(todo, field) for field in todo.dict()}

    print('todos_dict ...', todo_dict)

    # Correct the json.dumps call
    message = {
        "todos": todo_dict
    }
    todo_json = json.dumps(message, default=convert_datetime_to_str).encode("utf-8")
    
    print("todosJSON ", todo_json)

    # Produce message
    await producer.send_and_wait("todos", todo_json)
    session.add(todo)
    session.commit()
    session.refresh(todo)
    print('todos ..... ..', todo)
    return todo


### ============================================================================================================= ###

## kafka message for producer

@app.post("/api_add_todos/", response_model=Todo)
async def create_todo_api(todo_form: TodoDetail, session: DB_SESSION, producer: KAFKA_PRODUCER) -> Todo:
    print('Todo : KAFKA_PRODUCER ...')
    todo = Todo(**todo_form.model_dump())   
    todo_dict = {field: getattr(todo, field) for field in todo.dict()}

    print('todo_dict ...', todo_dict)

    # Correct the json.dumps call
    message = {
        "producer_response": todo_dict
    }
    todo_json = json.dumps(message, default=convert_datetime_to_str).encode("utf-8")
    
    print("todoJSON ", todo_json)

    # Produce message
    await producer.send_and_wait("producer_response", todo_json)
    session.add(todo)
    session.commit()
    session.refresh(todo)
    print('todo ..', todo)
    return todo

### ============================================================================================================= ###

@app.get("/api_todos/", response_model=list[Todo])
def read_todos(session: Annotated[Session, Depends(get_session)]):
        todos = session.exec(select(Todo)).all()
        return todos

### ============================================================================================================= ###



### ============================================================================================================= ###

















