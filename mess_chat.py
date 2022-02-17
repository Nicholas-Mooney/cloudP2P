from sqlite3 import connect
from pydantic import BaseModel
from fastapi import FastAPI
import pika
from rmq import MessageServer

app = FastAPI()

recieved_message_list = []
sent_message_list = []
incoming_message_list = []
class Message(BaseModel):
    message: str

@app.get("/")
async def startup():
    return 'The API is currently running'

@app.post("/send/")
async def send(message : Message):
    connect = MessageServer()
    connect.send_message(message.message)
    sent_message_list.append(message)
    return {
        'message_count' : len(sent_message_list),
        'data' : sent_message_list
    }

@app.get("/messages/")
async def messages():
    connect = MessageServer()
    connect.receieve_messages(1)
    return incoming_message_list