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

#set params for queue
#send message
#num messages
#queue name

@app.get("/")
async def startup():
    return 'The API is currently running'

@app.get("/send1/")
async def send():
    connect = MessageServer()
    connect.send_messages("test")
    sent_message_list.append(len(sent_message_list))
    return {
        'message_count' : len(sent_message_list),
        'data' : sent_message_list
    }

@app.get("/send/")
async def send():
    connect = MessageServer()
    connect.send_message("test")
    sent_message_list.append(len(sent_message_list))
    return {
        'message_count' : len(sent_message_list),
        'data' : sent_message_list
    }

@app.get("/messages/")
async def messages():
    connect = MessageServer()
    return connect.receieve_messages(num_messages=1)