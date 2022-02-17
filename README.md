# Basic Message Chat
This is a basic implementation of a distributed message chat server, rather than a point to point version.

## Requirements
* Pika, FastAPI, and RabbitMQ required on the machine
    * ```pip install -r requirements.txt``` to install required libraries
    * [RabbitMQ Installation Guide](https://www.rabbitmq.com/download.html)

## To Run
* ```python -m uvicorn mess_chat:app --reload```

## Libraries Used
* [Python Pika](https://pypi.org/project/pika/#:~:text=Pika%20is%20a%20RabbitMQ%20%28AMQP%200-9-1%29%20client%20library,RabbitMQ%E2%80%99s%20extensions.%20Python%202.7%20and%203.4%2B%20are%20supported.)
* [Python Pydantic](https://pydantic-docs.helpmanual.io/)

## Microservices / API's
* [RabbitMQ](https://www.rabbitmq.com/#features)
* [FastAPI](https://fastapi.tiangolo.com/)

## Useful Information
* [RabbitMQ Introduction](https://www.rabbitmq.com/tutorials/tutorial-one-python.html)
* [Pika Introduction](https://pika.readthedocs.io/en/stable/intro.html)
* [Remote Procedure Call w/ Pika](https://www.rabbitmq.com/tutorials/tutorial-six-python.html)
* [Connection with RabbitMQ](https://www.cloudamqp.com/blog/how-to-run-rabbitmq-with-python.html)
* [Channel Basics](https://www.rabbitmq.com/channels.html#basics)

## Tutorial Videos
* [Sample FastAPI Post](https://www.youtube.com/watch?v=wS9LfFtXdBs&ab_channel=codeme)
* [RabbitMQ Basics](https://www.youtube.com/watch?v=Cie5v59mrTg&ab_channel=HusseinNasser)