#1connection
#2channels
#2queues
#1exchange

from gc import callbacks

import pika
import logging
import functools

RMQ_HOST = 'localhost' #"http://cps-devops.gonzaga.edu""localhost"
RMQ_PORT = 5672
RMQ_USERNAME = "guest" #"class""guest"
RMQ_PASSWORD = "guest" #"CPSC313""guest"
RMQ_DEFAULT_VH = "/"

GET_ALL_MESSAGES = -1
RMQ_TEST_HOST = 'localhost'

RMQ_DEFAULT_PUBLIC_QUEUE = 'general'
RMQ_DEFAULT_PRIVATE_QUEUE = ''

RMQ_DEFAULT_EXCHANGE_NAME = 'general'
RMQ_DEFAULT_EXCHANGE_TYPE = 'fanout'

LOG_FORMAT = '%(asctime)s -- %(levelname)s -- %(message)s'
LOGGER = logging.getLogger(__name__)

class RMQPublisher():
    def __init__(self) -> None:
        self._connection = None
        self._channel = None
        self._consumer_tag = None

    def establish_connection(self) -> None:
        credentials = pika.PlainCredentials(RMQ_USERNAME, RMQ_PASSWORD)
        parameters = pika.ConnectionParameters(host=RMQ_HOST, port=RMQ_PORT,
            virtual_host=RMQ_DEFAULT_VH, credentials=credentials, heartbeat=6000)
        self._connection = pika.BlockingConnection(parameters=parameters)
        self.connection_open()
    def connection_open(self):
        self.open_channel()

    def open_channel(self):
        self._connection.channel()
        self._channel = self._connection.channel()
        self.setup_exchange(RMQ_DEFAULT_EXCHANGE_NAME)

    def setup_exchange(self, exchange_name):
        self._channel.exchange_declare(
            exchange = exchange_name,
            exchange_type = RMQ_DEFAULT_EXCHANGE_TYPE,
            )
        self._channel.queue_declare(queue = RMQ_DEFAULT_PUBLIC_QUEUE)
        self._channel.queue_bind(RMQ_DEFAULT_PUBLIC_QUEUE, RMQ_DEFAULT_EXCHANGE_NAME, routing_key='hello')
        self._channel.basic_qos(prefetch_count= 1)
        self._consumer_tag = self._channel.basic_consume(queue = RMQ_DEFAULT_PUBLIC_QUEUE, on_message_callback = self.on_message)
    def connection_close(self):
        self._channel = None
        self._connection.close()



    def on_message(self, _unused, basic_deliver, properties, body):
        self.acknowledge_message(basic_deliver.delivery_tag)
    def acknowledge_message(self, delivery_tag):
        self._channel.basic_ack(delivery_tag=delivery_tag)


    def publish_message(self, message):
        self._channel.basic_publish(exchange = '', routing_key = 'message', body = message)
        print(f' [x] {message} was sent to the MQ')
        self.connection_close()

    def consume_callback(self, method, properties, body):
        print(" [x] Received %r" % body)
    def consume_message(self):
        self._channel.basic_consume(
            queue = RMQ_DEFAULT_PUBLIC_QUEUE,
            on_message_callback = self.consume_callback,
            auto_ack = True)
        print(RMQ_DEFAULT_PUBLIC_QUEUE)
        print(' [*] Waiting for messages...')
        self._channel.start_consuming()
        self.connection_close()


class MessageServer():
    def __init__(self) -> None:
        self._server = RMQPublisher()
        self._connection = self._server.establish_connection()
        print('Connection Successful.')

    def send_message(self, message_content: str) -> bool:
        self._server.publish_message(message_content)
        pass
    def receieve_messages(self, num_messages: int = GET_ALL_MESSAGES) -> list:
        self._server.consume_message()
        pass

def main():
    logging.basicConfig(filename='mess_chat.log', level=logging.DEBUG, format=LOG_FORMAT, filemode='w')
    example = MessageServer()

if __name__ == '__main__':
    main()