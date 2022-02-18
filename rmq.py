#1connection | to his server
#2channels | general & private
#2queues | general & private
#1exchange ? fanout

#set names at begginning of code path
#dont do if public else if private
#DONT DO CALLBACKS



'''
make a public and private
private channel for yourself
public called general

channel() = queues are named
channel() =

private
connection is same
channel bind to private queue
no exchange for private channel
channel . queue declare

public
connection is same
channel bind to public queue
declare exchange
channel . consume / send

#send to general channel
#get from any queue
basic publish
    exchange    |type fanout
    routing key = queue name ||which is ...
    message
our channel is "general"
and channel is "privateKey"
'''

#message limits
#gereator nack


#channel . consume
#default get all messages -1
#auto ack | target channel cancel for no ack
#for messages in channel nummessages consume
#use consume not basic consume

from gc import callbacks
import pika
import logging
import functools

RMQ_HOST = "cps-devops.gonzaga.edu" #"http://cps-devops.gonzaga.edu""localhost"
RMQ_PORT = 5672
RMQ_USERNAME = "class" #"class""guest"
RMQ_PASSWORD = "CPSC313" #"CPSC313""guest"
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
            exchange = 'general',
            exchange_type = 'fanout',
            )
        self._channel.queue_declare(queue = '')
        self._channel.queue_bind('general', 'general', routing_key='general')
        self._channel.basic_qos(prefetch_count= 1)
        #self._consumer_tag = self._channel.basic_consume(queue = RMQ_DEFAULT_PUBLIC_QUEUE, on_message_callback = self.on_message)


    def connection_close(self):
        self._channel = None
        self._connection.close()


    def acknowledge_message(self, delivery_tag):
        self._channel.basic_ack(delivery_tag=delivery_tag)

    def publish_message(self, message):
        self._channel.basic_publish(exchange = 'general', routing_key = 'message', body = message)
        print(f' [x] {message} was sent to the MQ')
        self.connection_close()


    def consume_message(self):
        messages = ['1']
        self._channel.basic_consume(
            queue = RMQ_DEFAULT_PUBLIC_QUEUE,
            on_message_callback = self.consume_callback,
            auto_ack = True)
        print(RMQ_DEFAULT_PUBLIC_QUEUE)
        print(' [*] Waiting for messages...')
        for frame, prop, body in self._channel.consume('general'):
            print("message: ", end="")
            print(body)
            messages.append(body)
        print("awd" + str(messages))
        return messages

class MessageServer():
    def __init__(self) -> None:
        self._server = RMQPublisher()
        self._connection = self._server.establish_connection()
        print('Connection Successful.')
    def send_messages(self, message_content: str) -> bool:
        #self._server.publish_message(message_content)
        for i in range(100):
            self.send_message(i)
        pass

    def send_message(self, message_content: str) -> bool:
        #self._server.publish_message(message_content)
        credentials = pika.PlainCredentials(
            RMQ_USERNAME,
            RMQ_PASSWORD)
        connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    RMQ_HOST,
                    RMQ_PORT,
                    "/",
                    credentials=credentials))
        channel = connection.channel()
        channel.queue_declare(queue='general')
        channel.basic_publish(exchange='', routing_key='general', body='Hello World!')
        print(" [x] Sent 'Hello World!'")
        connection.close()
        pass

    def receieve_messages(self, num_messages: int = GET_ALL_MESSAGES) -> list:
        #return self._server.consume_message()\
        message_list = []
        credentials = pika.PlainCredentials(
            RMQ_USERNAME,
            RMQ_PASSWORD)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                RMQ_HOST,
                RMQ_PORT,
                "/",
                credentials=credentials))

        channel = connection.channel()
        channel.queue_declare(queue='general')
        for method_frame,prop,body in channel.consume(queue='general', auto_ack=True, inactivity_timeout=0.5):
            print(" [x] Received %r" % body)
            message_list.append(body)
            if method_frame.delivery_tag == 10:
                break

        requeued_messages = channel.cancel()
        channel.close()
        connection.close()
        print('Requeued %i messages' % requeued_messages)
        return message_list

def main():
    #logging.basicConfig(filename='mess_chat.log', level=logging.DEBUG, format=LOG_FORMAT, filemode='w')
    example = MessageServer()

if __name__ == '__main__':
    main()