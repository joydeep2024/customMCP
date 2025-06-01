from confluent_kafka import Producer,Consumer
import threading
import json
import time
import uuid

BOOTSTRAP_SERVERS = 'hostname:9092'

FROM_TOPIC = "test_from_topic"
TO_TOPIC = "test_to_topic"

class kafka_transport:
    def __init__(self, bootstrap_servers,from_topic, to_topic):
        self.id = uuid.uuid4()
        self.bootstap_servers = bootstrap_servers
        self.from_topics = [from_topic]
        self.to_topic = to_topic

        conf = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': "kafka_transport_group",  # TODO: check whether consumer group needs to be unique as we may not want to re read data
            'auto.offset.reset': 'latest'
        }
        
        self.consumer = Consumer(conf)
        self.producer = Producer({'bootstrap.servers': bootstrap_servers})

        self.response_received = threading.Event()
        self.response_data = None
        self.request_id_counter = 0

        threading.Thread(target=self._consumer_listener, daemon=True).start()
        time.sleep(1)

    def _kafka_write(self, message):
        serialized_message = json.dumps(message)
        #print(serialized_message)

        print(f"Thread clear for : {self.id}")
        self.response_received.clear()

        self.producer.produce(self.to_topic, serialized_message.encode())
        self.producer.flush()

    def _kafka_read(self):
        # Wait for response
        print("Waiting for response...")
        if self.response_received.wait(): ## there should be a timeout based upon whether it is waiting for a response or waiting for a message
            print("Response received")
        else:
            print("Timeout waiting for response.")
        try:                       
            # Read the JSON content
            response = self.response_data
            self.response_received.clear()
            return response
        except Exception as e:
            print(f"Error receiving or parsing response: {e}")
            raise

    def _consumer_listener(self):
        self.consumer.subscribe(self.from_topics)
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("Error: ", msg.error())
                continue

            response = json.loads(msg.value().decode())
            #print(f"Response: {response}")
            self.response_data = response
            print(f"Thread set for : {self.id}")
            self.response_received.set()

    def _handle_exception(self):
        self.response_received.clear()
                
    

if __name__ == '__main__':
     server_transport = kafka_transport(BOOTSTRAP_SERVERS,TO_TOPIC,TO_TOPIC)
     server_transport.request_id_counter = 1
     test_json =  {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "test"
        }
     server_transport._kafa_write(test_json)
     response = server_transport._kafka_read()
     print(response)
      