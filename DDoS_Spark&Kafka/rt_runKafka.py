import subprocess
import time
import json
import threading
from confluent_kafka import Producer, Consumer
from rt_producer import read_packets_from_pcap, process_packets
#from read_csvfile import read_packets_from_csv, process_packets


def start_zookeeper():
    print("Starting Zookeeper...")
    try:
        subprocess.Popen(["/Users/jejebubu/Desktop/bigproject/kafka/bin/zookeeper-server-start.sh", "/Users/jejebubu/Desktop/bigproject/kafka/config/zookeeper.properties"])
    except Exception as e:
        print(f"Error starting Zookeeper: {e}")

def start_kafka_broker():
    print("Starting Kafka broker...")
    try:
        subprocess.Popen(["/Users/jejebubu/Desktop/bigproject/kafka/bin/kafka-server-start.sh", "/Users/jejebubu/Desktop/bigproject/kafka/config/server.properties"])
    except Exception as e:
        print(f"Error starting Kafka broker: {e}")

def run_producer(packets):
    print("Running producer...")
    producer = Producer({
        'bootstrap.servers': 'localhost:9092',
        'client.id': 'my_producer',
        'acks': 'all'
    })

    def delivery_callback(err, msg):
        if err:
            print(f'Failed to deliver message: {err}')
        else:
            print(f'Message delivered to topic {msg.topic()} partition {msg.partition()} offset {msg.offset()}')

    while True:
        for packet_info in packets:
            producer.produce('capture-packet', key=None, value=json.dumps(packet_info).encode('utf-8'), callback=delivery_callback)
            print(f'Produced message: {packet_info}')
            producer.flush()
            time.sleep(1)

def run_consumer():
    print("Running consumer...")
    try:
        consumer = Consumer({
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'packet_consumer_group',
            'auto.offset.reset': 'earliest'  # Start reading from the beginning of the topic
        })
        consumer.subscribe(['capture-packet'])

        while True:
            msg = consumer.poll(1.0)  # Poll for messages, with a timeout of 1 second
            if msg is None:
                continue
            if msg.error():
                print(f'Error consuming message: {msg.error()}')
                continue
            print(f'Message received: {msg.value().decode("utf-8")}')

    except Exception as e:
        print(f"Error running consumer: {e}")
    finally:
        consumer.close()

def countdown(delay):
    for i in range(delay, 0, -1):
        print(f"Starting in {i} seconds...")
        time.sleep(1)

def main():
    pcap_file_path = '/Users/jejebubu/Desktop/bigproject/envkafka/rt_capture_file.pcapng'
    #pcap_file_path = '/Users/jejebubu/Desktop/bigproject/envkafka/20.pcap'
    #csv_file_path = '/Users/jejebubu/Desktop/bigproject/envkafka/test_file.csv'
    
    start_zookeeper()
    countdown(10)
    
    start_kafka_broker()
    countdown(10)

    # Read packet information from the pcap file
    raw_packets = read_packets_from_pcap(pcap_file_path)
    #raw_packets = read_packets_from_csv(csv_file_path)
    
    # Process the raw packets
    packets = process_packets(raw_packets)

    # Countdown before starting the producer and consumer
    delay = 10
    print(f"Waiting for {delay} seconds before starting...")
    
    # Start producer and consumer in separate threads
    producer_thread = threading.Thread(target=run_producer, args=(packets,))
    consumer_thread = threading.Thread(target=run_consumer)

    producer_thread.start()
    consumer_thread.start()

    producer_thread.join()
    consumer_thread.join()

if __name__ == "__main__":
    main()
