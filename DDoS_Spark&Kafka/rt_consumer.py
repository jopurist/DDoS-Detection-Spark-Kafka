from kafka import KafkaConsumer
import json

def consume_packets(consumer, num_messages, output_file):
    received_packets = []
    
    while True:
        message = consumer.poll(timeout_ms=1000)
        
        if message is None:
            print("No message received")
            continue
        
        for tp, msgs in message.items():
            for msg in msgs:
                packet_info = json.loads(msg.value.decode('utf-8'))
                received_packets.append(packet_info)
                print(f"Received packet: {packet_info}")  # Log received packets
    
        print(f"Total received packets: {len(received_packets)}")
    
        # Write received packets to the output file
        print(f"Saving {len(received_packets)} packets to {output_file}")
    
        try:
            with open(output_file, 'w') as f:
                json.dump(received_packets, f, indent=4)
            print(f"Successfully saved {len(received_packets)} packets to {output_file}")
        except Exception as e:
            print(f"Error saving packets to {output_file}: {e}")


# Kafka configuration
bootstrap_servers = 'localhost:9092'
group_id = 'packet_consumer_group'
topic_name = 'capture-packet'

# Output file to save the packets
output_file = '/Users/jejebubu/Desktop/bigproject/envkafka/rt_received_packets.json'

# Create KafkaConsumer instance
consumer = KafkaConsumer(topic_name,
                         group_id=group_id,
                         bootstrap_servers=bootstrap_servers,
                         auto_offset_reset='earliest')

# Consume packets from the Kafka topic and save to file
consume_packets(consumer, num_messages=5, output_file=output_file)
