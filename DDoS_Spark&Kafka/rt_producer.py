from kafka import KafkaProducer
import pyshark
import msgpack
import logging

logging.basicConfig(level=logging.INFO)


def read_packets_from_pcap(pcap_file_path):
    try:
        logging.info("Reading packets from pcap file...")
        capture = pyshark.FileCapture(pcap_file_path)
        packets = [pkt for pkt in capture]
        logging.info(f"Read {len(packets)} packets from pcap file.")
        return packets
    except Exception as e:
        logging.error(f"Error reading packets from pcap file: {e}")
        return []

def process_packets(packets):
    logging.info("Processing packets...")
    processed_packets = []
 
     # Initialize counters
    tx_packets = 0
    tx_bytes = 0
    rx_packets = 0
    rx_bytes = 0
    
       
    for pkt in packets:
        if 'IP' in pkt:
            try:
                # Initialize packet info dictionary
                
                src_message = {}
                dst_message = {}
                
                packet_info = {
                    'ip_src' : str(pkt.ip.src) ,
                    'ip_dst' : str(pkt.ip.dst) ,
                    'srcport_indexed': str(pkt.tcp.srcport) if hasattr(pkt, 'tcp') and hasattr(pkt.tcp, 'srcport') else 'empty',
                    'dstport_indexed': str(pkt.tcp.dstport) if hasattr(pkt, 'tcp') and hasattr(pkt.tcp, 'dstport') else 'empty',
                    'ip_proto_indexed': str(pkt.ip.proto) if hasattr(pkt.ip, 'proto') else 'empty',
                    'tcp_state_indexed': str(pkt.tcp.flags) if hasattr(pkt, 'tcp') and hasattr(pkt.tcp, 'flags') else 'empty',
                    'frame_len': str(pkt.length),
                    'tcp_seq': str(pkt.tcp.seq) if hasattr(pkt, 'tcp') and hasattr(pkt.tcp, 'seq') else 'empty',
                    'tcp_ack': str(pkt.tcp.ack) if hasattr(pkt, 'tcp') and hasattr(pkt.tcp, 'ack') else 'empty',
                    'Packets': str(pkt.length),
                    'Bytes': str(pkt.length),
                    'Tx_Packets': str(tx_packets),
                    'Tx_Bytes': str(tx_bytes),
                    'Rx_Packets': str(rx_packets),
                    'Rx_Bytes': str(rx_bytes)
                }
                
                # Split the IP address into octets
                ip_src_octets = packet_info['ip_src'].split('.')
                ip_dst_octets = packet_info['ip_dst'].split('.')

               # Prepare the message to be sent to Kafka
                for i in range(4):
                    src_message['ip_src_octet_'+str(i)] = int(ip_src_octets[i])
                    dst_message['ip_dst_octet_'+str(i)] = int(ip_dst_octets[i])

                # Assign IP octets back to packet_info
                for i in range(4):
                    packet_info['ip_src_octet_'+str(i)] = src_message['ip_src_octet_'+str(i)]
                    packet_info['ip_dst_octet_'+str(i)] = dst_message['ip_dst_octet_'+str(i)]
                
                # Update counters based on packet type
                if packet_info['srcport_indexed'] != 'empty' or packet_info['dstport_indexed'] != 'empty':
                    tx_packets += 1
                    tx_bytes += int(packet_info['Bytes'])
                else:
                    rx_packets += 1
                    rx_bytes += int(packet_info['Bytes'])
                
                # Add TX packet count to packet info
                packet_info['Tx_Packets'] = str(tx_packets)
                packet_info['Tx_Bytes'] = str(tx_bytes)
                
                # Map TCP state from TCP flags to actual state
                tcp_state = packet_info['tcp_state_indexed']

                if tcp_state == "0x0002":
                    tcp_state = "CON"
                elif tcp_state == "0x0001":
                    tcp_state = "FIN"
                elif tcp_state == "0x0010":
                    tcp_state = "CLO"
                elif tcp_state == "0x0004":
                    tcp_state = "RST"
                elif tcp_state == "0x0020":
                    tcp_state = "URH"
                elif tcp_state == "0x0040":
                    tcp_state = "URN"
                else:
                    tcp_state = "UNKNOWN"

                # Assign TCP states to packet_info
                packet_info['tcp_state_indexed'] = tcp_state

                
                # Append packet info to processed_packets list
                processed_packets.append(packet_info)
            except Exception as e:
                logging.error(f"Error processing packet: {e}")
    
    logging.info(f"Processed {len(processed_packets)} packets.")
    
    return processed_packets



def send_packets_to_kafka(packets, topic_name):
    logging.info("Sending packets to Kafka...")
    producer_config = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': 'my_producer',
        'acks': 'all'
    }
    
    producer = KafkaProducer(bootstrap_servers=producer_config['bootstrap.servers'],
                             client_id=producer_config['client.id'],
                             acks=producer_config['acks'],
                             value_serializer=lambda x: msgpack.packb(x))
    
    try:
        for packet in packets:
            producer.send(topic_name, value=packet)
        
        producer.flush()
        logging.info(f"Sent {len(packets)} packets to Kafka topic {topic_name}")
    except Exception as e:
        logging.error(f"Error sending packets to Kafka: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    # Define the Kafka topic name
    topic_name = 'capture-packet'

    # Define pcap file path
    pcap_file_path = '/Users/jejebubu/Desktop/bigproject/envkafka/rt_capture_file.pcapng'
    #pcap_file_path = '/Users/jejebubu/Desktop/bigproject/envkafka/20.pcap'
    
    # Read packet information from the pcap file
    raw_packets = read_packets_from_pcap(pcap_file_path)
    
    # Process the raw packets
    packets = process_packets(raw_packets)

    if packets:
        # Send the processed packet information to the Kafka topic
        send_packets_to_kafka(packets, topic_name)
    else:
        logging.warning("No packets to send.")
