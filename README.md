# Real-Time DDoS Detection: Leveraging Apache Kafka and Spark

## Overview
My 3rd year's Cybersecurity course project. Task to perform DDoS detection using Spark and Kafka.
- Writeup [link](https://docs.google.com/document/d/1DZn7WEMb9xe2B1LT9_K-xvDwFXxmj3rNY6umtImJtzU/edit?usp=sharing)

## Objectives
- To test the compatability and limitation of using Spark and Kafka for real-time detection.
- Use ML models (LR, DTC, RFC, MLP) to test the accuracy, recall, precision, and f1 score of the detection.
- In class we learn about these tools and their usage. In this project we make it happen.

## Architecture / Setup
- Use Wireshark to capture live network packets
- Feed the packets into Kafka to perform packet dissection and feature extraction
- Feed Kafka output into Spark Streaming for data processing. Then channeled through the ML model trained to identify DDoS attacks.

## Diagram
![Capture](https://github.com/user-attachments/assets/7dc36611-93f5-43d2-b255-572ec39d96e6)


## Results
- The pipeline flows from each tools nicely and neatly.
- The features we selected all have the most impact on the detection. 
- ![Capture](https://github.com/user-attachments/assets/2e3d43be-eba3-496e-8c30-37d95a3bf557)
- Shows real promise for using these tools for making a detection system.

