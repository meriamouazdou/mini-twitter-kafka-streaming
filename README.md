# Mini-Twitter — Real-Time Streaming with Apache Kafka
**Academic Project | Big Data Engineering**

## Overview
A simplified Twitter-like messaging platform built to demonstrate 
real-time data streaming architecture using Apache Kafka. 
Messages are produced, consumed, and processed in real-time 
across distributed topics.

## Architecture
Producer (User Posts) → Kafka Topic → Consumer (Feed Processing)
↓
Stream Processing
↓
Storage & Display
## Features
- Real-time message publishing and consumption
- Multi-topic Kafka architecture
- Producer/Consumer implementation in Java
- Scalable and fault-tolerant design

## Tech Stack
| Layer | Technologies |
|-------|-------------|
| Streaming | Apache Kafka |
| Language | Java |
| Architecture | Producer/Consumer Pattern |
| Design | Distributed Messaging System |

## How to Run
```bash
# Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka broker
bin/kafka-server-start.sh config/server.properties

# Run Producer
java -cp target/mini-twitter.jar ProducerApp

# Run Consumer
java -cp target/mini-twitter.jar ConsumerApp
```

## Key Concepts Demonstrated
- Event-driven architecture
- Distributed messaging
- Real-time data pipelines
- Fault tolerance and scalability
