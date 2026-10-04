# ADR 0003: Redpanda Instead of Apache Kafka for Local Streaming Development

## Status
Accepted

## Context
Phase 4 introduces real-time streaming ingestion of DTCC ticker price messages. The standard enterprise streaming API is Apache Kafka. 

However, running a traditional Apache Kafka cluster (or even KRaft mode) locally on developer workstations introduces significant resource overhead:
- Requires the JVM with substantial heap allocation (minimum 1–2 GB at idle).
- JVM garbage collection pauses can cause resource spikes on laptops with limited available RAM.
- Startup latency and cluster coordination complexity make quick iterations slower.

Our developer workstation has 15.7 GB total RAM, with about 12.7 GB already in use at idle and WSL2 capped at 7.6 GiB.

## Decision
We chose **Redpanda** as our streaming message broker:
1. **100% Kafka API Compatibility**: Redpanda implements the Apache Kafka wire protocol natively. PySpark Structured Streaming (`spark-sql-kafka-0-10`) and standard Python Kafka clients (`confluent-kafka`, `kafka-python`) work with zero code modifications.
2. **C++ Native Engine**: Redpanda is implemented in C++ using the Seastar thread-per-core architecture, with zero JVM overhead and no garbage collection pauses.
3. **Low Resource Footprint**: Can be strictly configured to run with a **512 MB memory limit** and **1 CPU core** (`--smp 1 --memory 512M --reserve-memory 0M`), keeping total container memory comfortably under **750 MB**.
4. **Built-in Tooling (`rpk`)**: Includes the native `rpk` CLI for topic creation, cluster inspection, and message consumption without installing external Kafka utilities.

## Consequences
- **Positive**: Blazing fast startup (< 2 seconds), reliable performance within strict memory bounds, and 100% compatibility with standard Kafka clients.
- **Negative / Trade-offs**: In production AWS enterprise deployments, Amazon MSK (Managed Streaming for Apache Kafka) is often used. However, since the client APIs are identical, all producer, consumer, and Spark Structured Streaming code transfers to MSK without changes.
