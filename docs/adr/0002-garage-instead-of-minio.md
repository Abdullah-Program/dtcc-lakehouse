# ADR 0002: Garage S3 Instead of MinIO for Local Object Storage

## Status
Accepted

## Context
Phase 5 decouples lakehouse storage from the local container filesystem to model modern cloud data architectures (such as AWS S3 + Apache Polaris REST catalog + Apache Iceberg).

In local development environments, an S3-compatible object storage service is required to simulate AWS S3 endpoints. MinIO was historically the common default choice for local S3 simulation. However:
1. **Licensing**: MinIO changed its open-source license from Apache 2.0 to GNU AGPLv3, creating licensing friction and corporate compliance restrictions in commercial data platforms.
2. **Resource Overhead**: MinIO has evolved into an enterprise data platform with significant idle memory consumption (~300–600 MB) and higher CPU overhead.
3. **Hardware Constraints**: Our development workstation has 15.7 GB total host RAM (often ~12.7 GB in use at idle) and WSL2 is capped at 7.6 GiB. Running PySpark (2500M), Redpanda (750M), Apache Polaris (512M), and an object store simultaneously requires extreme memory discipline.

## Decision
We chose **Garage** (`dxflrs/garage:v2.4.1`) developed by Deuxfleurs as our local S3-compatible object storage service:
1. **Ultra-Low Resource Footprint**: Written natively in Rust and running on a scratch Docker base image with no shell or runtime dependencies, Garage consumes **under 40 MB RAM** at idle. We can comfortably run it with a strict **250M memory limit**.
2. **Standard AWS S3 API Compatibility**: Implements standard Amazon S3 REST APIs with AWS Signature Version 4 (SigV4) HMAC-SHA256 authentication, path-style addressing, and bucket ACLs.
3. **Reliable Single-Node Layout**: Uses an embedded SQLite metadata engine and two-phase layout management (`layout assign` and `layout apply`) for deterministic partitioning.
4. **Storage Reliability on WSL2**: Utilizes native Docker named volumes (`garage_meta`, `garage_data`) within WSL2's native Linux ext4 filesystem, eliminating POSIX file locking issues (`fcntl`/`flock`) that occur when running database engines over Windows 9P/NTFS bind mounts.

## Consequences
- **Positive**: Sub-second startup (<1s), zero memory pressure on the developer laptop, standard S3 API compatibility verified via pure Python standard library test suites (`tests/test_garage_s3.py`), and seamless interoperability with PySpark S3A and Apache Polaris.
- **Negative / Trade-offs**: In production AWS enterprise deployments, Amazon S3 is the native managed service. However, because Garage adheres to the standard AWS S3 API, all client configurations (endpoint URI, credentials, bucket names) translate to AWS S3 with zero application code changes.
