### **About This Project: Forensic Operations Gateway**

**Overview**
This interface is the frontend component of a real-time telemetry pipeline designed for AI safety research. Its primary function is to ingest, queue, and monitor dense AI agent execution traces. Because analyzing complex agent coordination requires significant computational overhead, this project decouples the user interface from the heavy processing by bridging a modern web stack with enterprise supercomputing infrastructure.

**The Architecture Flow**
The system operates on an event-driven architecture to ensure high throughput and prevent data loss during traffic spikes:
1. **The Client (SvelteKit):** Acts as the control plane, allowing researchers to upload standard `.json` execution traces and monitor the real-time heartbeat of the backend cluster.
2. **The API Gateway (FastAPI):** A stateful proxy layer that receives payloads and handles secure cryptographic handshakes.
3. **The Event Broker (Apache Kafka on Aiven):** An asynchronous message queue that holds the execution traces in a secure cloud environment, ensuring the compute nodes are never overwhelmed.
4. **The Compute Workers (Python):** Background consumer scripts running continuously on dedicated compute nodes, pulling data from Kafka to execute the forensic simulations.

**Why Supercomputing (HPC)?**
The downstream processing of these agent traces involves complex simulations and parallel data evaluations that exceed the practical limits of standard cloud hosting. By routing the processing to the ASU Sol supercomputer, the pipeline leverages high-performance compute clusters specifically optimized for rigorous, resource-intensive research workloads.

**Engineering Challenges**
A core challenge of this architecture was securely connecting a public-facing web application and a cloud-based Kafka broker to a highly restricted university research network. To bypass strict enterprise firewalls and proxy redirect loops without compromising security, the system utilizes local SSH port forwarding and secure SSL/TLS certificate authentication. This creates an encrypted, stateful tunnel directly into the compute nodes, allowing seamless data ingestion into a closed HPC environment.
