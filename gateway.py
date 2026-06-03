import os
import json
import time
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from kafka import KafkaProducer, KafkaConsumer
from contracts import AgentExecutionTrace

app = FastAPI(title="Forensic Operations Gateway")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_methods=["*"],
    allow_headers=["*"],
)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka-888b017-haseebq-ai-safety.k.aivencloud.com:26518")

# The Ultimate Path Finder
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
search_paths = [BASE_DIR, ".", "/etc/secrets", "/opt/render/project/src"]

cert_dir = ""
for path in search_paths:
    if os.path.exists(os.path.join(path, "service.key")):
        cert_dir = path
        break

if not cert_dir:
    # If it fails, print the exact hard drive contents to the Render logs
    print(f"[DEBUG] Current working directory: {os.getcwd()}")
    print(f"[DEBUG] Files in BASE_DIR ({BASE_DIR}): {os.listdir(BASE_DIR)}")
    if os.path.exists("/etc/secrets"):
        print(f"[DEBUG] Files in /etc/secrets: {os.listdir('/etc/secrets')}")
    
    raise FileNotFoundError(
        "CRITICAL ERROR: Cannot find SSL Secret Files. Check the [DEBUG] logs above this error to see what Render actually mounted."
    )

print(f"[SUCCESS] Found SSL certificates in: {cert_dir}")

# Connect to Aiven
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    security_protocol="SSL",
    ssl_keyfile=os.path.join(cert_dir, "service.key"),
    ssl_certfile=os.path.join(cert_dir, "service.cert"),
    ssl_cafile=os.path.join(cert_dir, "ca.pem"),
    api_version=(2, 5, 0),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

heartbeat_consumer = KafkaConsumer(
    'hpc-heartbeat',
    bootstrap_servers=[KAFKA_BROKER],
    security_protocol="SSL",
    ssl_keyfile=os.path.join(cert_dir, "service.key"),
    ssl_certfile=os.path.join(cert_dir, "service.cert"),
    ssl_cafile=os.path.join(cert_dir, "ca.pem"),
    api_version=(2, 5, 0),
    auto_offset_reset='latest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

@app.get("/api/v1/hpc-status")
def get_hpc_status():
    raw_messages = heartbeat_consumer.poll(timeout_ms=3000)
    if not raw_messages:
        return {"status": "OFFLINE", "message": "No heartbeat detected."}
    for tp, msgs in raw_messages.items():
        if not msgs: continue
        latest_msg = msgs[-1].value
        if time.time() - latest_msg["timestamp"] < 65:
            return {"status": "ONLINE"}
    return {"status": "OFFLINE"}

@app.post("/api/v1/ingest", status_code=202)
def ingest_trace(trace: AgentExecutionTrace):
    try:
        producer.send("ops-database-cdc-stream", trace.model_dump(mode='json'))
        producer.flush()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))