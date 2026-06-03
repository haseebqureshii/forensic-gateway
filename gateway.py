import os
import json
import time
import tempfile
import atexit
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from kafka import KafkaProducer, KafkaConsumer
from contracts import AgentExecutionTrace

app = FastAPI(title="Forensic Operations Gateway")

# Allow your local Svelte UI to talk to this cloud API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_methods=["*"],
    allow_headers=["*"],
)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka-888b017-haseebq-ai-safety.k.aivencloud.com:26518")

# Create secure temporary files from Render environment variables
tmp_key = tempfile.NamedTemporaryFile(delete=False, mode='w')
tmp_key.write(os.getenv("SSL_KEY", ""))
tmp_key.close()

tmp_cert = tempfile.NamedTemporaryFile(delete=False, mode='w')
tmp_cert.write(os.getenv("SSL_CERT", ""))
tmp_cert.close()

tmp_ca = tempfile.NamedTemporaryFile(delete=False, mode='w')
tmp_ca.write(os.getenv("SSL_CA", ""))
tmp_ca.close()

def cleanup_certs():
    for file_path in [tmp_key.name, tmp_cert.name, tmp_ca.name]:
        if os.path.exists(file_path):
            os.unlink(file_path)
atexit.register(cleanup_certs)

# Connect to Aiven Kafka
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    security_protocol="SSL",
    ssl_keyfile=tmp_key.name,
    ssl_certfile=tmp_cert.name,
    ssl_cafile=tmp_ca.name,
    api_version=(2, 5, 0),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

heartbeat_consumer = KafkaConsumer(
    'hpc-heartbeat',
    bootstrap_servers=[KAFKA_BROKER],
    security_protocol="SSL",
    ssl_keyfile=tmp_key.name,
    ssl_certfile=tmp_cert.name,
    ssl_cafile=tmp_ca.name,
    api_version=(2, 5, 0),
    auto_offset_reset='latest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

@app.get("/api/v1/hpc-status")
async def get_hpc_status():
    raw_messages = heartbeat_consumer.poll(timeout_ms=3000)
    if not raw_messages:
        return {"status": "OFFLINE", "message": "No heartbeat detected."}
    for tp, msgs in raw_messages.items():
        if not msgs: continue
        latest_msg = msgs[-1].value
        if time.time() - latest_msg["timestamp"] < 30:
            return {"status": "ONLINE"}
    return {"status": "OFFLINE"}

@app.post("/api/v1/ingest", status_code=202)
async def ingest_trace(trace: AgentExecutionTrace):
    try:
        producer.send("ops-database-cdc-stream", trace.model_dump(mode='json'))
        producer.flush()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))