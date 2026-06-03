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

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_methods=["*"],
    allow_headers=["*"],
)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka-888b017-haseebq-ai-safety.k.aivencloud.com:26518")

# --- 1. SSL Setup (The Tempfile Method) ---
ssl_key_data = os.getenv("KAFKA_SSL_KEY")
ssl_cert_data = os.getenv("KAFKA_SSL_CERT")
ssl_ca_data = os.getenv("KAFKA_SSL_CA")

if not all([ssl_key_data, ssl_cert_data, ssl_ca_data]):
    raise ValueError("CRITICAL ERROR: KAFKA_SSL environment variables are missing.")

tmp_key = tempfile.NamedTemporaryFile(delete=False, mode='w')
tmp_key.write(ssl_key_data.strip())
tmp_key.close()

tmp_cert = tempfile.NamedTemporaryFile(delete=False, mode='w')
tmp_cert.write(ssl_cert_data.strip())
tmp_cert.close()

tmp_ca = tempfile.NamedTemporaryFile(delete=False, mode='w')
tmp_ca.write(ssl_ca_data.strip())
tmp_ca.close()

def cleanup_certs():
    for f in [tmp_key.name, tmp_cert.name, tmp_ca.name]:
        if os.path.exists(f):
            os.unlink(f)
atexit.register(cleanup_certs)

# --- 2. Kafka Connections ---
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

# --- 3. Stateful Cache ---
last_known_heartbeat = 0.0

@app.get("/api/v1/hpc-status")
def get_hpc_status():
    global last_known_heartbeat
    
    # Drain the queue fast so the UI stays snappy
    raw_messages = heartbeat_consumer.poll(timeout_ms=1000)
    
    if raw_messages:
        for tp, msgs in raw_messages.items():
            if not msgs: continue
            latest_msg = msgs[-1].value
            last_known_heartbeat = latest_msg["timestamp"]
            
    # Honor the TTL lease for 75 seconds
    if time.time() - last_known_heartbeat < 75:
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