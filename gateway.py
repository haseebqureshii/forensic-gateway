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

# 1. Hardcode the fallback so it never evaluates to None
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka-888b017-haseebq-ai-safety.k.aivencloud.com:26518")

# 2. Loudly verify Secret Files exist and dynamically find their path
cert_dir = ""
for path in [".", "/etc/secrets"]:
    if os.path.exists(f"{path}/service.key"):
        cert_dir = path
        break

if not cert_dir:
    raise FileNotFoundError(
        "CRITICAL ERROR: Cannot find SSL Secret Files. "
        "Ensure 'service.key', 'service.cert', and 'ca.pem' exist in the Render 'Secret Files' tab."
    )

# Connect to Aiven
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    security_protocol="SSL",
    ssl_keyfile=f"{cert_dir}/service.key",
    ssl_certfile=f"{cert_dir}/service.cert",
    ssl_cafile=f"{cert_dir}/ca.pem",
    api_version=(2, 5, 0),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

heartbeat_consumer = KafkaConsumer(
    'hpc-heartbeat',
    bootstrap_servers=[KAFKA_BROKER],
    security_protocol="SSL",
    ssl_keyfile=f"{cert_dir}/service.key",
    ssl_certfile=f"{cert_dir}/service.cert",
    ssl_cafile=f"{cert_dir}/ca.pem",
    api_version=(2, 5, 0),
    auto_offset_reset='latest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Note: Removed 'async' so FastAPI automatically handles the blocking Kafka poll in a threadpool!
@app.get("/api/v1/hpc-status")
def get_hpc_status():
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
def ingest_trace(trace: AgentExecutionTrace):
    try:
        producer.send("ops-database-cdc-stream", trace.model_dump(mode='json'))
        producer.flush()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))