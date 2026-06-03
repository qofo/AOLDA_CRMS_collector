
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional
import httpx
import os
from datetime import datetime, timezone

# ==========================================
# 1. Environment Configuration and Constants
# ==========================================
app = FastAPI(title="CRMS Metric Gateway", description="Ingests metrics from Fluent Bit and forwards to Gnocchi")

# Gnocchi server connection details (can be overridden by environment variables)
GNOCCHI_ENDPOINT = os.getenv("GNOCCHI_ENDPOINT", "http://192.168.0.121:8041")
GNOCCHI_USER = os.getenv("GNOCCHI_USER", "admin")
GNOCCHI_PASSWORD = os.getenv("GNOCCHI_PASSWORD", "")

# Security Policy: Whitelist of allowed metrics
ALLOWED_METRICS = {
    "guest.cpu.util",
    "guest.memory.used_percent",
    "guest.net.in.bytes",
    "guest.disk.read.bytes"
}

# ==========================================
# 2. Data Schema Definition (CRMS Canonical Schema)
# ==========================================
class ResourceSchema(BaseModel):
    type: str
    id: str
    project_id: str

class MetricSchema(BaseModel):
    name: str
    type: str
    unit: str

class MeasureSchema(BaseModel):
    timestamp: float
    value: float

class CanonicalPayload(BaseModel):
    date: Optional[float] = None  # Used for handling time values automatically attached by Fluent Bit
    resource: ResourceSchema
    metric: MetricSchema
    measure: MeasureSchema

# ==========================================
# 3. Gnocchi 비동기 전송 로직 (Forwarding & Lazy Creation)
# ==========================================
async def forward_to_gnocchi(payload: CanonicalPayload):
    resource_id = payload.resource.id
    project_id = payload.resource.project_id
    metric_name = payload.metric.name
    
    url = f"{GNOCCHI_ENDPOINT}/v1/resource/generic/{resource_id}/metric/{metric_name}/measures"
    
    dt = datetime.fromtimestamp(payload.measure.timestamp, tz=timezone.utc)
    gnocchi_payload = [{"timestamp": dt.isoformat(), "value": payload.measure.value}]
    auth = (GNOCCHI_USER, GNOCCHI_PASSWORD)
    
    try:
        async with httpx.AsyncClient() as client:
            # 1. 측정값 전송 시도
            response = await client.post(url, json=gnocchi_payload, auth=auth, timeout=5.0)
            
            # 2. 404 에러 발생 시 (자원 또는 메트릭이 Gnocchi에 없을 때)
            if response.status_code == 404:
                print(f"[INFO] Gnocchi에 자원이 없습니다. 자동 생성을 시도합니다: {resource_id}")
                
                # 자원 및 메트릭 생성 페이로드
                create_payload = {
                    "id": resource_id,
                    "project_id": project_id,
                    "metrics": { metric_name: {} } # 기본 아카이브 정책 사용
                }
                
                create_resp = await client.post(f"{GNOCCHI_ENDPOINT}/v1/resource/generic", json=create_payload, auth=auth)
                
                # 409 Conflict: 자원(VM)은 있는데 메트릭만 없을 경우
                if create_resp.status_code == 409:
                    await client.post(f"{GNOCCHI_ENDPOINT}/v1/resource/generic/{resource_id}/metric", 
                                      json={metric_name: {}}, auth=auth)
                
                # 생성 완료 후 데이터 재전송 시도
                retry_resp = await client.post(url, json=gnocchi_payload, auth=auth, timeout=5.0)
                retry_resp.raise_for_status()
                print(f"[SUCCESS] 자원 생성 후 전송 성공: {metric_name} ({payload.measure.value})")
                return

            response.raise_for_status()
            print(f"[SUCCESS] 전송 성공: {metric_name} ({payload.measure.value}) for VM: {resource_id}")
            
    except httpx.HTTPStatusError as e:
        print(f"[ERROR] Gnocchi API 거절: {e.response.status_code} - {e.response.text}")
    except Exception as e:
        print(f"[ERROR] Gnocchi 연결 실패: {e}")

# ==========================================
# 4. API Endpoints (Ingestion & Validation)
# ==========================================
@app.post("/v1/metrics", status_code=202)
async def ingest_metric(payload: CanonicalPayload, background_tasks: BackgroundTasks):
    """
    Ingests metrics from Fluent Bit, validates them, and forwards to Gnocchi 
    using background tasks.
    """
    # 1. Validate if the metric is in the allowed whitelist
    if payload.metric.name not in ALLOWED_METRICS:
        print(f"[WARN] Blocked unauthorized metric: {payload.metric.name}")
        raise HTTPException(status_code=400, detail=f"Metric '{payload.metric.name}' is not allowed.")
    
    # 2. Return acceptance immediately and offload Gnocchi transmission to background tasks
    background_tasks.add_task(forward_to_gnocchi, payload)
    
    return {"status": "accepted"}
