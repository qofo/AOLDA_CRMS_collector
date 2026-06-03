
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional
import httpx
import os
from datetime import datetime, timezone
from typing import Optional, List

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
async def ingest_metric(payloads: List[CanonicalPayload], background_tasks: BackgroundTasks):
    """
    Fluent Bit으로부터 메트릭 배열(Batch)을 수신받아 검증 후 백그라운드에서 Gnocchi로 전송합니다.
    """
    accepted_count = 0
    
    # Fluent Bit이 보낸 여러 개의 메트릭을 반복문으로 처리
    for payload in payloads:
        # 1. 허용된 메트릭인지 검사 (Validation)
        if payload.metric.name not in ALLOWED_METRICS:
            print(f"[WARN] 허용되지 않은 메트릭 차단: {payload.metric.name}")
            continue # 에러를 내지 않고 해당 메트릭만 무시 (다른 정상 데이터는 살림)
        
        # 2. 백그라운드 태스크로 개별 전송 작업 할당
        background_tasks.add_task(forward_to_gnocchi, payload)
        accepted_count += 1
    
    return {"status": "accepted", "processed_metrics": accepted_count}
