# ==============================================================================
# CRMS Metric Gateway (Stateless Batch Router)
# Description: A high-throughput API gateway that receives telemetry from thousands
#              of VMs, sanitizes the data (Time Drift, Whitelists), and pushes it 
#              to Gnocchi using the Batch Measures API.
# Architectural Constraint: This service is STRICTLY STATELESS. It does not 
#                           create Gnocchi resources. Provisioning must handle that.
# ==============================================================================

from fastapi import FastAPI, BackgroundTasks, HTTPException, Header, Depends
from pydantic import BaseModel
from typing import List, Dict, Any
from collections import defaultdict
import httpx
import os
import time
import datetime

app = FastAPI(title="CRMS Metric Gateway (Strict Mode)")

# --- Backend Connectivity ---
GNOCCHI_ENDPOINT = os.getenv("GNOCCHI_ENDPOINT", "http://192.168.0.121:8041")
GNOCCHI_USER = os.getenv("GNOCCHI_USER", "admin")
GNOCCHI_PASSWORD = os.getenv("GNOCCHI_PASSWORD", "")

# --- Security & Stability Thresholds ---
# PSK for authenticating agent traffic. Prevents malicious metric injection.
GATEWAY_PSK = os.getenv("GATEWAY_PSK", "crms-secret-token-v1") 

# [OOM Defense] Maximum allowed array size per HTTP request. 
# Rejects abnormally large payloads buffered by agents after network partitions.
MAX_PAYLOAD_SIZE = 1000  

# [Time Drift Defense] Maximum allowed difference between Gateway time and Agent time.
# Prevents corrupted historical data from ruining TSDB aggregations (5 minutes).
MAX_TIME_DRIFT_SEC = 300 

# Strict whitelist enforcing the separation of concerns (Guest vs Hypervisor metrics).
ALLOWED_METRICS = {
    "guest.cpu.util",
    "guest.memory.used_percent",
    "guest.net.in.bytes",
    "guest.disk.read.bytes"
}

# ==========================================
# Schema Definitions (Canonical Model)
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
    resource: ResourceSchema
    metric: MetricSchema
    measure: MeasureSchema

# ==========================================
# Dependencies & Middleware
# ==========================================
def verify_agent_token(x_auth_token: str = Header(...)):
    """
    Security Gatekeeper: Validates the Pre-Shared Key (PSK).
    Drops connections from unauthorized sources with a 401 response.
    """
    if x_auth_token != GATEWAY_PSK:
        raise HTTPException(status_code=401, detail="Unauthorized agent token")
    return x_auth_token

# ==========================================
# Core Processing Logic
# ==========================================
async def batch_forward_to_gnocchi(payloads: List[CanonicalPayload]):
    """
    Transforms the linear array of CanonicalPayloads into the nested dictionary 
    structure required by Gnocchi's Batch Measures API, executing a single network I/O.
    """
    batch_data: Dict[str, Dict[str, List[Dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    current_time = time.time()
    
    valid_count = 0
    
    for item in payloads:
        # 1. Whitelist Enforcement: Drop unauthorized metric names silently.
        if item.metric.name not in ALLOWED_METRICS:
            continue
            
        # 2. Time Drift Enforcement: Drop metrics from VMs with broken NTP clocks.
        if abs(current_time - item.measure.timestamp) > MAX_TIME_DRIFT_SEC:
            continue

        # Convert Unix Timestamp to ISO 8601 string (Required by Gnocchi TSDB).
        iso_time = datetime.datetime.fromtimestamp(item.measure.timestamp, tz=datetime.timezone.utc).isoformat()
        
        # Build the Gnocchi Batch dictionary: resource_id -> metric_name -> [{measure}]
        batch_data[item.resource.id][item.metric.name].append({
            "timestamp": iso_time,
            "value": item.measure.value
        })
        valid_count += 1

    # Terminate early if all items were dropped by validation filters.
    if not batch_data:
        return

    url = f"{GNOCCHI_ENDPOINT}/v1/batch/resources/metrics/measures"
    auth = (GNOCCHI_USER, GNOCCHI_PASSWORD)

    # Execute HTTP POST with a generous 15-second timeout to handle TSDB lock contentions.
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(url, json=batch_data, auth=auth, timeout=15.0)
            
            # A 400 Bad Request indicates that the target resource or metric UUID does NOT exist.
            # Following the Stateless rule, we do NOT attempt to create it. We rely on the 
            # provisioning system to rectify the missing resource.
            if response.status_code == 400:
                print(f"[ERROR] Batch Reject (Provisioning delayed or Invalid Schema): {response.text}")
            else:
                response.raise_for_status()
                print(f"[SUCCESS] Sent {valid_count} measures across {len(batch_data)} resources.")
        except Exception as e:
            # Catch-all for network timeouts or 5xx server errors from Gnocchi.
            print(f"[ERROR] Gnocchi API Failed: {e}")

# ==========================================
# API Endpoints
# ==========================================
@app.post("/v1/metrics", status_code=202)
async def ingest_metrics(
    payloads: List[CanonicalPayload], 
    background_tasks: BackgroundTasks,
    token: str = Depends(verify_agent_token)
):
    """
    Entrypoint for telemetry ingestion. Validates payload size to prevent OOM attacks,
    then offloads the actual processing to a background task thread to free up 
    the HTTP connection immediately (High Concurrency).
    """
    if len(payloads) > MAX_PAYLOAD_SIZE:
        raise HTTPException(
            status_code=413, 
            detail=f"Payload size exceeds limits ({MAX_PAYLOAD_SIZE})"
        )
        
    if not payloads:
        raise HTTPException(status_code=400, detail="Empty payload list")

    # Delegate the heavy I/O operation to Starlette's BackgroundTasks queue.
    background_tasks.add_task(batch_forward_to_gnocchi, payloads)
    
    return {"status": "accepted"}