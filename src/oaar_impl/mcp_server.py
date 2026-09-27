import time

from fastapi import FastAPI, HTTPException, Request

app = FastAPI(title="oaar-mcp-server")

_seen_executions: dict = {}


@app.get("/health")
def health():
    return {"status": "ok", "service": "mcp-server"}


@app.get("/tools")
def tools():
    return {
        "tools": [
            {"name": "echo", "effect_class": "read_only"},
        ]
    }


@app.post("/execute")
async def execute(request: Request):
    body = await request.json()
    if body.get("fail"):
        raise HTTPException(status_code=503, detail="transient failure")
    idempotency_key = body.get("idempotency_key")
    traceparent = request.headers.get("traceparent", "")
    if idempotency_key and idempotency_key in _seen_executions:
        return {
            "deduplicated": True,
            "traceparent": traceparent,
            "result": _seen_executions[idempotency_key],
        }
    result = {"echo": body.get("payload", ""), "executed_at": time.time()}
    if idempotency_key:
        _seen_executions[idempotency_key] = result
    return {"deduplicated": False, "traceparent": traceparent, "result": result}
