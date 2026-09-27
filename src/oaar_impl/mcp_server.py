from fastapi import FastAPI

app = FastAPI(title="oaar-mcp-server")


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
