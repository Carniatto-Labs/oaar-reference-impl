from fastapi import FastAPI

app = FastAPI(title="oaar-llm-gateway")


@app.get("/health")
def health():
    return {"status": "ok", "service": "llm-gateway"}


@app.post("/v1/chat/completions")
def chat_completions(body: dict):
    return {
        "id": "chatcmpl-oaar-stub-0001",
        "object": "chat.completion",
        "model": body.get("model", "oaar-stub-model"),
        "choices": [
            {
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "The requested artifact has been produced.",
                },
                "finish_reason": "stop",
            }
        ],
        "usage": {"prompt_tokens": 42, "completion_tokens": 11, "total_tokens": 53},
    }
