import json
import os

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from opentelemetry.propagate import inject

from oaar_impl.otel_setup import configure_telemetry, shutdown_telemetry
from oaar_impl.purchase_agent import run_purchase_agent

configure_telemetry()

app = FastAPI(title="oaar-chat-panel")

GRAFANA = os.environ.get("OAAR_GRAFANA_URL", "http://localhost:3000")


@app.get("/health")
def health():
    return {"status": "ok", "service": "chat-panel"}


@app.post("/api/chat")
def chat(body: dict):
    prompt = (body.get("prompt") or "").strip()
    thread_id = (body.get("thread_id") or "").strip() or "thread-shop-anon"

    def stream():
        import queue
        import threading

        events: queue.Queue = queue.Queue()
        final_sent = {"value": False}

        def emit(payload: str):
            events.put(f"data: {payload}\n\n")

        def mark_final(payload: str):
            final_sent["value"] = True
            emit(payload)

        def worker():
            try:
                for event in run_purchase_agent(prompt, thread_id):
                    payload = json.dumps(event)
                    (mark_final if event.get("type") == "final" else emit)(payload)
            except Exception as exc:
                if not final_sent["value"]:
                    emit(json.dumps({
                        "type": "final", "outcome": "declined", "reason": "agent_error",
                        "stage": "runtime", "order_id": "-", "summary": f"Agent failed: {exc}",
                    }))
            finally:
                events.put(None)

        threading.Thread(target=worker, daemon=True).start()
        while True:
            item = events.get()
            if item is None:
                break
            yield item
        yield "event: end\ndata: {}\n\n"

    from fastapi.responses import StreamingResponse

    return StreamingResponse(stream(), media_type="text/event-stream")


@app.get("/api/catalog")
def catalog():
    import urllib.request

    headers = {}
    inject(headers)
    request = urllib.request.Request("http://product-api:8082/products", headers=headers)
    with urllib.request.urlopen(request, timeout=10) as resp:
        return json.loads(resp.read())


PANEL_HTML = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>OAAR · Purchase Assistant</title>
<style>
  :root {{
    --bg: #111217; --panel: #181b1f; --border: #2c3235; --text: #d8d9da;
    --muted: #8e8e8e; --accent: #ff8833; --green: #73bf69; --red: #f2495c;
    --blue: #5794f2; --purple: #b877d9; --yellow: #fade2a;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: var(--bg); color: var(--text); font-family: Inter, -apple-system, 'Segoe UI', sans-serif; height: 100vh; display: flex; flex-direction: column; }}
  header {{ display: flex; align-items: center; gap: 14px; padding: 12px 20px; background: var(--panel); border-bottom: 1px solid var(--border); }}
  header h1 {{ font-size: 15px; font-weight: 600; letter-spacing: .3px; }}
  header h1 span {{ color: var(--accent); }}
  header .spacer {{ flex: 1; }}
  header a.dash {{ color: var(--muted); font-size: 12px; text-decoration: none; border: 1px solid var(--border); border-radius: 6px; padding: 5px 10px; }}
  header a.dash:hover {{ color: var(--accent); border-color: var(--accent); }}
  #thread {{ font-size: 11px; color: var(--muted); }}
  main {{ flex: 1; overflow-y: auto; padding: 24px; display: flex; flex-direction: column; gap: 18px; max-width: 860px; width: 100%; margin: 0 auto; }}
  .turn {{ display: flex; flex-direction: column; gap: 10px; }}
  .user-msg {{ align-self: flex-end; background: #2b3a4d; border-radius: 12px 12px 2px 12px; padding: 10px 14px; max-width: 75%; font-size: 14px; }}
  .agent-card {{ align-self: flex-start; background: var(--panel); border: 1px solid var(--border); border-radius: 10px; padding: 14px; width: 92%; }}
  .steps {{ display: flex; flex-direction: column; gap: 6px; }}
  .step {{ display: flex; gap: 10px; align-items: baseline; font-size: 13px; animation: fade .25s ease; }}
  @keyframes fade {{ from {{ opacity: 0; transform: translateY(3px); }} to {{ opacity: 1; }} }}
  .step .icon {{ width: 18px; text-align: center; }}
  .step .label {{ color: var(--muted); min-width: 84px; font-size: 11px; text-transform: uppercase; letter-spacing: .5px; }}
  .step.decision .label {{ color: var(--purple); }}
  .step.tool .label {{ color: var(--blue); }}
  .step.tool.error .label {{ color: var(--red); }}
  .step.effect .label {{ color: var(--yellow); }}
  .step .body {{ flex: 1; }}
  .step .kind {{ font-weight: 600; margin-right: 6px; }}
  .final {{ margin-top: 10px; border-radius: 8px; padding: 10px 14px; font-size: 14px; line-height: 1.45; }}
  .final.accepted {{ background: rgba(115,191,105,.12); border-left: 3px solid var(--green); }}
  .final.declined {{ background: rgba(242,73,92,.12); border-left: 3px solid var(--red); }}
  .final .badge {{ display: inline-block; font-size: 10px; font-weight: 700; letter-spacing: 1px; padding: 2px 8px; border-radius: 4px; margin-bottom: 6px; }}
  .final.accepted .badge {{ background: var(--green); color: #10261a; }}
  .final.declined .badge {{ background: var(--red); color: #fff; }}
  .final .links {{ margin-top: 8px; font-size: 12px; }}
  .final .links a {{ color: var(--blue); text-decoration: none; margin-right: 14px; }}
  .final .links a:hover {{ text-decoration: underline; }}
  .thinking {{ color: var(--muted); font-size: 13px; display: flex; align-items: center; gap: 8px; }}
  .thinking::after {{ content: ''; width: 14px; height: 14px; border: 2px solid var(--accent); border-top-color: transparent; border-radius: 50%; animation: spin .7s linear infinite; }}
  @keyframes spin {{ to {{ transform: rotate(360deg); }} }}
  footer {{ padding: 14px 20px 18px; background: var(--panel); border-top: 1px solid var(--border); }}
  .chips {{ display: flex; gap: 8px; flex-wrap: wrap; max-width: 860px; margin: 0 auto 10px; }}
  .chip {{ font-size: 12px; color: var(--muted); background: var(--bg); border: 1px solid var(--border); border-radius: 14px; padding: 5px 12px; cursor: pointer; }}
  .chip:hover {{ color: var(--accent); border-color: var(--accent); }}
  .input-row {{ display: flex; gap: 10px; max-width: 860px; margin: 0 auto; }}
  #prompt {{ flex: 1; background: var(--bg); border: 1px solid var(--border); border-radius: 8px; color: var(--text); font-size: 14px; padding: 11px 14px; outline: none; }}
  #prompt:focus {{ border-color: var(--accent); }}
  #send {{ background: var(--accent); color: #1a1108; font-weight: 700; border: none; border-radius: 8px; padding: 0 22px; font-size: 14px; cursor: pointer; }}
  #send:disabled {{ opacity: .5; cursor: default; }}
  .catalog {{ font-size: 11px; color: var(--muted); max-width: 860px; margin: 8px auto 0; }}
</style>
</head>
<body>
<header>
  <h1><span>OAAR</span> · Purchase Assistant</h1>
  <span id="thread"></span>
  <div class="spacer"></div>
  <a class="dash" href="{GRAFANA}/d/oaar-runtime" target="_blank">Runtime dashboard</a>
  <a class="dash" href="{GRAFANA}/d/oaar-effects" target="_blank">Outcomes &amp; effects</a>
  <a class="dash" href="{GRAFANA}/d/oaar-purchase" target="_blank">Purchase flow</a>
</header>
<main id="chat"></main>
<footer>
  <div class="chips" id="chips"></div>
  <div class="input-row">
    <input id="prompt" placeholder="e.g. Buy the ANC headphones with card 4111 1111 1111 1111" autocomplete="off">
    <button id="send">Send</button>
  </div>
  <div class="catalog" id="catalog"></div>
</footer>
<script>
const GRAFANA = "{GRAFANA}";
const threadId = "thread-shop-" + Math.random().toString(36).slice(2, 10);
document.getElementById("thread").textContent = threadId;
const chat = document.getElementById("chat");
const input = document.getElementById("prompt");
const sendBtn = document.getElementById("send");

const CHIPS = [
  "Buy the ANC headphones with card 4111 1111 1111 1111",
  "I want 2x mechanical keyboard, mastercard 5500 0000 0000 0001",
  "Buy the 4K monitor with my amex 3714 4963 5398 431",
  "Get me a fone bluetooth, elo card 5067 6789 0123 4567",
  "Buy the gaming mouse with blocked card 4111 1111 1111 0000",
  "Purchase a vintage guitar, visa 4242 4242 4242 4242"
];
const chipsBox = document.getElementById("chips");
CHIPS.forEach(text => {{
  const chip = document.createElement("button");
  chip.className = "chip"; chip.textContent = text;
  chip.onclick = () => submit(text);
  chipsBox.appendChild(chip);
}});

fetch("/api/catalog").then(r => r.json()).then(d => {{
  document.getElementById("catalog").textContent =
    "Catalog: " + d.products.map(p => p.name + (p.stock ? "" : " (out of stock)")).join(" · ");
}}).catch(() => {{}});

const ICONS = {{ decision: "\U0001F9E0", tool: "\U0001F527", effect: "\u26A1", run: "\u25B6" }};

function tempoLink(traceId) {{
  const left = JSON.stringify({{ datasource: "oaar-tempo", queries: [{{ expr: traceId, queryType: "traceql", refId: "A" }}] }});
  return GRAFANA + "/explore?left=" + encodeURIComponent(left);
}}

function submit(text) {{
  if (!text.trim()) return;
  input.value = ""; sendBtn.disabled = true;
  const turn = document.createElement("div");
  turn.className = "turn";
  turn.innerHTML = '<div class="user-msg"></div>';
  turn.querySelector(".user-msg").textContent = text;
  const card = document.createElement("div");
  card.className = "agent-card";
  card.innerHTML = '<div class="steps"></div><div class="thinking">running agent loop</div>';
  turn.appendChild(card);
  chat.appendChild(turn);
  chat.scrollTop = chat.scrollHeight;

  const steps = card.querySelector(".steps");
  let traceId = "";
  let current = null;

  function addStep(cls, icon, label, body) {{
    const el = document.createElement("div");
    el.className = "step " + cls;
    const b = document.createElement("span"); b.textContent = body;
    el.innerHTML = '<span class="icon">' + icon + '</span><span class="label">' + label + '</span><span class="body"></span>';
    el.querySelector(".body").appendChild(b);
    steps.appendChild(el);
    chat.scrollTop = chat.scrollHeight;
    return el;
  }}

  fetch("/api/chat", {{
    method: "POST", headers: {{ "Content-Type": "application/json" }},
    body: JSON.stringify({{ prompt: text, thread_id: threadId }})
  }}).then(resp => {{
    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";
    function pump() {{
      return reader.read().then(({{ done, value }}) => {{
        if (done) {{ sendBtn.disabled = false; return; }}
        buffer += decoder.decode(value, {{ stream: true }});
        const lines = buffer.split("\\n");
        buffer = lines.pop();
        lines.forEach(line => {{
          if (!line.startsWith("data: ")) return;
          let ev; try {{ ev = JSON.parse(line.slice(6)); }} catch (e) {{ return; }}
          card.querySelector(".thinking")?.remove();
          if (ev.type === "run") {{
            traceId = ev.trace_id;
            addStep("run", ICONS.run, "run", "run " + ev.run_id.slice(0, 14) + "\\u2026 · order " + ev.order_id);
          }} else if (ev.type === "decision") {{
            current = addStep("decision", ICONS.decision, ev.kind, ev.basis + " \\u2192 " + ev.outcome);
          }} else if (ev.type === "tool") {{
            current = addStep("tool" + (ev.status === "error" ? " error" : ""), ICONS.tool, ev.name, ev.detail);
          }} else if (ev.type === "effect") {{
            current = addStep("effect", ICONS.effect, "effect", ev.resource + " · " + ev.status + " · " + ev.detail);
          }} else if (ev.type === "final") {{
            const fin = document.createElement("div");
            fin.className = "final " + ev.outcome;
            const badge = document.createElement("span");
            badge.className = "badge";
            badge.textContent = ev.outcome.toUpperCase() + (ev.reason ? " · " + ev.reason.replace(/_/g, " ") : "");
            fin.appendChild(badge);
            const p = document.createElement("div"); p.textContent = ev.summary; fin.appendChild(p);
            const links = document.createElement("div"); links.className = "links";
            if (traceId) {{
              const a = document.createElement("a"); a.href = tempoLink(traceId); a.target = "_blank";
              a.textContent = "Open trace in Grafana Tempo"; links.appendChild(a);
            }}
            const b2 = document.createElement("a"); b2.href = GRAFANA + "/d/oaar-purchase"; b2.target = "_blank";
            b2.textContent = "Purchase flow dashboard"; links.appendChild(b2);
            fin.appendChild(links);
            card.appendChild(fin);
            chat.scrollTop = chat.scrollHeight;
          }}
        }});
        return pump();
      }});
    }}
    return pump();
  }}).catch(err => {{
    card.querySelector(".thinking")?.remove();
    const fin = document.createElement("div");
    fin.className = "final declined";
    fin.textContent = "Request failed: " + err;
    card.appendChild(fin);
    sendBtn.disabled = false;
  }});
}}

sendBtn.onclick = () => submit(input.value);
input.addEventListener("keydown", e => {{ if (e.key === "Enter") submit(input.value); }});
</script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
def index():
    return PANEL_HTML


@app.on_event("shutdown")
def _shutdown():
    shutdown_telemetry()
