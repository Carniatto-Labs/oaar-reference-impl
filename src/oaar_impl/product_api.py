import time
import unicodedata

from fastapi import FastAPI, HTTPException, Request
from opentelemetry import trace
from opentelemetry.propagate import extract
from opentelemetry.trace import SpanKind

from oaar_impl.otel_setup import configure_telemetry

configure_telemetry()

app = FastAPI(title="oaar-product-api")

CATALOG = {
    "sku-1234": {"id": "sku-1234", "name": "Fone Bluetooth ANC", "price": 299.90, "stock": 7, "tags": ["fone", "headphone", "bluetooth", "anc", "audio", "earphone"]},
    "sku-5678": {"id": "sku-5678", "name": "Teclado Mecanico 65%", "price": 449.00, "stock": 3, "tags": ["teclado", "keyboard", "mecanico", "mechanical", "65"]},
    "sku-9012": {"id": "sku-9012", "name": "Monitor 27 4K", "price": 1899.00, "stock": 0, "tags": ["monitor", "4k", "27", "tela", "display", "screen"]},
    "sku-2233": {"id": "sku-2233", "name": "Mouse Gamer 16K DPI", "price": 189.90, "stock": 12, "tags": ["mouse", "gamer", "gaming", "16k", "dpi", "periferico"]},
    "sku-4455": {"id": "sku-4455", "name": "Webcam 4K Pro", "price": 549.00, "stock": 5, "tags": ["webcam", "camera", "4k", "video", "chamada"]},
    "sku-6677": {"id": "sku-6677", "name": "Headset USB 7.1", "price": 219.00, "stock": 9, "tags": ["headset", "fone", "usb", "7.1", "audio", "microfone"]},
    "sku-8899": {"id": "sku-8899", "name": "SSD NVMe 2TB", "price": 799.00, "stock": 4, "tags": ["ssd", "nvme", "2tb", "armazenamento", "storage", "disco"]},
    "sku-1122": {"id": "sku-1122", "name": "Hub USB-C 7 em 1", "price": 149.00, "stock": 15, "tags": ["hub", "usb-c", "usbc", "adaptador", "adapter", "7 em 1"]},
}
CATALOG_VERSION = "3.2.0"

BRAND_LIMITS = {"visa": 1500.0, "mastercard": 800.0, "amex": 2000.0, "elo": 500.0, "unknown": 300.0}


def _normalize(text: str) -> list[str]:
    lowered = unicodedata.normalize("NFKD", text.lower())
    stripped = "".join(ch for ch in lowered if not unicodedata.combining(ch))
    return [tok for tok in stripped.replace(",", " ").split() if len(tok) > 1]


@app.get("/health")
def health():
    return {"status": "ok", "service": "product-api", "catalog_version": CATALOG_VERSION}


@app.get("/products")
def list_products():
    return {"products": list(CATALOG.values())}


@app.get("/products/search")
def search_products(q: str, request: Request):
    context = extract(dict(request.headers))
    with trace.get_tracer("product-api").start_as_current_span(
        "product.search", context=context, kind=SpanKind.SERVER,
        attributes={"product.query": q},
    ) as span:
        time.sleep(0.04)
        tokens = _normalize(q)
        scored = []
        for product in CATALOG.values():
            haystack = _normalize(product["name"] + " " + " ".join(product["tags"]))
            overlap = sum(1 for tok in tokens if tok in haystack)
            if overlap:
                scored.append((overlap, product))
        scored.sort(key=lambda pair: (-pair[0], pair[1]["price"]))
        results = [p for _, p in scored[:3]]
        span.set_attribute("product.results", len(results))
        return {"status": "ok", "results": results}


@app.get("/products/{product_id}")
def get_product(product_id: str, request: Request):
    context = extract(dict(request.headers))
    with trace.get_tracer("product-api").start_as_current_span(
        "product.get", context=context, kind=SpanKind.SERVER,
        attributes={"product.id": product_id},
    ) as span:
        time.sleep(0.05)
        product = CATALOG.get(product_id)
        if product is None:
            span.set_attribute("product.found", False)
            raise HTTPException(status_code=404, detail="product not found")
        span.set_attribute("product.found", True)
        span.set_attribute("product.price", product["price"])
        span.set_attribute("product.stock", product["stock"])
        span.set_attribute("product.catalog.version", CATALOG_VERSION)
        return {"status": "ok", "product": product}


@app.post("/products/{product_id}/reserve")
async def reserve_product(product_id: str, request: Request):
    body = await request.json()
    quantity = int(body.get("quantity", 1))
    context = extract(dict(request.headers))
    with trace.get_tracer("product-api").start_as_current_span(
        "product.reserve", context=context, kind=SpanKind.SERVER,
        attributes={"product.id": product_id, "product.quantity": quantity},
    ) as span:
        time.sleep(0.03)
        product = CATALOG.get(product_id)
        if product is None:
            span.set_attribute("product.reserved", False)
            raise HTTPException(status_code=404, detail={"status": "declined", "reason": "not_found", "stage": "product_catalog"})
        if product["stock"] < quantity:
            span.set_attribute("product.reserved", False)
            span.set_attribute("product.stock", product["stock"])
            raise HTTPException(status_code=409, detail={"status": "declined", "reason": "out_of_stock", "stage": "inventory", "stock": product["stock"]})
        product["stock"] -= quantity
        span.set_attribute("product.reserved", True)
        span.set_attribute("product.stock", product["stock"])
        return {"status": "reserved", "product_id": product_id, "stock": product["stock"]}


@app.post("/products/{product_id}/release")
async def release_product(product_id: str, request: Request):
    body = await request.json()
    quantity = int(body.get("quantity", 1))
    context = extract(dict(request.headers))
    with trace.get_tracer("product-api").start_as_current_span(
        "product.release", context=context, kind=SpanKind.SERVER,
        attributes={"product.id": product_id, "product.quantity": quantity},
    ) as span:
        product = CATALOG.get(product_id)
        if product is not None:
            product["stock"] += quantity
        span.set_attribute("product.stock", product["stock"] if product else -1)
        return {"status": "released", "product_id": product_id, "stock": product["stock"] if product else 0}
