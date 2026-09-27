import time

from fastapi import FastAPI, HTTPException, Request
from opentelemetry import trace
from opentelemetry.propagate import extract
from opentelemetry.trace import SpanKind

from oaar_impl.otel_setup import configure_telemetry

configure_telemetry()

app = FastAPI(title="oaar-product-api")

CATALOG = {
    "sku-1234": {"id": "sku-1234", "name": "Fone Bluetooth ANC", "price": 299.90, "stock": 7},
    "sku-5678": {"id": "sku-5678", "name": "Teclado Mecanico 65%", "price": 449.00, "stock": 3},
    "sku-9012": {"id": "sku-9012", "name": "Monitor 27 4K", "price": 1899.00, "stock": 0},
}
CATALOG_VERSION = "3.1.0"


@app.get("/health")
def health():
    return {"status": "ok", "service": "product-api", "catalog_version": CATALOG_VERSION}


@app.get("/products")
def list_products():
    return {"products": list(CATALOG.values())}


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
