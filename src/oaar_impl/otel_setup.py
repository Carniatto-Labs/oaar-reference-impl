import logging
import os

from opentelemetry import _logs, metrics, trace
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

_configured = False

EXPORT_INTERVAL_MILLIS = 5000
EXPORT_TIMEOUT_SECONDS = 5


def _resource() -> Resource:
    return Resource.create(
        {
            "service.name": os.getenv("OTEL_SERVICE_NAME", "oaar-impl"),
            "service.version": os.getenv("OAAR_SERVICE_VERSION", "0.1.0"),
            "service.namespace": "oaar",
        }
    )


def _endpoint() -> str:
    return os.getenv("OAAR_OTLP_ENDPOINT", "http://otel-collector:4318").rstrip("/")


def configure_telemetry() -> None:
    global _configured
    if _configured:
        return
    endpoint = _endpoint()
    resource = _resource()

    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(
        BatchSpanProcessor(
            OTLPSpanExporter(endpoint=f"{endpoint}/v1/traces", timeout=EXPORT_TIMEOUT_SECONDS)
        )
    )
    trace.set_tracer_provider(tracer_provider)

    meter_provider = MeterProvider(
        resource=resource,
        metric_readers=[
            PeriodicExportingMetricReader(
                OTLPMetricExporter(
                    endpoint=f"{endpoint}/v1/metrics", timeout=EXPORT_TIMEOUT_SECONDS
                ),
                export_interval_millis=EXPORT_INTERVAL_MILLIS,
            )
        ],
    )
    metrics.set_meter_provider(meter_provider)

    logger_provider = LoggerProvider(resource=resource)
    logger_provider.add_log_record_processor(
        BatchLogRecordProcessor(
            OTLPLogExporter(endpoint=f"{endpoint}/v1/logs", timeout=EXPORT_TIMEOUT_SECONDS)
        )
    )
    _logs.set_logger_provider(logger_provider)

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    if not any(isinstance(h, LoggingHandler) for h in root.handlers):
        root.addHandler(LoggingHandler(level=logging.INFO, logger_provider=logger_provider))
    if not any(isinstance(h, logging.StreamHandler) for h in root.handlers):
        root.addHandler(logging.StreamHandler())
    _configured = True


def shutdown_telemetry() -> None:
    global _configured
    for provider in (
        trace.get_tracer_provider(),
        metrics.get_meter_provider(),
        _logs.get_logger_provider(),
    ):
        try:
            provider.shutdown()
        except Exception:
            pass
    _configured = False
