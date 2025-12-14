# tracing_setup.py (REWRITTEN FOR OTel TRACES & LOGS)

import logging
import random
import os

# --- TRACE IMPORTS ---
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.propagate import inject
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.semconv.resource import ResourceAttributes
# ---------------------

# --- LOG IMPORTS (NEW) ---
from opentelemetry._logs import set_logger_provider
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
# -------------------------

# --- SHARED IMPORTS ---
from opentelemetry.sdk.resources import Resource
# ----------------------


# --- CONFIGURATION ---

# NOTE: Dynatrace uses the same endpoint for traces and logs, but different paths
OTEL_EXPORTER_OTLP_TRACE_ENDPOINT = os.getenv("DT_TRACE_ENDPOINT_URL", "http://localhost:4318/v1/traces")
OTEL_EXPORTER_OTLP_LOGS_ENDPOINT = os.getenv("DT_LOGS_ENDPOINT_URL", "http://localhost:4318/v1/logs")
DT_API_TOKEN = os.getenv("DT_AUTH_TOKEN", "")

HEADERS = {"Api-Token": DT_API_TOKEN}
RESOURCE_ATTRIBUTES = {
    ResourceAttributes.SERVICE_NAME: "Vote-Service",
    ResourceAttributes.DEPLOYMENT_ENVIRONMENT: "production"
}
resource = Resource.create(RESOURCE_ATTRIBUTES)

# ------------------------------------------
trace_logger = logging.getLogger("tracing_debug")
trace_logger.setLevel(logging.INFO)
# ------------------------------------------


## 1. OPENTELEMETRY TRACE SETUP

tracer_provider = TracerProvider(resource=resource)
trace_exporter = OTLPSpanExporter(
    endpoint=OTEL_EXPORTER_OTLP_TRACE_ENDPOINT,
    headers=HEADERS,
    timeout=5
)
span_processor = BatchSpanProcessor(trace_exporter)
tracer_provider.add_span_processor(span_processor)
trace.set_tracer_provider(tracer_provider)
tracer = trace.get_tracer(__name__)
trace_logger.info("OpenTelemetry Tracer configured for direct OTLP export.")


## 2. OPENTELEMETRY LOG SETUP (NEW)

logger_provider = LoggerProvider(resource=resource)
set_logger_provider(logger_provider)

log_exporter = OTLPLogExporter(
    endpoint=OTEL_EXPORTER_OTLP_LOGS_ENDPOINT,
    headers=HEADERS,
    timeout=5
)
log_processor = BatchLogRecordProcessor(log_exporter)
logger_provider.add_log_record_processor(log_processor)

# This handler redirects all Python logging calls to the OTel Log Exporter
otel_handler = LoggingHandler(logger_provider=logger_provider)
trace_logger.info("OpenTelemetry Logger configured for direct OTLP export.")
# ------------------------------------------


# --- 3. WRAPPER FUNCTIONS (UPDATED) ---

def start_trace_span(span_name):
    """Starts a new OpenTelemetry span as the current active span."""
    return tracer.start_as_current_span(span_name)

def get_current_traceparent():
    """
    Returns the W3C traceparent header representing the current span.
    (Implementation remains the same: injects current context)
    """
    propagator = TraceContextTextMapPropagator()
    carrier = {}
    propagator.inject(carrier)
    w3c_header = carrier.get("traceparent", None)
    
    # Fallback/resilience logic remains the same (removed from display for brevity)
    if not w3c_header or w3c_header.endswith("-0000000000000000-01"):
        try:
            # Manually generate a new W3C header
            trace_id = hex(random.getrandbits(128))[2:].zfill(32)
            span_id = hex(random.getrandbits(64))[2:].zfill(16)
            w3c_header = f"00-{trace_id}-{span_id}-01"
        except Exception:
            return None
    
    return w3c_header

# --- 4. INSTRUMENTATION HOOK (UPDATED) ---
def instrument_flask(app):
    """Instruments the Flask app to automatically trace web requests AND set up OTel logging."""
    
    # Trace instrumentation
    FlaskInstrumentor().instrument_app(app)
    
    # Log instrumentation (NEW)
    # The default Flask logger is named 'flask.app', so we target it.
    flask_logger = logging.getLogger('flask.app')
    
    # 1. Remove the old JSON handler (which only printed to stdout)
    flask_logger.handlers.clear()
    
    # 2. Add the OTel handler (which forwards to the OTLP Exporter)
    flask_logger.addHandler(otel_handler)
    flask_logger.setLevel(logging.INFO)
    
    # Also forward Gunicorn logs (if used) to OTel
    gunicorn_error_logger = logging.getLogger('gunicorn.error')
    gunicorn_error_logger.handlers.clear()
    gunicorn_error_logger.addHandler(otel_handler)
    gunicorn_error_logger.setLevel(logging.INFO)

    trace_logger.info("Flask and Log instrumentation applied.")