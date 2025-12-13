# tracing_setup.py (NEW OTLP EXPORTER SETUP)

import logging
import random
import os
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.propagate import inject
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.semconv.resource import ResourceAttributes

trace_logger = logging.getLogger("tracing_debug")
trace_logger.setLevel(logging.INFO)

OTEL_EXPORTER_OTLP_ENDPOINT = os.getenv("DT_ENDPOINT_URL", "http://localhost:4318/v1/traces")
DT_API_TOKEN = os.getenv("DT_AUTH_TOKEN", "")

# 1. Configure the Resource (Dynatrace Service Name)
resource = Resource.create({
    ResourceAttributes.SERVICE_NAME: "Vote-Service",
    ResourceAttributes.DEPLOYMENT_ENVIRONMENT: "production"
})

tracer_provider = TracerProvider(resource=resource)

# 2. Configure the OTLP Exporter
exporter = OTLPSpanExporter(
    endpoint=OTEL_EXPORTER_OTLP_ENDPOINT,
    headers={"Api-Token": DT_API_TOKEN},
    timeout=5
)

# 3. Add the Span Processor to the Provider
span_processor = BatchSpanProcessor(exporter)
tracer_provider.add_span_processor(span_processor)

# 4. Set the global Tracer Provider
trace.set_tracer_provider(tracer_provider)
tracer = trace.get_tracer(__name__)
trace_logger.info("OpenTelemetry Tracer configured for direct OTLP export.")
# ------------------------------------------


# --- 2. WRAPPER FUNCTIONS (RETAINED FOR RESILIENCE) ---

def start_trace_span(span_name):
    """Starts a new OpenTelemetry span as the current active span."""
    trace_logger.info(f"Starting span: {span_name}")
    return tracer.start_as_current_span(span_name)

def get_current_traceparent():
    """
    Returns the W3C traceparent header representing the current span.
    Uses propagation or manually generates a valid ID if context is missing.
    """
    propagator = TraceContextTextMapPropagator()
    carrier = {}
    propagator.inject(carrier)
    w3c_header = carrier.get("traceparent", None)
    
    # Keep the manual ID generation fallback for resilience
    if not w3c_header or w3c_header.endswith("-0000000000000000-01"):
        try:
            # Manually generate a new W3C header
            trace_id = hex(random.getrandbits(128))[2:].zfill(32)
            span_id = hex(random.getrandbits(64))[2:].zfill(16)
            w3c_header = f"00-{trace_id}-{span_id}-01"
            trace_logger.warning(f"OTel context missing; manually generated: {w3c_header}")
        except Exception as e:
            trace_logger.error(f"Failed manual trace generation: {e}")
            return None
    
    return w3c_header

# --- 3. INSTRUMENTATION HOOK ---
def instrument_flask(app):
    """Instruments the Flask app to automatically trace web requests."""
    FlaskInstrumentor().instrument_app(app)
    trace_logger.info("Flask instrumentation applied.")