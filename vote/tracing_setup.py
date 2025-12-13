# tracing_setup.py
# ---------------------------------------------------
# OpenTelemetry + Dynatrace OneAgent Hook Setup
# Provides:
#  - tracer = get_tracer(__name__)
#  - start_trace_span(name)
#  - get_current_traceparent() (Full W3C header for Redis)
# ---------------------------------------------------

from opentelemetry import trace
from opentelemetry.trace import get_tracer
from opentelemetry.propagate import inject
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
import logging
import random # Required for manual ID generation if agent fails
import time

# ---------------------------------------
# CONFIGURE LOGGING
# ---------------------------------------
trace_logger = logging.getLogger("tracing_debug")
trace_logger.setLevel(logging.INFO)

# ---------------------------------------
# 1. TRACER SETUP (rely on OneAgent hook)
# ---------------------------------------
# We intentionally skip calling trace.set_tracer_provider() or defining the
# OTLP exporter, as the Dynatrace OneAgent will hook the global provider 
# upon application startup.

tracer = get_tracer(__name__)
trace_logger.info("OpenTelemetry tracer retrieved. Relying on OneAgent hook for export.")

# ---------------------------------------
# 2. Start a span helper
# ---------------------------------------
def start_trace_span(span_name):
    """
    Starts a new OpenTelemetry span as the current active span.
    """
    trace_logger.info(f"Starting span: {span_name}")
    return tracer.start_as_current_span(span_name)


# ---------------------------------------
# 3. Generate W3C traceparent header for Redis/HTTP (PROPAGATION)
# ---------------------------------------
def get_current_traceparent():
    """
    Returns the W3C traceparent header representing the current span.
    Guarantees a valid W3C header by generating one if the agent fails to provide context.
    """
    propagator = TraceContextTextMapPropagator()
    carrier = {}
    
    # Use the standard OTEL inject method to get the current context
    propagator.inject(carrier)
    
    w3c_header = carrier.get("traceparent", None)
    
    # FIX APPLIED HERE: If the context is missing or all zeros, generate a new one.
    # This prevents 'null' from being sent to Redis.
    if not w3c_header or w3c_header.endswith("-0000000000000000-01"):
        try:
            # Generate a new W3C header for propagation continuity
            trace_id = hex(random.getrandbits(128))[2:].zfill(32)
            span_id = hex(random.getrandbits(64))[2:].zfill(16)
            w3c_header = f"00-{trace_id}-{span_id}-01"
            trace_logger.warning(f"Agent context missing; manually generated: {w3c_header}")
        except Exception as e:
            trace_logger.error(f"Failed manual trace generation: {e}")
            return None
    
    return w3c_header


# ---------------------------------------
# 4. Extract RAW Trace ID for Log Correlation
#    FUNCTION REMOVED: Logic moved to app.py for guaranteed integrity.
# ---------------------------------------
# def get_current_trace_id_raw():
#     ...
#     return None