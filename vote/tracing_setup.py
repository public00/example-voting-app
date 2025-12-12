from opentelemetry import trace
from opentelemetry.trace import get_tracer
import logging 

# Setup a dedicated logger for troubleshooting tracing issues
trace_logger = logging.getLogger('tracing_debug')
trace_logger.setLevel(logging.INFO) 

# Initialize a tracer instance. This relies on the Dynatrace OneAgent
# We assume the Python logging framework is already configured in app.py
tracer = get_tracer(__name__)

# Log the initial state of the tracer provider
if trace.get_tracer_provider():
    trace_logger.info(f"Tracer Provider initialized successfully: {type(trace.get_tracer_provider())}")
else:
    trace_logger.error("Tracer Provider is NOT initialized. The Dynatrace OneAgent hook may have failed.")


# Helper to get the W3C Traceparent string for propagation
def get_current_traceparent():
    span = trace.get_current_span()
    
    # Check if a valid trace is active
    if span and span.get_span_context().is_valid:
        ctx = span.get_span_context()
        trace_logger.info(f"Span is valid. Trace ID: {ctx.trace_id:032x}")
        # W3C format: 00-TraceID-SpanID-01 (version, trace-id, parent-id, flags)
        return f"00-{ctx.trace_id:032x}-{ctx.span_id:016x}-01"
    
    else:
        # Log why the trace is not active
        if span:
            trace_logger.error(f"Current span is invalid. Context validity: {span.get_span_context().is_valid}. Likely creation failure.")
        else:
            trace_logger.error("No current span available (trace generation failed).")
            
    return None

def start_trace_span(span_name, kind=trace.SpanKind.SERVER):
    # Log before starting the span
    trace_logger.info(f"Attempting to start span: {span_name}")
    
    # Start the span
    span = tracer.start_as_current_span(span_name, kind=kind)
    
    # Log after starting the span
    if span.get_span_context().is_valid:
        trace_logger.info(f"Span '{span_name}' started successfully.")
    else:
        trace_logger.error(f"Span '{span_name}' started, but context is invalid. Agent integration failure suspected.")
        
    return span