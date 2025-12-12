from opentelemetry import trace
from opentelemetry.trace import get_tracer

# Initialize a tracer instance. This relies on the Dynatrace OneAgent
tracer = get_tracer(__name__)

# Helper to get the W3C Traceparent string for propagation
def get_current_traceparent():
    span = trace.get_current_span()
    
    # Check if a valid trace is active
    if span and span.get_span_context().is_valid:
        ctx = span.get_span_context()
        # W3C format: 00-TraceID-SpanID-01 (version, trace-id, parent-id, flags)
        return f"00-{ctx.trace_id:032x}-{ctx.span_id:016x}-01"
    
    return None

def start_trace_span(span_name, kind=trace.SpanKind.SERVER):
    return tracer.start_as_current_span(span_name, kind=kind)