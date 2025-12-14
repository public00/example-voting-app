# app.py (REWRITTEN FOR OTel LOGS)

from flask import Flask, jsonify, request, render_template, make_response, g
from redis import Redis
# REMOVED: from pythonjsonlogger import jsonlogger
import json
import random
import socket
import os
import logging
import time

# --- Code-Level Resilience: Fail-Safe Import ---
try:
    # Import instrument_flask (NEW)
    from tracing_setup import get_current_traceparent, start_trace_span, instrument_flask
except ImportError as e:
    print(f"FATAL: Tracing setup failed: {e}. Running without distributed tracing/logging.")
    
    # (Dummy functions remain for resilience)
    def get_current_traceparent(): return None
    def start_trace_span(span_name, kind=None):
        class DummySpan:
            def __enter__(self): return None
            def __exit__(self, exc_type, exc_val, exc_tb): pass
            def record_exception(self, e): pass
            def set_attribute(self, key, value): pass
        return DummySpan()
# ----------------------------------------------------

option_a = os.getenv('OPTION_A', "Cats")
option_b = os.getenv('OPTION_B', "Dogs")
hostname = socket.gethostname()

app = Flask(__name__)

# --- NEW: Call the Instrumentation Hook (Handles all OTel setup now) ---
instrument_flask(app)
# ----------------------------------------

# --- LOGGING SETUP (SIMPLIFIED) ---
# We no longer need the jsonlogger. The OTel LoggingHandler in tracing_setup
# takes over. We just need to get the app logger instance.

# NOTE: The OTel handler is configured in instrument_flask(app)
app.logger.setLevel(logging.INFO)
# ---------------------------------

def get_redis():
    if not hasattr(g, 'redis'):
        g.redis = Redis(host="redis", db=0, socket_timeout=5)
    return g.redis

@app.route("/health")
def health():
    try:
        redis = get_redis()
        redis.ping()
        app.logger.info("Health check successful") # OTel will capture this log
        return "OK", 200
    except Exception as e:
        # Note: OTel will correlate this error log to the trace automatically
        app.logger.error("Health check failed", extra={'error': str(e)}) 
        return "Unhealthy", 500

@app.route("/api/vote", methods=['POST'])
def cast_vote_api():
    voter_id = request.cookies.get('voter_id')
    if not voter_id:
        voter_id = hex(random.getrandbits(64))[2:-1]

    # Trace is automatically started by FlaskInstrumentor, and the span is retrieved implicitly for logging correlation.
    with start_trace_span("vote-api-request") as span:
        try:
            content = request.json
            vote = content['vote']
            
            # Since OTel logs automatically inject trace/span IDs, we only need to log relevant business data.
            # The manual traceparent extraction is no longer strictly necessary for log correlation.
            app.logger.info('Vote received via API', extra={
                'vote': vote,
                'voter_id': voter_id,
                # OTel automatically adds: 'otel.trace_id', 'otel.span_id'
            })

            redis = get_redis()
            
            # We still explicitly inject traceparent into the Redis payload for distributed tracing to a worker process.
            traceparent = get_current_traceparent()
            data = json.dumps({
                'voter_id': voter_id,
                'vote': vote,
                'traceparent': traceparent 
            })
            redis.rpush('votes', data)

            resp = jsonify(success=True, message="Vote cast")
            resp.set_cookie('voter_id', voter_id)
            return resp, 200

        except Exception as e:
            # This error log will be automatically correlated with the current trace/span
            app.logger.error("API Error", extra={'error': str(e)}) 
            if span:
                span.record_exception(e)
                span.set_attribute("error", True)
            return jsonify(success=False, error="Internal Server Error"), 500


@app.route("/", methods=['GET'])
def hello():
    voter_id = request.cookies.get('voter_id')
    if not voter_id:
        voter_id = hex(random.getrandbits(64))[2:-1]

    with start_trace_span("vote-homepage-request") as span:
        
        # OTel captures the implicit log from the / route
        app.logger.info("Rendering homepage")
        
        resp = make_response(render_template(
            'index.html',
            option_a=option_a,
            option_b=option_b,
            hostname=hostname,
            vote=None,
        ))
        resp.set_cookie('voter_id', voter_id)
        return resp


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80, debug=True, threaded=True)