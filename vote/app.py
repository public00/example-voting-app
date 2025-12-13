from flask import Flask, jsonify, request, render_template, make_response, g
from redis import Redis
from pythonjsonlogger import jsonlogger
import json
import random
import socket
import os
import logging
import time

# --- Code-Level Resilience: Fail-Safe Import ---
try:
    # Import all necessary tracing functions, including the new raw ID extractor
    from tracing_setup import get_current_traceparent, start_trace_span
except ImportError as e:
    # Define placeholder functions for graceful degradation
    print(f"FATAL: Tracing setup failed: {e}. Running without distributed tracing.")
    
    def get_current_traceparent(): return None
    def start_trace_span(span_name, kind=None):
        class DummySpan:
            def __enter__(self): return None
            def __exit__(self, exc_type, exc_val, exc_tb): pass
            def record_exception(self, e): pass
            def set_attribute(self, key, value): pass
        return DummySpan()
# We don't need get_current_trace_id_raw() anymore, it was causing problems.
# ----------------------------------------------------

option_a = os.getenv('OPTION_A', "Cats")
option_b = os.getenv('OPTION_B', "Dogs")
hostname = socket.gethostname()

app = Flask(__name__)

# --- JSON Logger Setup ---
logHandler = logging.StreamHandler()
# Ensure the formatter handles the JSON structure correctly
formatter = jsonlogger.JsonFormatter('%(asctime)s %(levelname)s %(message)s') 
logHandler.setFormatter(formatter)
app.logger.addHandler(logHandler)

gunicorn_error_logger = logging.getLogger('gunicorn.error')
app.logger.handlers.extend(gunicorn_error_logger.handlers)
app.logger.setLevel(logging.INFO)
# -------------------------

def get_redis():
    if not hasattr(g, 'redis'):
        g.redis = Redis(host="redis", db=0, socket_timeout=5)
    return g.redis

@app.route("/health")
def health():
    try:
        redis = get_redis()
        redis.ping()
        return "OK", 200
    except Exception as e:
        app.logger.error("Health check failed", extra={'error': str(e)})
        return "Unhealthy", 500

@app.route("/api/vote", methods=['POST'])
def cast_vote_api():
    voter_id = request.cookies.get('voter_id')
    if not voter_id:
        voter_id = hex(random.getrandbits(64))[2:-1] 

    with start_trace_span("vote-api-request") as span:
        try:
            content = request.json
            vote = content['vote']
            
            # 1. Get the full W3C header for propagation via Redis
            # This is guaranteed to be a valid W3C header string.
            traceparent = get_current_traceparent()
            
            # 2. Extract the raw Trace ID for local logging (FIXED LOGIC)
            if traceparent:
                try:
                    # W3C format is: 00-TRACEID-SPANID-01. We want the second element.
                    raw_trace_id_for_log = traceparent.split('-')[1]
                except:
                    raw_trace_id_for_log = "parsing_error"
            else:
                raw_trace_id_for_log = "null"

            app.logger.info('Vote received via API', extra={
                'vote': vote, 
                'voter_id': voter_id,
                # THIS LOGGED ID NOW MATCHES THE ONE SENT TO REDIS
                'traceparent_generated': raw_trace_id_for_log
            })

            redis = get_redis()
            
            # Inject the final, guaranteed Trace Context into Redis Payload
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
            app.logger.error("API Error", extra={'error': str(e)})
            if span:
                span.record_exception(e)
                span.set_attribute("error", True)
            return jsonify(success=False, error="Internal Server Error"), 500


@app.route("/", methods=['GET'])
def hello():
    """Renders the homepage."""
    voter_id = request.cookies.get('voter_id')
    if not voter_id:
        voter_id = hex(random.getrandbits(64))[2:-1]

    with start_trace_span("vote-homepage-request") as span:
        
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