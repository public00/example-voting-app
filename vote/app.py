from flask import Flask, jsonify, render_template, request, make_response, g
from redis import Redis
from pythonjsonlogger import jsonlogger
import os
import socket
import random
import json
import logging
import time

# --- Code-Level Resilience: Fail-Safe Import ---
try:
    # This module contains the tracing logic that uses opentelemetry
    from tracing_setup import get_current_traceparent, start_trace_span
except ImportError as e:
    # If tracing dependencies are missing (like during a Docker build error), 
    # we create dummy functions so the application runs without crashing.
    print(f"FATAL: Tracing setup failed: {e}. Running without distributed tracing.")
    
    # Define placeholder functions for graceful degradation
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

logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter('%(asctime)s %(levelname)s %(message)s')
logHandler.setFormatter(formatter)
app.logger.addHandler(logHandler)

gunicorn_error_logger = logging.getLogger('gunicorn.error')
app.logger.handlers.extend(gunicorn_error_logger.handlers)
app.logger.setLevel(logging.INFO)

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

    # Manually start a trace span using the imported function.
    with start_trace_span("vote-api-request") as span:
        try:
            content = request.json
            vote = content['vote']
            
            # The traceparent is now retrieved from the span we just created
            traceparent = get_current_traceparent()
            
            app.logger.info('Vote received via API', extra={
                'vote': vote, 
                'voter_id': voter_id,
                'traceparent_generated': traceparent if traceparent else "null"
            })

            redis = get_redis()
            
            # Inject Trace Context into Redis Payload
            data = json.dumps({
                'voter_id': voter_id, 
                'vote': vote,
                'traceparent': traceparent # <-- Use the generated trace
            })
            redis.rpush('votes', data)

            resp = jsonify(success=True, message="Vote cast")
            resp.set_cookie('voter_id', voter_id)
            return resp, 200

        except Exception as e:
            app.logger.error("API Error", extra={'error': str(e)})
            # If span is active, record the error
            if span:
                span.record_exception(e)
                span.set_attribute("error", True)
            return jsonify(success=False, error="Internal Server Error"), 500
    
@app.route("/", methods=['POST','GET'])
def hello():
    voter_id = request.cookies.get('voter_id')
    if not voter_id:
        voter_id = hex(random.getrandbits(64))[2:-1]

    vote = None

    # Manually start a trace span here too
    with start_trace_span("vote-homepage-request") as span:
        if request.method == 'POST':
            redis = get_redis()
            vote = request.form['vote']
            
            # The traceparent is now retrieved from the span we just created
            traceparent = get_current_traceparent()

            app.logger.info('Vote received', extra={
                'vote_choice': vote, 
                'voter_id': voter_id, 
                'app': 'vote-frontend',
                'traceparent_generated': traceparent
            })
            
            # Pass the generated traceparent to Redis
            data = json.dumps({
                'voter_id': voter_id, 
                'vote': vote,
                'traceparent': traceparent
            })
            redis.rpush('votes', data)

        resp = make_response(render_template(
            'index.html',
            option_a=option_a,
            option_b=option_b,
            hostname=hostname,
            vote=vote,
        ))
        resp.set_cookie('voter_id', voter_id)
        return resp

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80, debug=True, threaded=True)