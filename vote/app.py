# app.py (CORRECTED FOR CLASS-BASED TRACING SETUP)

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
    # 1. Import the class itself
    from tracing_setup import TracingSetup 
    
    # 2. Instantiate the class globally (CRITICAL NEW STEP)
    tracing_config = TracingSetup()
    
    # 3. Assign the instance methods to the global names used throughout app.py
    #    (This maintains compatibility with the rest of your app)
    get_current_traceparent = tracing_config.get_current_traceparent
    start_trace_span = tracing_config.start_trace_span
    instrument_flask = tracing_config.instrument_flask # FIXES NameError
    
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
    # Need to define a dummy for instrument_flask too, otherwise line 38 fails
    def instrument_flask(app): pass 
# ----------------------------------------------------

option_a = os.getenv('OPTION_A', "Cats")
option_b = os.getenv('OPTION_B', "Dogs")
hostname = socket.gethostname()

app = Flask(__name__)

# --- NEW: Call the Instrumentation Hook (Handles all OTel setup now) ---
# This line now successfully calls the function defined in the try/except block above.
instrument_flask(app)
# ----------------------------------------

# --- LOGGING SETUP (SIMPLIFIED) ---
# ... (rest of the logging setup remains the same)
app.logger.setLevel(logging.INFO)
# ---------------------------------

# ... (rest of the application code remains the same) ...

def get_redis():
    # ...
    # Uses Redis
    # ...

@app.route("/health")
def health():
    # ...
    # Uses app.logger.info
    # ...

@app.route("/api/vote", methods=['POST'])
def cast_vote_api():
    # ...
    # Uses start_trace_span and get_current_traceparent
    # ...
    
@app.route("/", methods=['GET'])
def hello():
    # ...
    # Uses start_trace_span
    # ...

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80, debug=True, threaded=True)