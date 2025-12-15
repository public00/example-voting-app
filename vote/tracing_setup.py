import logging
import random
import os

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter 
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.instrumentation.redis import RedisInstrumentor 
from opentelemetry.propagate import inject
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.semconv.resource import ResourceAttributes

from opentelemetry._logs import set_logger_provider
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter 

from opentelemetry.sdk.resources import Resource

class TracingSetup:
    def __init__(self):
        
        self.otlp_endpoint = os.getenv(
            "OTEL_EXPORTER_OTLP_ENDPOINT",
            "http://dynatrace-otel-collector:4318" 
        )
        
        self.resource = Resource.create({
            ResourceAttributes.SERVICE_NAME: "Vote-Service",
            ResourceAttributes.DEPLOYMENT_ENVIRONMENT: "production"
        })

        # --- Logger for setup messages ---
        self.logger = logging.getLogger("tracing_setup")
        self.logger.setLevel(logging.INFO)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        formatter = logging.Formatter("[%(levelname)s] %(asctime)s - %(message)s")
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

        self.logger.info(f"OTLP Collector endpoint: {self.otlp_endpoint}")
        
        # Initialize OpenTelemetry
        self._setup_tracer()
        self._setup_logger()
        self._instrument_libraries()

    def _setup_tracer(self):
        try:
            self.tracer_provider = TracerProvider(resource=self.resource)
            trace_exporter = OTLPSpanExporter(
                endpoint=self.otlp_endpoint,
                timeout=5
            )
            span_processor = BatchSpanProcessor(trace_exporter)
            self.tracer_provider.add_span_processor(span_processor)
            trace.set_tracer_provider(self.tracer_provider)
            self.tracer = trace.get_tracer(__name__)
            self.logger.info("Tracer provider initialized successfully.")
        except Exception as e:
            self.logger.exception(f"Error initializing tracer: {e}")

    def _setup_logger(self):
        try:
            self.logger_provider = LoggerProvider(resource=self.resource)
            set_logger_provider(self.logger_provider)
            
            log_exporter = OTLPLogExporter(
                endpoint=self.otlp_endpoint,
                timeout=5
            )
            log_processor = BatchLogRecordProcessor(log_exporter)
            self.logger_provider.add_log_record_processor(log_processor)

            self.otel_handler = LoggingHandler(logger_provider=self.logger_provider)
            self.logger.info("Logger provider initialized successfully.")
        except Exception as e:
            self.logger.exception(f"Error initializing logger: {e}")

    # INnstrument non-web libraries (like Redis) ---
    def _instrument_libraries(self):
        self.logger.info("Instrumenting Redis.")
        try:
            RedisInstrumentor().instrument() # add redis spans
            self.logger.info("Redis instrumentation applied successfully.")
        except Exception as e:
            self.logger.exception(f"Error instrumenting Redis: {e}")
    # -------------------------------------------------------------

    def start_trace_span(self, span_name):
        self.logger.info(f"Starting span: {span_name}")
        return self.tracer.start_as_current_span(span_name)

    def get_current_traceparent(self):
        propagator = TraceContextTextMapPropagator()
        carrier = {}
        propagator.inject(carrier)
        w3c_header = carrier.get("traceparent", None)

        if not w3c_header or w3c_header.endswith("-0000000000000000-01"):
            self.logger.info("No current traceparent found, generating manually.")
            try:
                trace_id = hex(random.getrandbits(128))[2:].zfill(32)
                span_id = hex(random.getrandbits(64))[2:].zfill(16)
                w3c_header = f"00-{trace_id}-{span_id}-01"
                self.logger.info(f"Generated W3C traceparent: {w3c_header}")
            except Exception as e:
                self.logger.exception(f"Error generating traceparent: {e}")
                return None

        self.logger.info(f"Current traceparent: {w3c_header}")
        return w3c_header

    def instrument_flask(self, app):
        self.logger.info("Instrumenting Flask app for traces and logs.")
        try:
            FlaskInstrumentor().instrument_app(app)
            flask_logger = logging.getLogger('flask.app')
            flask_logger.handlers.clear()
            flask_logger.addHandler(self.otel_handler)
            flask_logger.setLevel(logging.INFO)

            gunicorn_logger = logging.getLogger('gunicorn.error')
            gunicorn_logger.handlers.clear()
            gunicorn_logger.addHandler(self.otel_handler)
            gunicorn_logger.setLevel(logging.INFO)

            self.logger.info("Flask and Gunicorn instrumentation applied successfully.")
        except Exception as e:
            self.logger.exception(f"Error instrumenting Flask: {e}")