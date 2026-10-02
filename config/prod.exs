import Config

config :stoat_gateway,
  ws_port: System.get_env("WS_PORT", "14703"),
  mongodb: System.get_env("MONGODB_URI", "mongodb://localhost:27017/"),
  rabbit: [
    host: System.get_env("RABBIT_HOST", "localhost"),
    port: System.get_env("RABBIT_PORT", "5672"),
    username: System.get_env("RABBIT_USERNAME", "rabbituser"),
    password: System.get_env("RABBIT_PASSWORD", "rabbitpass")
  ],
  redis: System.get_env("REDIS_URI", "redis://localhost:6379"),
  revolt: %{}

config :opentelemetry,
  resource: %{service: %{name: "stoat_gateway"}},
  span_processor: :batch,
  traces_exporter: :otlp,
  # 100% of traces
  sampler: {:trace_id_ratio_based, 1}

config :opentelemetry_exporter,
  otlp_protocol: :http_protobuf,
  otlp_endpoint: System.get_env("OTEL_ENDPOINT", "http://localhost:10428/insert/opentelemetry")
