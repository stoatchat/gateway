import Config

if config_env() == :prod do
  revolt =
    case Toml.decode_file("Revolt.toml") do
      {:ok, config} -> config
      {:error, _} -> %{}
    end

  config :stoat_gateway,
    ws_port: System.get_env("WS_PORT", "14703"),
    mongodb:
      get_in(revolt, ["database", "mongodb"]) ||
        System.get_env("MONGODB_URI", "mongodb://localhost:27017/"),
    rabbit: [
      host: get_in(revolt, ["rabbit", "host"]) || System.get_env("RABBIT_HOST", "localhost"),
      port: get_in(revolt, ["rabbit", "port"]) || System.get_env("RABBIT_PORT", "5672"),
      username:
        get_in(revolt, ["rabbit", "username"]) ||
          System.get_env("RABBIT_USERNAME", "rabbituser"),
      password:
        get_in(revolt, ["rabbit", "password"]) ||
          System.get_env("RABBIT_PASSWORD", "rabbitpass")
    ],
    redis:
      get_in(revolt, ["database", "redis"]) ||
        System.get_env("REDIS_URI", "redis://localhost:6379"),
    revolt: revolt

  config :opentelemetry,
    resource: %{service: %{name: "stoat_gateway"}},
    span_processor: :batch,
    traces_exporter: :otlp,
    # 100% of traces
    sampler: {:trace_id_ratio_based, 1}

  config :opentelemetry_exporter,
    otlp_protocol: :http_protobuf,
    otlp_endpoint: System.get_env("OTEL_ENDPOINT", "http://localhost:10428/insert/opentelemetry")
end
