import Config

config :stoat_gateway,
  ws_port: System.get_env("WS_PORT", "14703"),
  mongodb: "mongodb://localhost:27017/",
  rabbit: [
    host: "localhost",
    port: "5672",
    username: "rabbituser",
    password: "rabbitpass"
  ],
  redis: "redis://localhost:6379",
  revolt: %{
    "api" => %{
      "users" => %{
        "early_adopter_cutoff" => 1_784_761_200
      }
    }
  }

config :opentelemetry, traces_exporter: {:otel_exporter_stdout, []}
