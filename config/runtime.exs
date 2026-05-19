import Config

if config_env() == :prod do
  config :stoat_gateway,
    ws_port: System.get_env("WS_PORT"),
    mongodb: System.get_env("MONGODB_URI"),
    rabbit: System.get_env("RABBITMQ_URI")
end
