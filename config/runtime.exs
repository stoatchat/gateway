import Config

if config_env() == :prod do
  config :stoat_gateway,
    ws_port: System.get_env("WS_PORT", "4000"),
    mongodb: System.get_env("MONGODB_URI", "mongodb://localhost:27017/revolt"),
    rabbit: System.get_env("RABBITMQ_URI", "amqp://rabbituser:rabbitpass@localhost"),
    redis: System.get_env("REDIS_URI", "redis://localhost:6379")
end
