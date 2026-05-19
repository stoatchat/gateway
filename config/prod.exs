import Config

config :stoat_gateway,
  ws_port: System.get_env("WS_PORT"),
  mongodb: System.get_env("MONGO_DB_URI"),
  rabbit: System.get_env("RABBITMQ_URI")
