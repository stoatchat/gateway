import Config

config :stoat_gateway,
  ws_port: 4000,
  mongodb: "mongodb://localhost:27017/db",
  rabbit: "amqp://rabbituser:rabbitpass@localhost"
