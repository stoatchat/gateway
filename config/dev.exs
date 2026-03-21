import Config

config :stoat_gateway,
  ws_port: 4000,
  mongodb: "mongodb://localhost:27017/revolt",
  rabbit: "amqp://rabbituser:rabbitpass@localhost"
