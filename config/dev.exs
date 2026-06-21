import Config

config :stoat_gateway,
  ws_port: 4000,
  mongodb: "mongodb://localhost:27017/revolt",
  rabbit: "amqp://rabbituser:rabbitpass@localhost",
  redis: "redis://localhost:6379"

config :libcluster,
  topologies: [
    dev_cluster: [
      strategy: Cluster.Strategy.Epmd,
      config: [hosts: [:"sgw-1@127.0.0.1", :"sgw-2@127.0.0.1"]]
    ]
  ]
