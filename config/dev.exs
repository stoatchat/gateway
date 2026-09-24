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
        "early_adopter_cutoff" => 1784761200
      }  
    }
  }

config :libcluster,
  topologies: [
    dev_cluster: [
      strategy: Cluster.Strategy.Epmd,
      config: [hosts: [:"sgw-1@127.0.0.1", :"sgw-2@127.0.0.1"]]
    ]
  ]
