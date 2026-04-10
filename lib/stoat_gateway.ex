defmodule StoatGateway do
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Registry, keys: :unique, name: Stoat.Servers},
      {Registry, keys: :unique, name: Stoat.Sessions},
      {DynamicSupervisor, name: Stoat.Sessions.Supervisor, strategy: :one_for_one},
      {DynamicSupervisor, name: Stoat.Servers.Supervisor, strategy: :one_for_one},
      {Bandit,
       plug: StoatGateway.Web.Router,
       scheme: :http,
       port: Application.get_env(:stoat_gateway, :ws_port)},
      {Mongo,
       [name: :mongo_db, url: Application.get_env(:stoat_gateway, :mongodb), pool_size: 2]},
      StoatGateway.Events.Consumer
    ]

    opts = [strategy: :one_for_one, name: StoatGateway.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
