defmodule StoatGateway do
  use Application

  @impl true
  def start(_type, _args) do
    # TODO: investigate perf implications of object dedup on insert
    # likely this whole logic will die once we can get the right fields in events
    :ets.new(:gdm_subscriptions, [:named_table, :bag, :public])
    :ets.new(:channel_server_refs, [:named_table, :set, :public])

    children = [
      {Registry, keys: :unique, name: Stoat.Servers},
      {Registry, keys: :duplicate, name: Stoat.Sessions},
      {Registry, keys: :unique, name: Stoat.Presence},
      {DynamicSupervisor, name: Stoat.Sessions.Supervisor, strategy: :one_for_one},
      {DynamicSupervisor, name: Stoat.Servers.Supervisor, strategy: :one_for_one},
      {DynamicSupervisor, name: Stoat.Presence.Supervisor, strategy: :one_for_one},
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
