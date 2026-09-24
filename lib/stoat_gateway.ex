defmodule StoatGateway do
  use Application

  @impl true
  def start(_type, _args) do
    :ets.new(:channel_server_refs, [:named_table, :set, :public])

    children = [
      StoatGateway.Telemetry.Supervisor,
      %{id: :presence_group, start: {:pg, :start_link, [:presence]}},
      %{id: :gdm_group, start: {:pg, :start_link, [:gdm_channels]}},
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
       [
         name: :mongo_db,
         url: Application.get_env(:stoat_gateway, :mongodb),
         database: "revolt",
         pool_size: 2
       ] ++ mongo_ssl_opts()},
      {Redix, {Application.get_env(:stoat_gateway, :redis), [name: :redix]}},
      StoatGateway.Events.Consumer,
      StoatGateway.Remote.Coordinator,
      {Cluster.Supervisor,
        [Application.get_env(:libcluster, :topologies), [name: StoatGateway.ClusterSupervisor]]}
    ]

    opts = [strategy: :one_for_one, name: StoatGateway.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp mongo_ssl_opts do
    with url when is_binary(url) <- Application.get_env(:stoat_gateway, :mongodb),
         query when is_binary(query) <- URI.parse(url).query,
         %{"tlsCAFile" => ca_file} <- URI.decode_query(query) do
      [ssl_opts: [cacertfile: ca_file]]
    else
      _ -> []
    end
  end
end
