defmodule StoatGateway do
  use Application
  import Telemetry.Metrics

  @impl true
  def start(_type, _args) do
    :ets.new(:channel_server_refs, [:named_table, :set, :public])

    children = [
      {Peep,
       name: Stoat.Metrics.Peep,
       metrics: [
         last_value("vm.memory.total", unit: :byte),
         counter("gateway.session.init.count",
           event_name: [:gateway, :session, :init],
           tags: [:type]
         ),
         counter("gateway.consumer.process.count",
           event_name: [:gateway, :consumer, :process],
           tags: [:event, :type]
         ),
         counter("bandit.websocket.start.count",
           event_name: [:bandit, :websocket, :start]
         ),
         distribution("bandit.websocket.stop.duration",
           event_name: [:bandit, :websocket, :stop],
           measurement: :duration,
           unit: {:native, :millisecond}
         ),
         sum("bandit.websocket.recv.text_frame.count",
           event_name: [:bandit, :websocket, :stop],
           measurement: :recv_text_frame_count
         ),
         sum("bandit.websocket.recv.text_frame.bytes",
           event_name: [:bandit, :websocket, :stop],
           measurement: :recv_text_frame_bytes,
           unit: :byte
         ),
         sum("bandit.websocket.recv.binary_frame.count",
           event_name: [:bandit, :websocket, :stop],
           measurement: :recv_binary_frame_count
         ),
         sum("bandit.websocket.recv.binary_frame.bytes",
           event_name: [:bandit, :websocket, :stop],
           measurement: :recv_binary_frame_bytes,
           unit: :byte
         ),
         sum("bandit.websocket.send.text_frame.count",
           event_name: [:bandit, :websocket, :stop],
           measurement: :send_text_frame_count
         ),
         sum("bandit.websocket.send.text_frame.bytes",
           event_name: [:bandit, :websocket, :stop],
           measurement: :send_text_frame_bytes,
           unit: :byte
         ),
         sum("bandit.websocket.send.binary_frame.count",
           event_name: [:bandit, :websocket, :stop],
           measurement: :send_binary_frame_count
         ),
         sum("bandit.websocket.send.binary_frame.bytes",
           event_name: [:bandit, :websocket, :stop],
           measurement: :send_binary_frame_bytes,
           unit: :byte
         )
       ],
       global_tags: %{node: node()}},
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
       ]},
      {Redix, {Application.get_env(:stoat_gateway, :redis), [name: :redix]}},
      StoatGateway.Events.Consumer
    ]

    opts = [strategy: :one_for_one, name: StoatGateway.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
