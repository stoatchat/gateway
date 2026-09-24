defmodule StoatGateway.Telemetry.Supervisor do
  use Supervisor

  import Telemetry.Metrics

  def start_link(init_arg) do
    Supervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl true
  def init(_init_arg) do
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
       global_tags: %{node: node()}}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
