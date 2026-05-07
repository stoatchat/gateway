defmodule StoatGateway.Events.Consumer do
  use Broadway
  require Logger

  def start_link(_) do
    Broadway.start_link(__MODULE__,
      name: StoatGateway.Events.Consumer,
      producer: [
        module:
          {BroadwayRabbitMQ.Producer,
           connection: Application.get_env(:stoat_gateway, :rabbit),
           queue: "stoat_events",
           qos: [prefetch_count: 10],
           on_failure: :reject},
        concurrency: 1
      ],
      processors: [default: [concurrency: 100]]
    )
  end

  defp decode_message!(data) when is_binary(data) do
    data |> Jason.decode()
  end

  @impl true
  def handle_message(_, message, _) do
    message
    |> Broadway.Message.update_data(&decode_message!/1)
    |> process_message()
  end

  defp process_message(%Broadway.Message{data: {:ok, data}} = message) do
    process_event(data)
    message
  end

  defp process_message(%Broadway.Message{data: {:error, reason}} = message) do
    Logger.error("Error Processing message error=#{reason}")
    IO.inspect(reason)
    message
  end

  # NOTE: Big problem here is what data some events handle
  # Biggest thing to figure out is presence and DMs
  # Presence:
  #   - Through API so there's handling there
  #   - Could have a GenServer per user_id which deduplicates so only one status
  #     - This GenServer then handles fanout (pubsub, through guilds, etc?)
  #   - Needs to be deduplicated as much as possible
  #   - In the future we can then remove this responsibility from Delta
  # DMs:
  #   - Same as a Server or just fetch recipients through Registry and let them handle it?
  #     TODO: Check a DM `Message` Event to finalise this
  #
  # Generally figure out quirks of current setup
  # Test relay is just `PSUBSCRIBE *` into RMQ queue

  # TODO: Determine nicest way, ideally we do map pattern matching once, easier readability
  def process_event(%{"type" => event_type} = data) do
    # TODO: wrap telemetry and otel context around this
    # Can also add future pushnotif decisions here?
    handle_event(event_type, data)
  end

  def handle_event("Message", %{"member" => %{"_id" => %{"server" => server_id}}} = payload) do
    server_fanout(server_id, {:Message, payload})
  end

  def handle_event("ChannelAck", %{"user" => user_id}=data) do
    session_fanout(user_id, {:ChannelAck, data})
  end
  
  # TODO: combine equal pattern match for fanout together
  def handle_event("UserSettingsUpdate", %{"id" => user_id} = data) do
    session_fanout(user_id, {:UserSettingsUpdate, data})
  end

  def handle_event("UserRelationship", %{"id" => user_id} = data) do
    session_fanout(user_id, {:UserRelationship, data})
  end

  def handle_event(event, payload) do
    Logger.info("Unhandled event=#{event} payload=#{inspect(payload)}")
  end

  def server_fanout(server_id, {event, data}) do 
    {:ok, server} = StoatGateway.Server.lookup_or_start(server_id)
    StoatGateway.Server.dispatch(server, event, data)
  end

  def session_fanout(user_id, payload) do
    sessions = Registry.lookup(Stoat.Sessions, user_id)

    Enum.each(sessions, fn {pid, _session_id} ->
      send(pid, {:socket_dispatch, payload})
    end)
  end
end
