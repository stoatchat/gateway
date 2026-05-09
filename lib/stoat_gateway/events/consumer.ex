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

  # NOTE: Big problem here is what data some events provide
  # Ideally:
  # - We have channel type to reduce look ups
  # - Channels in a server have a server_id key
  # Test relay is just `PSUBSCRIBE *` into RMQ queue

  def process_event(%{"type" => event_type} = data) do
    # TODO: wrap telemetry and otel context around this
    # Can also add future pushnotif decisions here?
    handle_event(event_type, data)
  end
  
  # Custom logic to pattern handle messages in servers & dm channels
  def handle_event("Message", %{"member" => %{"_id" => %{"server" => server_id}}} = data) do
    server_fanout(server_id, {:Message, data})
  end
  
  # Channel scoped events
  def handle_event("MessageUpdate", data), do: handle_channel_event(:MessageUpdate, data)
  def handle_event("MessageAppend", data), do: handle_channel_event(:MessageAppend, data)
  def handle_event("MessageDelete", data), do: handle_channel_event(:MessageDelete, data)
  def handle_event("MessageReact", data), do: handle_channel_event(:MessageReact, data)
  def handle_event("MessageUnreact", data), do: handle_channel_event(:MessageUnreact, data)
  def handle_event("MessageRemoveReaction", data), do: handle_channel_event(:MessageRemoveReaction, data)
  def handle_event("BulkMessageDelete", data), do: handle_channel_event(:BulkMessageDelete, data)
  
  # Server scoped events
  def handle_event("ServerCreate", data) do
    # TODO: Start GenServer + Link with Owner Sessions
  end

  def handle_event("ServerUpdate", data), do: handle_server_event(:ServerUpdate, data) 
  def handle_event("ServerDelete", data), do: handle_server_event(:ServerDelete, data)
  def handle_event("ServerMemberUpdate", data), do: handle_server_event(:ServerMemberUpdate, data)
  def handle_event("ServerMemberJoin", data), do: handle_server_event(:ServerMemberJoin, data)
  def handle_event("ServerMemberLeave", data), do: handle_server_event(:ServerMemberLeave, data)
  def handle_event("ServerRoleUpdate", data), do: handle_server_event(:ServerRoleUpdate, data)
  def handle_event("ServerRoleDelete", data), do: handle_server_event(:ServerRoleDelete, data)
  def handle_event("ServerRoleRanksUpdate", data), do: handle_server_event(:ServerRoleRanksUpdate, data)

  # User scoped events
  #   Presence Events

  def handle_event("ChannelAck", data), do: handle_user_event(:ChannelAck, data)

  def handle_event("UserUpdate", data), do: handle_presence_event(:UserUpdate, data)
  def handle_event("UserSettingsUpdate", data), do: handle_user_event(:UserSettingsUpdate, data) 
  def handle_event("UserRelationship", data), do: handle_user_event(:UserRelationship, data)
  def handle_event("UserPlatformWipe", data), do: nil

  def handle_event(event, data) do
    Logger.info("Unhandled event=#{event} payload=#{inspect(data)}")
  end

  defp handle_channel_event(event, %{"channel" => channel_id}) do
    # TODO: Check against server mapping or push out to gdm subscriptions
    # both ets tables so might slow things down
    # in future we can move away from the redis pubsub baked architecture and include more in the event from delta
  end

  defp handle_presence_event(event, %{"id" => user_id} = _data) do
    # TODO: Find presence in registry and cast :dispatch
  end
  
  defp handle_server_event(event, %{"id" => server_id} = data) do
    server_fanout(server_id, {event, data})
  end

  defp handle_user_event(event, %{"id" => user_id} = data) do
    Logger.debug("Pushing user event to sessions")
    session_fanout(user_id, {event, data})
  end

  defp server_fanout(server_id, {event, data}) do 
    {:ok, server} = StoatGateway.Server.lookup_or_start(server_id)
    StoatGateway.Server.dispatch(server, event, data)
  end

  defp session_fanout(user_id, data) do
    sessions = Registry.lookup(Stoat.Sessions, user_id)

    Enum.each(sessions, fn {pid, session_id} ->
      Logger.debug("fanning out to session #{inspect(pid)} id #{session_id}")
      send(pid, {:socket_dispatch, data})
    end)
  end
end
