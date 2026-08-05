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
           queue: "internal.event",
           metadata: [:headers],
           declare: [{:durable, true}],
           bindings: [
             {"revolt.default", [{:routing_key, "internal.event"}]}
           ],
           qos: [prefetch_count: 10],
           on_failure: :reject},
        concurrency: 1
      ],
      processors: [default: [concurrency: 100]]
    )
  end

  defp decode_message!(data) when is_binary(data) do
    # Maybe switch to binary_term/2 with safe - does incur a performance hit
    :erlang.binary_to_term(data)
  end

  @impl true
  def handle_message(_, message, _) do
    message
    |> Broadway.Message.update_data(&decode_message!/1)
    |> process_message()
  end

  defp process_message(
         %Broadway.Message{data: data, metadata: %{headers: headers}} =
           message
       ) do
    route_key =
      Enum.find_value(headers, fn {"c", _, route_key} -> route_key end)

    Logger.debug(
      "consumer: process_event: route_key=#{inspect(route_key)} event=#{inspect(data)}"
    )

    process_event(data, route_key)
    message
  end

  # p_broadcast: events with an array of channels
  def process_event(%{"type" => event_type} = data, route_keys) when is_list(route_keys) do
    :telemetry.execute([:gateway, :consumer, :process], %{}, %{event: event_type, type: :bulk})

    Keyword.values(route_keys)
    |> Enum.each(&handle_event(event_type, {&1, data}))
  end

  # p: single channel events
  def process_event(%{"type" => event_type} = data, route_key) do
    :telemetry.execute([:gateway, :consumer, :process], %{}, %{event: event_type, type: :single})
    handle_event(event_type, {route_key, data})
  end

  # Custom logic to pattern handle messages in servers & dm channels
  def handle_event("Message", {_, %{"member" => %{"_id" => %{"server" => server_id}}} = data}) do
    server_fanout(server_id, {:Message, data})
  end

  def handle_event("Message", {_, %{"system" => _system}}=data) do
    handle_channel_event(:Message, data)
  end

  def handle_event("Message", {_, %{"channel" => channel_id} = data}) do
    handle_dm_event(:Message, channel_id, data)
  end

  # Channel scoped events
  def handle_event("MessageUpdate", data), do: handle_channel_event(:MessageUpdate, data)
  def handle_event("MessageAppend", data), do: handle_channel_event(:MessageAppend, data)
  def handle_event("MessageDelete", data), do: handle_channel_event(:MessageDelete, data)
  def handle_event("MessageReact", data), do: handle_channel_event(:MessageReact, data)
  def handle_event("MessageUnreact", data), do: handle_channel_event(:MessageUnreact, data)

  def handle_event("MessageRemoveReaction", data),
    do: handle_channel_event(:MessageRemoveReaction, data)

  def handle_event("BulkMessageDelete", data), do: handle_channel_event(:BulkMessageDelete, data)

  # Match against server channels
  def handle_event("ChannelCreate", {_, %{"server" => _server_id}} = data) do
    handle_server_event(:ChannelCreate, data)
  end

  # NOTE: DM channel creation
  def handle_event("ChannelCreate", data), do: handle_presence_event(:ChannelCreate, data)

  def handle_event("ChannelUpdate", data), do: handle_channel_event(:ChannelUpdate, data)
  def handle_event("ChannelDelete", data), do: handle_channel_event(:ChannelDelete, data)
  def handle_event("ChannelGroupLeave", data), do: handle_channel_event(:ChannelGroupleave, data)
  def handle_event("ChannelGroupJoin", data), do: handle_channel_event(:ChannelGroupJoin, data)

  def handle_event("VoiceChannelJoin", data), do: handle_channel_event(:VoiceChannelJoin, data)
  def handle_event("VoiceChannelLeave", data), do: handle_channel_event(:VoiceChannelLeave, data)
  def handle_event("VoiceChannelMove", data), do: handle_channel_event(:VoiceChannelMove, data)
  def handle_event("UserMoveVoiceChannel", data), do: handle_presence_event(:UserMoveVoiceChannel, data)

  def handle_event("VoiceCallUpdate", {route, %{"channel_id" => channel_id}} = data)
      when route == channel_id do
    handle_channel_event(:VoiceCallUpdate, data)
  end

  def handle_event("VoiceCallUpdate", data), do: handle_presence_event(:VoiceCallUpdate, data)

  def handle_event("WebhookCreate", data), do: handle_channel_event(:WebhookCreate, data)
  def handle_event("WebhookUpdate", data), do: handle_channel_event(:WebhookUpdate, data)
  def handle_event("WebhookDelete", data), do: handle_channel_event(:WebhookDelete, data)

  def handle_event("UserVoiceStateUpdate", data),
    do: handle_channel_event(:UserVoiceStateUpdate, data)

  # Server scoped events
  def handle_event("ServerCreate", {_, %{"id" => id, "server" => %{"owner" => owner_id}}}) do
    # NOTE: Event gives us all the data so we need a different start_link pattern match
    # NOTE: ServerMemberJoin likely starts the server so this is just to link the owner...
    {_, server_pid} = StoatGateway.Server.lookup_or_start(id)

    case StoatGateway.Presence.lookup(owner_id) do
      {:ok, pid} -> send(pid, {:presence_server_create, {id, server_pid}})
      _ -> nil
    end
  end

  def handle_event("ServerUpdate", data), do: handle_server_event(:ServerUpdate, data)
  def handle_event("ServerDelete", data), do: handle_server_event(:ServerDelete, data)
  def handle_event("ServerMemberUpdate", data), do: handle_server_event(:ServerMemberUpdate, data)
  def handle_event("ServerMemberJoin", data), do: handle_server_event(:ServerMemberJoin, data)

  def handle_event("ServerMemberLeave", {_, %{"user" => user_id} = payload} = data) do
    handle_server_event(:ServerMemberLeave, data)
    handle_presence_event(:ServerMemberLeave, {user_id, payload})
  end

  def handle_event("ServerRoleUpdate", data), do: handle_server_event(:ServerRoleUpdate, data)
  def handle_event("ServerRoleDelete", data), do: handle_server_event(:ServerRoleDelete, data)

  def handle_event("ServerRoleRanksUpdate", data),
    do: handle_server_event(:ServerRoleRanksUpdate, data)

  def handle_event("EmojiCreate", data), do: handle_server_event(:EmojiCreate, data)
  def handle_event("EmojiUpdate", data), do: handle_server_event(:EmojiUpdate, data)
  def handle_event("EmojiDelete", data), do: handle_server_event(:EmojiDelete, data)
  # User scoped events
  #   Presence Events

  def handle_event("ChannelAck", data), do: handle_presence_event(:ChannelAck, data)
  def handle_event("UserUpdate", data), do: handle_presence_event(:UserUpdate, data)
  def handle_event("UserSlowmodes", data), do: handle_presence_event(:UserSlowmodes, data)

  def handle_event("UserSettingsUpdate", data),
    do: handle_presence_event(:UserSettingsUpdate, data)

  def handle_event("UserRelationship", data), do: handle_presence_event(:UserRelationship, data)
  def handle_event("UserPlatformWipe", _data), do: nil

  def handle_event(event, data) do
    Logger.info("Unhandled event=#{event} data=#{inspect(data)}")
  end

  def parse_channel_id(%{"channel" => channel_id}), do: channel_id
  def parse_channel_id(%{"channel_id" => channel_id}), do: channel_id
  def parse_channel_id(%{"id" => channel_id}), do: channel_id
  def parse_channel_id(%{"_id" => channel_id}), do: channel_id
  def parse_channel_id(%{id: channel_id}), do: channel_id

  def is_channel_event?(:Message), do: true
  def is_channel_event?(:MessageAppend), do: true
  def is_channel_event?(:MessageUpdate), do: true
  def is_channel_event?(:MessageDelete), do: true
  def is_channel_event?(:MessageReact), do: true
  def is_channel_event?(:MessageUnreact), do: true
  def is_channel_event?(:MessageRemoveReaction), do: true
  def is_channel_event?(:ChannelCreate), do: true
  def is_channel_event?(:ChannelUpdate), do: true
  def is_channel_event?(:ChannelDelete), do: true
  def is_channel_event?(:ChannelStartTyping), do: true
  def is_channel_event?(:ChannelStopTyping), do: true
  def is_channel_event?(:VoiceChannelJoin), do: true
  def is_channel_event?(:VoiceChannelLeave), do: true
  def is_channel_event?(:VoiceChannelMove), do: true
  def is_channel_event?(:VoiceCallUpdate), do: true
  def is_channel_event?(:UserVoiceStateUpdate), do: true
  def is_channel_event?(:WebhookCreate), do: true
  def is_channel_event?(:WebhookUpdate), do: true
  def is_channel_event?(:WebhookDelete), do: true
  def is_channel_event?(_), do: false

  defp handle_channel_event(event, {channel_id, data}) do
    # NOTE: Both ets tables so might slow things down
    # Another RMQ header with destination type?
    case :ets.lookup(:channel_server_refs, channel_id) do
      [{_, server_id}] -> server_fanout(server_id, {event, data})
      _ -> handle_dm_event(event, channel_id, data)
    end
  end

  def handle_dm_event(event, channel_id, data) do
    subscriptions = :pg.get_members(:gdm_channels, channel_id)

    Enum.each(subscriptions, fn pid ->
      send(pid, {:dm_event_dispatch, {event, data}})
    end)
  end

  defp handle_presence_event(event, {user_id, data}) do
    case StoatGateway.Presence.lookup(user_id) do
      {:ok, pid} -> send(pid, {:presence_event_dispatch, {event, data}})
      _ -> nil
    end
  end

  defp handle_server_event(event, {server_id, data}) do
    server_fanout(server_id, {event, data})
  end

  defp server_fanout(server_id, {event, data}) do
    {:ok, server} = StoatGateway.Server.lookup_or_start(server_id)
    StoatGateway.Server.dispatch(server, event, data)
  end
end
