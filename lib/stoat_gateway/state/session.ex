defmodule Stoat.State.Ready do
  @derive Jason.Encoder

  defstruct type: "Ready",
            users: [],
            servers: [],
            channels: [],
            members: [],
            emojis: [],
            voice_states: [],
            user_settings: %{},
            channel_unreads: [],
            policy_changes: []
end

defmodule StoatGateway.Session do
  use GenServer, restart: :temporary
  require Logger
  # Arbitrary 30s timeout to resume
  @socket_disconnect_timeout 30_000

  defstruct ready: false,
            user_id: nil,
            data: %{},
            session: nil,
            linked_socket: nil,
            type: :user,
            forwarding: true,
            servers: [],
            channels: [],
            memberships: [],
            users: [],
            linked_servers: []

  @type t :: %__MODULE__{
          ready: boolean(),
          user_id: String.t(),
          data: map(),
          session: String.t(),
          linked_socket: pid(),
          type: atom(),
          forwarding: boolean(),
          servers: list(),
          channels: list(),
          memberships: list(),
          users: list(),
          linked_servers: list()
        }

  def start_link(%{
        socket: socket,
        data: %{"user_id" => id, "_id" => session} = _data,
        type: type
      }) do
    GenServer.start_link(
      __MODULE__,
      %__MODULE__{
        user_id: id,
        linked_socket: socket,
        session: session,
        type: type
      }
    )
  end

  def init(state) do
    # TODO: Add metrics here for connected session 
    Logger.debug("session: init self: #{inspect(self())} with state: #{inspect(state)}")
    Process.monitor(state.linked_socket)
    Registry.register(Stoat.Sessions, state.user_id, state.session)
    {:ok, state, {:continue, :ready}}
  end

  def handle_continue(:ready, state) do
    user = Stoat.User.fetch_by_id(state.user_id)
    state = %{state | data: user}
    memberships = Stoat.User.fetch_server_memberships(state.user_id)
    server_ids = Stoat.User.server_ids_from_memberships(memberships)

    server_pids =
      Enum.map(server_ids, fn server_id ->
        {:ok, pid} = StoatGateway.Server.lookup_or_start(server_id)
        # v2: we'll reduce DB calls and initially send server_ids as unavailable
        # as servers are started the membership process will cause ServerAvailable events to get fired 
        member =
          Enum.find(memberships, fn %{"_id" => %{"server" => server_id}} ->
            server_id == server_id
          end)

        GenServer.cast(
          pid,
          {:session_link_async, state.session, state.type, state.user_id, self(), member}
        )

        ref = Process.monitor(pid)
        {server_id, pid, ref}
      end)

    servers = Stoat.Server.fetch_many(server_ids)

    channel_ids = servers |> Enum.flat_map(& &1["channels"])
    user_channels = Stoat.User.fetch_user_channels(state.user_id)
    channels = Mongo.find(:mongo_db, "channels", %{_id: %{"$in": channel_ids}}) |> Enum.to_list()

    channels =
      Stoat.Permissions.filter_inaccessible_channels(
        channels ++ user_channels,
        servers,
        memberships,
        state.user_id
      )

    emojis = Stoat.Server.find_emojis_by_many(server_ids)

    user_settings = Stoat.User.fetch_user_settings(state.user_id)
    channel_unreads = Stoat.User.fetch_unreads(state.user_id)

    policy_changes =
      case state.type do
        :bot ->
          []

        :user ->
          last_acknowledge_time = Map.get(state.data, "last_acknowledged_policy_change", 0)
          Stoat.User.fetch_policy_changes(last_acknowledge_time)
      end

    user_ids = Map.get(state.data, "relations", [])
    users = [build_ready_user(state.data, "User") | build_ready_relations_from_state(user_ids)]

    ready_payload = %Stoat.State.Ready{
      servers: servers,
      channels: channels,
      members: memberships,
      emojis: emojis,
      user_settings: user_settings,
      channel_unreads: channel_unreads,
      policy_changes: policy_changes,
      users: users
    }

    send(state.linked_socket, {:ready, ready_payload})

    {:noreply,
     %{
       state
       | ready: true,
         linked_servers: server_pids,
         servers: servers,
         channels: channels,
         memberships: memberships
     }}
  end

  def handle_cast({:event_begin_typing, channel_id}, state) do
    # Limit to channels we can view -> server will calculate final perms
    # we can then move more state onto the server process and calculate it all there?
    # benefit would be less state everywhere and the server can probably cache all of this
    case Enum.find(state.channels, fn %{"_id" => id} -> id == channel_id end) do
      %{"server" => server_id} ->
        case Enum.find(state.linked_servers, fn {id, _, _} -> id == server_id end) do
          {_, pid, _} -> GenServer.cast(pid, {:dispatch_begin_typing, channel_id, state.user_id})
          _ -> nil
        end

      # TODO: Process DMs
      _ ->
        nil
    end

    {:noreply, state}
  end

  def handle_info({:socket_dispatch, {event, body}}, state) do
    # TODO: Handle no socket here and holdon to events, upon max close session
    send(state.linked_socket, {:event_dispatch, event, body})
    {:noreply, state}
  end

  # Dead WS handling
  def handle_info(
        {:DOWN, _ref, :process, pid, _},
        %__MODULE__{:linked_socket => socket_pid} = state
      ) do
    # TODO(twitch): Check for ref to a linked server
    if pid == socket_pid do
      # Websocket has disconnected- go into a no-forwarding mode until we timeout or have a new session
      Logger.debug(
        "session: #{inspect(self())} received :DOWN from linked socket- into nonforward mode"
      )

      Process.send_after(self(), :check_socket_timeout, @socket_disconnect_timeout)
      {:noreply, %{state | forwarding: false}}
    else
      {:noreply, state}
    end
  end

  def handle_info(:check_socket_timeout, state) do
    case Process.alive?(state.linked_socket) do
      true ->
        {:ok, state}

      _ ->
        Logger.debug("session: terminating session #{inspect(self())} due to socket timeout")
        {:stop, :normal, state}
    end
  end

  @spec build_ready_relations_from_state(map()) :: list(map())
  defp build_ready_relations_from_state(relations) do
    Enum.map(relations, fn %{"_id" => id, "status" => status} ->
      user = Stoat.User.fetch_by_id(id)
      # TODO: Fetch presence from ETS before v2?
      # Rearrange this flow in v2 to lazyload like server_session_link
      # Until then: fire presence update on connect?
      build_ready_user(user, status)
    end)
  end

  defp build_ready_user(user, relationship_status) do
    %Stoat.PublicUser{
      relationship: relationship_status,
      username: Map.get(user, "username"),
      discriminator: Map.get(user, "discriminator"),
      display_name: Map.get(user, "display_name"),
      avatar: Map.get(user, "avatar", %{}),
      badges: Map.get(user, "badges"),
      online: false
    }
  end
end
