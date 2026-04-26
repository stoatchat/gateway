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
  use GenServer, restart: :transient
  require Logger
  # Arbitrary 30s timeout to resume
  @socket_disconnect_timeout 30_000

  defstruct ready: false,
            user_id: nil,
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

  def start_link(%{socket: socket, data: %{"user_id" => id, "_id" => session} = _data}) do
    GenServer.start_link(
      __MODULE__,
      %__MODULE__{
        user_id: id,
        linked_socket: socket,
        session: session
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
    memberships = Stoat.User.fetch_server_memberships(state.user_id)
    server_ids = Stoat.User.server_ids_from_memberships(memberships)

    server_pids =
      Enum.map(server_ids, fn server_id ->
        {:ok, pid} = StoatGateway.Server.lookup_or_start(server_id)
        GenServer.cast(pid, {:link_session, state.user_id, self()})
        {server_id, pid}
      end)

    servers = Stoat.Server.fetch_many(server_ids)

    channel_ids = servers |> Enum.flat_map(& &1["channels"])
    channels = Mongo.find(:mongo_db, "channels", %{_id: %{"$in": channel_ids}}) |> Enum.to_list()
    channels = Stoat.Permissions.filter_inaccessible_channels(channels, servers, memberships)

    ready_payload = %Stoat.State.Ready{servers: servers, channels: channels, members: memberships}
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
    # TODO: Use state to find this
    server_id = Stoat.Server.fetch_by_channel_id(channel_id)

    case Enum.find(state.linked_servers, fn {id, _} -> id == server_id end) do
      {_, pid} -> GenServer.cast(pid, {:dispatch_begin_typing, channel_id, state.user_id})
    end

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
end
