defmodule Stoat.State.Ready do
  @derive Jason.Encoder

  defstruct type: "Ready",
            users: [],
            servers: [],
            channels: [],
            members: [],
            emojis: [],
            voice_states: [],
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
            linked_servers: []

  @type t :: %__MODULE__{
          ready: boolean(),
          user_id: String.t(),
          session: String.t(),
          linked_socket: pid(),
          type: atom(),
          forwarding: boolean(),
          servers: list(),
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
    server_ids = Stoat.User.fetch_server_memberships(state.user_id)

    server_pids =
      Enum.map(server_ids, fn server_id ->
        {:ok, pid} = StoatGateway.Server.lookup_or_start(server_id)
        GenServer.cast(pid, {:link_session, state.user_id, self()})
        {server_id, pid}
      end)

    servers = Stoat.Server.fetch_many(server_ids)

    channel_ids = servers |> Enum.to_list() |> Enum.map(& &1["channels"]) |> List.flatten()
    # TODO: calculate viewable channels with permissions
    channels = Mongo.find(:mongo_db, "channels", %{_id: %{"$in": channel_ids}}) |> Enum.to_list()

    ready_payload = %Stoat.State.Ready{servers: servers, channels: channels}
    send(state.linked_socket, {:ready, ready_payload})
    state = %{state | servers: server_pids}
    {:noreply, %{state | ready: true}}
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
