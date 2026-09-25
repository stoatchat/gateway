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
  alias StoatGateway.Web.ReadyFields
  use GenServer, restart: :transient
  require Logger
  # Arbitrary 20s timeout to resume
  @socket_disconnect_timeout 5_000

  defstruct ready: false,
            ready_fields: %ReadyFields{},
            user_id: nil,
            data: %{},
            session: nil,
            linked_socket: nil,
            linked_presence: nil,
            type: :user,
            forwarding: true,
            servers: [],
            channels: [],
            memberships: [],
            users: [],
            linked_servers: []

  @type t :: %__MODULE__{
          ready: boolean(),
          ready_fields: ReadyFields.t(),
          user_id: String.t(),
          data: map(),
          session: String.t(),
          linked_socket: pid(),
          linked_presence: pid(),
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
        type: type,
        ready_fields: ready_fields
      }) do
    GenServer.start_link(
      __MODULE__,
      %__MODULE__{
        user_id: id,
        linked_socket: socket,
        session: session,
        type: type,
        ready_fields: ready_fields
      }
    )
  end

  def start_link(%{
        socket: socket,
        data: %{
          "_id" => id
        },
        type: type,
        ready_fields: ready_fields
      }) do
    GenServer.start_link(
      __MODULE__,
      %__MODULE__{
        user_id: id,
        linked_socket: socket,
        session: id,
        type: type,
        ready_fields: ready_fields
      }
    )
  end

  @spec lookup(binary(), binary()) :: {:ok, pid()} | {:error, atom()}
  def lookup(id, session_id) do
    case Registry.match(Stoat.Sessions, id, session_id) do
      [{session_pid, nil}] ->
        {:ok, session_pid}

      _ ->
        {:error, :not_found}
    end
  end

  def init(state) do
    :telemetry.execute([:gateway, :session, :init], %{}, %{type: state.type})
    Logger.debug("session: init self: #{inspect(self())} with state: #{inspect(state)}")
    Process.monitor(state.linked_socket)
    Registry.register(Stoat.Sessions, state.user_id, state.session)
    # let the socket know of the PID in case we have error'd and are a new Process
    send(state.linked_socket, {:session_ack, self()})
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

    servers =
      Stoat.Server.fetch_many(server_ids)
      |> Enum.map(fn %{"_id" => id} = server ->
        count = Stoat.Server.fetch_approximate_user_count(id)
        Map.put(server, "approximate_member_count", count)
      end)

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

    user_ids =
      Map.get(state.data, "relations", [])
      |> Map.new(fn %{"_id" => id} = data -> {id, data} end)

    self_status = Map.get(user, "status", %{})

    ready_payload = %Stoat.State.Ready{}

    ready_payload =
      if state.ready_fields.users do
        %{
          ready_payload
          | users: [
              build_ready_user(state.data, "User", true, self_status)
              | build_ready_relations_from_state(user_ids, state)
            ]
        }
      else
        ready_payload
      end

    ready_payload =
      if state.ready_fields.servers do
        %{ready_payload | servers: servers}
      else
        ready_payload
      end

    ready_payload =
      if state.ready_fields.channels do
        %{ready_payload | channels: channels}
      else
        ready_payload
      end

    ready_payload =
      if state.ready_fields.members do
        %{ready_payload | members: memberships}
      else
        ready_payload
      end

    ready_payload =
      if state.ready_fields.emojis do
        %{ready_payload | emojis: Stoat.Server.find_emojis_by_many(server_ids)}
      else
        ready_payload
      end

    ready_payload =
      if !Enum.empty?(state.ready_fields.user_settings) do
        %{
          ready_payload
          | user_settings:
              Stoat.User.fetch_user_settings(state.user_id, state.ready_fields.user_settings)
        }
      else
        ready_payload
      end

    ready_payload =
      if state.ready_fields.channel_unreads do
        %{ready_payload | channel_unreads: Stoat.User.fetch_unreads(state.user_id)}
      else
        ready_payload
      end

    ready_payload =
      if state.ready_fields.policy_changes && state.type == :user do
        last_acknowledge_time = Map.get(state.data, "last_acknowledged_policy_change", 0)

        %{ready_payload | policy_changes: Stoat.User.fetch_policy_changes(last_acknowledge_time)}
      else
        ready_payload
      end

    ready_payload =
      if state.ready_fields.voice_states do
        %{
          ready_payload
          | voice_states: fetch_voice_states(filter_voice_enabled_channels(channels))
        }
      else
        ready_payload
      end

    send(state.linked_socket, {:ready, ready_payload})
    GenServer.cast(self(), {:presence_init_link, filter_dm_channels(channels)})

    {:noreply,
     %{
       state
       | ready: true,
         linked_servers: server_pids,
         servers: servers,
         channels: channels,
         memberships: memberships,
         data: user
     }}
  end

  def handle_cast({:presence_init_link, dm_channels}, %__MODULE__{} = state) do
    Logger.debug(
      "session=#{state.session} init presence link with channels #{inspect(dm_channels)}"
    )

    Redix.command(:redix, ["SADD", "sessions:#{state.user_id}", state.session])

    relationships = Map.get(state.data, "relations", [])
    self_status = Map.get(state.data, "status", %{})

    presence_pid =
      case StoatGateway.Presence.lookup(state.user_id) do
        {:ok, pid} ->
          pid

        _ ->
          {:ok, pid} =
            StoatGateway.Presence.supervised_start(
              state.user_id,
              dm_channels,
              relationships,
              self_status
            )

          pid
      end

    Process.monitor(presence_pid)
    status = Map.get(state.data, "status", %{})
    GenServer.cast(presence_pid, {:session_link_async, state.session, state.type, self(), status})
    {:noreply, %{state | linked_presence: presence_pid}}
  end

  def handle_cast({:event_typing, event, channel_id}, state) do
    # we can then move more state onto the server process and calculate it all there?
    # benefit would be less state everywhere and the server can probably cache all of this
    case Enum.find(state.channels, fn %{"_id" => id} -> id == channel_id end) do
      %{"server" => server_id} ->
        case Enum.find(state.linked_servers, fn {id, _, _} -> id == server_id end) do
          {_, pid, _} -> GenServer.cast(pid, {:dispatch_typing, event, channel_id, state.user_id})
          _ -> nil
        end

      # NOTE: GDM typing
      _ ->
        GenServer.cast(state.linked_presence, {:dispatch_typing, event, channel_id})
    end

    {:noreply, state}
  end

  def handle_info(
        {:socket_dispatch, {:UserSettingsUpdate, body}},
        %{linked_socket: socket} = state
      )
      when is_pid(socket) do
    new_body =
      Map.update!(body, "update", fn update ->
        Map.new(update, fn {k, v} -> {k, Tuple.to_list(v)} end)
      end)

    send(socket, {:event_dispatch_raw, new_body})

    {:noreply, state}
  end

  def handle_info({:socket_dispatch, {_event, body}}, %{linked_socket: socket} = state)
      when is_pid(socket) do
    send(socket, {:event_dispatch_raw, body})
    {:noreply, state}
  end

  # NOTE: SessionResume in future we'll buffer events here
  def handle_info({:socket_dispatch, _}, state), do: {:noreply, state}

  def handle_info({:event_server_create, id, pid}, state) do
    GenServer.cast(
      pid,
      {:session_link_async, state.session, state.type, state.user_id, self(), %{"roles" => []}}
    )

    ref = Process.monitor(pid)

    {:noreply, %{state | linked_servers: [{id, pid, ref} | state.linked_servers]}}
  end

  def handle_info({:presence_roundabout, payload}, %__MODULE__{} = state) do
    send(state.linked_presence, {:presence_update, payload})
    {:noreply, state}
  end

  def handle_info({:DOWN, _ref, :process, pid, _}, %__MODULE__{} = state)
      when pid == state.linked_socket do
    Logger.debug(
      "session: #{inspect(self())} received :DOWN from linked socket- into nonforward mode"
    )

    Process.send_after(self(), :check_socket_timeout, @socket_disconnect_timeout)
    {:noreply, %{state | forwarding: false}}
  end

  def handle_info({:DOWN, _ref, :process, pid, _}, %__MODULE__{} = state)
      when pid == state.linked_presence do
    Process.send_after(self(), :presence_reconnect_attempt, 2_000)
    {:noreply, state}
  end

  def handle_info({:DOWN, _ref, :process, _pid, _}, state), do: {:noreply, state}

  def handle_info(:check_socket_timeout, %__MODULE__{} = state) do
    case Process.alive?(state.linked_socket) do
      true ->
        {:ok, state}

      _ ->
        Logger.debug("session: terminating session #{inspect(self())} due to socket timeout")
        {:stop, :normal, state}
    end
  end

  def handle_info(:presence_reconnect_attempt, %__MODULE__{} = state) do
    case StoatGateway.Presence.lookup(state.user_id) do
      {:ok, pid} ->
        status = Map.get(state.data, "status", %{})

        GenServer.cast(
          pid,
          {:session_link_async, state.session, state.type, self(), status}
        )

        {:noreply, %{state | linked_presence: pid}}

      _ ->
        Process.send_after(self(), :presence_reconnect_attempt, 2_000)
        {:noreply, state}
    end
  end

  @spec build_ready_relations_from_state(map(), __MODULE__.t()) :: list(map())
  defp build_ready_relations_from_state(relations, state) do
    users = Stoat.User.fetch_by_ids(Map.keys(relations))

    Enum.map(relations, fn {id, %{"status" => relation_status}} ->
      user = Map.get(users, id)

      {online, status} = maybe_get_presence(id, relation_status, state)

      build_ready_user(user, relation_status, online, status)
    end)
    |> Enum.reject(&is_nil/1)
  end

  defp build_ready_user(_user, "BlockedOther", _online, _status), do: nil

  defp build_ready_user(user, relationship_status, online, status) do
    id = Map.get(user, "_id")

    %Stoat.PublicUser{
      relationship: relationship_status,
      username: Map.get(user, "username"),
      discriminator: Map.get(user, "discriminator"),
      display_name: Map.get(user, "display_name"),
      avatar: Map.get(user, "avatar", %{}),
      badges: Stoat.User.transform_badges(id, Map.get(user, "badges", 0)),
      pronouns: Map.get(user, "pronouns"),
      online: online,
      _id: id,
      status: status
    }
  end

  # Presence fetch transform based on relation:
  # https://github.com/stoatchat/stoatchat/blob/main/crates/core/database/src/util/bridge/v0.rs#L1084
  defp maybe_get_presence(_user_id, _relation, %__MODULE__{type: :bot} = _state) do
    {false, %{}}
  end

  defp maybe_get_presence(_user_id, "BlockedOther", %__MODULE__{} = _state) do
    {false, %{}}
  end

  defp maybe_get_presence(user_id, _relation, %__MODULE__{} = _state) do
    Logger.debug(
      "session: build_ready_users -> maybe_get_presence: fetching presence for #{user_id}"
    )

    case StoatGateway.Presence.lookup(user_id) do
      {:ok, pid} ->
        GenServer.call(pid, :fetch_presence_status)

      {:error, _} ->
        {false, %{}}
    end
  end

  defp fetch_voice_states(voice_channels) do
    Enum.map(voice_channels, fn %{"_id" => id} ->
      case Redix.command(:redix, ["SMEMBERS", "vc_members:#{id}"]) do
        {:ok, []} ->
          nil

        {:ok, members} ->
          participants = fetch_voice_participants(id, members)

          %{
            id: id,
            participants: participants
          }
      end
    end)
    |> Enum.reject(fn state -> state == nil end)
  end

  def fetch_voice_participants(channel_id, members) do
    Enum.map(members, fn id ->
      participant_key = "#{id}:#{channel_id}"

      case Redix.command(:redix, [
             "MGET",
             "joined_at:#{participant_key}",
             "is_publishing:#{participant_key}",
             "is_receiving:#{participant_key}",
             "screensharing:#{participant_key}",
             "camera:#{participant_key}"
           ]) do
        {:ok, [joined_at, publishing, receiving, screenshare, camera]} ->
          %{
            id: id,
            joined_at: joined_at,
            is_publishing: intstring_to_bool!(publishing),
            is_receiving: intstring_to_bool!(receiving),
            screensharing: intstring_to_bool!(screenshare),
            camera: intstring_to_bool!(camera)
          }

        _ ->
          nil
      end
    end)
  end

  def intstring_to_bool!("0"), do: false
  def intstring_to_bool!("1"), do: true
  def intstring_to_bool!(_), do: false

  defp filter_dm_channels(channels) do
    Enum.filter(channels, fn %{"channel_type" => type} ->
      type in ["DirectMessage", "Group", "SavedMessages"]
    end)
  end

  defp filter_voice_enabled_channels(channels) do
    Enum.filter(channels, fn %{"channel_type" => type} = channel ->
      case type do
        "DirectMessage" ->
          true

        "Group" ->
          true

        "TextChannel" ->
          case channel do
            %{"voice" => _voice} -> true
            _ -> false
          end

        _ ->
          false
      end
    end)
  end

  # Clean-up important state
  def terminate(_reason, state) do
    Registry.unregister_match(Stoat.Sessions, state.user_id, state.session)
    Redix.command(:redix, ["SREM", "sessions:#{state.user_id}", state.session])
  end

  def code_change(_old_vsn, state, _extra), do: {:ok, state}
end
