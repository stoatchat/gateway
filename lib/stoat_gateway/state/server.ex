defmodule StoatGateway.Server do
  use GenServer
  require Logger

  defstruct id: nil,
            data: %{},
            channels: [],
            linked_sessions: []

  def start_link(%{id: id}) do
    GenServer.start_link(__MODULE__, %__MODULE__{id: id},
      name: {:via, Registry, {Stoat.Servers, id}}
    )
  end

  @spec lookup_or_start(binary()) :: {:ok, pid()} | {:error, atom()}
  def lookup_or_start(id) do
    case Registry.lookup(Stoat.Servers, id) do
      [{server_pid, nil}] ->
        {:ok, server_pid}

      _ ->
        DynamicSupervisor.start_child(
          Stoat.Servers.Supervisor,
          {StoatGateway.Server, %{id: id}}
        )
    end
  end

  def init(state) do
    channels = Stoat.Server.fetch_channels(state.id)
    server = Stoat.Server.fetch_by_id(state.id)
    {:ok, %{state | data: server, channels: channels}, {:continue, :ensure_init_state}}
  end

  def dispatch(pid, event, data) do
    GenServer.cast(pid, {:dispatch, event, data})
  end

  def handle_continue(:ensure_init_state, state) do
    # Hack until we can get server id in each server channel related event
    channel_refs = build_channel_tuples(state)
    :ets.insert(:channel_server_refs, channel_refs)
    {:noreply, state}
  end

  def handle_cast(
        {:session_link_async, session_id, session_type, user_id, session_pid, member_data},
        state
      ) do
    Logger.debug("server:#{inspect(self())} add session:#{user_id}:#{inspect(session_pid)}")
    ref = Process.monitor(session_pid)

    session = %{
      session_id: session_id,
      user_id: user_id,
      pid: session_pid,
      monitor: ref,
      type: session_type,
      roles: Map.get(member_data, "roles", [])
    }

    if not user_session_exists?(session, state.linked_sessions) do
      :pg.join(:presence, user_id, self())
    end

    {:noreply, %{state | linked_sessions: [session | state.linked_sessions]}}
  end

  def handle_cast({:dispatch, event, payload}, state) do
    fanout({event, payload}, state.linked_sessions)
    new_state = push_state_changes(event, payload, state)
    {:noreply, new_state}
  end

  # TODO: probably want a general dispatch catch and then another func for specific topics
  # say channel, overall
  def handle_cast({:dispatch_begin_typing, channel_id, user_id}, state) do
    fanout(
      {:ChannelStartTyping, %{type: "ChannelStartTyping", id: channel_id, user: user_id}},
      state.linked_sessions
    )

    Logger.debug("server:#{inspect(self())} dispatching typing by #{user_id} to #{channel_id}")
    {:noreply, state}
  end

  def handle_cast({:dispatch_stop_typing, channel_id, user_id}, state) do
    fanout(
      {:ChannelStopTyping, %{type: "ChannelStopTyping", id: channel_id, user: user_id}},
      state.linked_sessions
    )

    Logger.debug("server:#{inspect(self())} dispatching typing by #{user_id} to #{channel_id}")
    {:noreply, state}
  end

  def handle_info({:presence_update, payload}, state) do
    fanout(payload, state.linked_sessions)
    {:noreply, state}
  end

  def handle_info({:DOWN, _ref, :process, pid, _}, state) do
    session =
      Enum.find(state.linked_sessions, %{}, fn session ->
        session.pid == pid
      end)

    new_sessions =
      Enum.reject(state.linked_sessions, fn session ->
        session.pid == pid
      end)

    user_id = Map.get(session, "user_id")

    if not user_session_exists?(user_id, new_sessions) do
      :pg.leave(:presence, session.user_id, self())
    end

    {:noreply,
     %{
       state
       | linked_sessions: new_sessions
     }}
  end

  def push_state_changes(
        :ServerMemberUpdate,
        %{
          "id" => %{"user" => member_id},
          "data" => member
        },
        state
      ) do
    affected_sessions = filter_sessions_by_id(state.linked_sessions, member_id)

    updated_sessions =
      Enum.map(affected_sessions, fn old_session ->
        updated_roles = Map.get(member, "roles")

        update_visibility_for_session(
          %{old_session | roles: updated_roles},
          old_session,
          state.channels,
          state.data
        )
      end)

    %{state | linked_sessions: updated_sessions}
  end

  def push_state_changes(:ServerRoleUpdate, _, state) do
    state
  end

  def push_state_changes(:ServerRoleDelete, _, state) do
    state
  end

  def push_state_changes(:ChannelUpdate, _, state) do
    state
  end

  def push_state_changes(_, _, state), do: state

  def update_visibility_for_session(new_session, old_session, channels, server) do
    # Check when not 1am!
    # TODO: Adjust permissions API to accept inconsistent keys like we have here...
    stripped_member = %{"_id" => %{"user" => old_session.user_id}, "roles" => old_session.roles}

    previous_viewable_channels =
      Enum.filter(channels, fn channel ->
        Stoat.Permissions.permissions_for_channel(channel, stripped_member, server)
        |> Stoat.Permissions.has_permission?(Stoat.Permissions.Bits.view_channel())
      end)

    stripped_member = Map.merge(stripped_member, %{"roles" => new_session.roles})

    updated_viewable_channels =
      Enum.filter(channels, fn channel ->
        Stoat.Permissions.permissions_for_channel(channel, stripped_member, server)
        |> Stoat.Permissions.has_permission?(Stoat.Permissions.Bits.view_channel())
      end)

    removed_channels = previous_viewable_channels -- updated_viewable_channels
    added_channels = updated_viewable_channels -- previous_viewable_channels

    dispatch_channel_deletes(new_session, removed_channels)
    dispatch_channel_creates(new_session, added_channels)

    new_session
  end

  # TODO: Dupe logic, simplify !
  def dispatch_channel_creates(session, [channel]) do
    send(
      session.pid,
      {:socket_dispatch, {:ChannelCreate, Map.put(channel, :type, :ChannelCreate)}}
    )
  end

  def dispatch_channel_creates(session, [_, _] = channels) do
    bulk_deletes =
      Enum.map(channels, fn channel ->
        Map.put(channel, :type, :ChannelCreate)
      end)

    send(session.pid, {:socket_dispatch, {:Bulk, %{type: :Bulk, v: bulk_deletes}}})
  end

  def dispatch_channel_creates(_, []), do: nil

  def dispatch_channel_deletes(session, [%{"_id" => id}]) do
    send(session.pid, {:socket_dispatch, {:ChannelDelete, %{type: :ChannelDelete, id: id}}})
  end

  def dispatch_channel_deletes(session, [_, _] = channels) do
    bulk_deletes =
      Enum.map(channels, fn %{"_id" => id} ->
        %{type: :ChannelDelete, id: id}
      end)

    send(session.pid, {:socket_dispatch, {:Bulk, %{type: :Bulk, v: bulk_deletes}}})
  end

  def dispatch_channel_deletes(_, []), do: nil

  def fanout(event, sessions) do
    # TODO: just take an enum of sessions and higher level functions can filter as needed
    Enum.each(sessions, &send(&1.pid, {:socket_dispatch, event}))
  end

  # TODO: Perhaps presence logic can change to avoid this tomfoolery
  defp user_session_exists?(user, sessions) do
    Enum.any?(sessions, fn session ->
      user == session.user_id
    end)
  end

  def filter_sessions_by_id(sessions, user_id) do
    Enum.filter(sessions, fn session -> session.user_id == user_id end)
  end

  defp build_channel_tuples(state) do
    channels = Map.get(state.data, "channels")

    Enum.map(channels, fn id ->
      {id, state.id}
    end)
  end
end
