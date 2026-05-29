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
      :pg.leave(:presence, user_id, self())
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
          "data" => %{"roles" => updated_roles}
        },
        state
      ) do
    affected_sessions = filter_sessions_by_id(state.linked_sessions, member_id)

    updated_sessions =
      Enum.map(state.linked_sessions, fn session ->
        if Enum.member?(affected_sessions, session) do
          %{session | roles: updated_roles}
        else
          session
        end
      end)

    updated_state = %{state | linked_sessions: updated_sessions}
    update_visibility_for_sessions(affected_sessions, state, updated_state)
    updated_state
  end

  def push_state_changes(
        :ServerRoleUpdate,
        %{"role_id" => role_id, "data" => %{"permissions" => permissions}},
        state
      ) do
    sessions = filter_sessions_by_role(state.linked_sessions, role_id)
    IO.inspect(sessions)
    state
  end

  def push_state_changes(:ServerRoleDelete, _, state) do
    # TODO: state to change 
    # - remove role from all members/sessions
    # - remove role from server (to void default_permissions)
    state
  end

  def push_state_changes(
        :ChannelUpdate,
        %{"id" => channel_id, "data" => %{"role_permissions" => role_permissions}},
        state
      ) do
    # TODO: state to change
    # - Update role permissions for each role in the map
    # - Recalclulate for each affected roles users (dedupe)
    state
  end

  def push_state_changes(_, _, state), do: state

  def update_visibility_for_sessions(sessions, old_state, new_state) do
    Enum.each(sessions, fn session ->
      update_visibility_for_session(session, old_state, new_state)
    end)
  end

  def update_visibility_for_session(old_session, new_session, old_state, new_state) do
    partial_member = %{"_id" => %{"user" => old_session.user_id}, "roles" => old_session.roles}

    previous_viewable_channels = Enum.filter(old_state.channels, fn -> 
      Stoat.Permissions.permissions_for_channel(channel, partial_member, old_state.data)
      |> Stoat.permissions.has_permission?(Stoat.Permissions.bits.view_channel())
    end)

    new_partial = Map.merge(partial_member, %{"roles" => new_session.roles})
    # Recalculated with updated-state e.g., new/deleted channels/roles/permissions for either
    updated_viewable_channels = Enum.filter(new_state.channels, fn -> 
      Stoat.Permissions.permissions_for_channel(channel, partial_member, new_state.data)
      |> Stoat.permissions.has_permission?(Stoat.Permissions.bits.view_channel())

    end)

    removed_channels = previous_viewable_channels -- updated_viewable_channels
    added_channels = updated_viewable_channels -- previous_viewable_channels

    build_channel_deletes(removed_channels) |> dispatch_maybe_bulk(new_session)
    build_channel_creates(added_channels) |> dispatch_maybe_bulk(new_session)
  end
  
  def build_channel_creates(channels) do
    Enum.map(channels, fn channel ->
      Map.put(channel, :type, :ChannelCreate)
    end)
  end

  def build_channel_deletes(channels) do
    Enum.map(channels, fn %{"_id" => id} ->
      %{type: :ChannelDelete, id: id}
    end)
  end

  def dispatch_maybe_bulk([payload] = _events, session) do
    send(session.pid, {:socket_dispatch, payload})
  end

  def dispatch_maybe_bulk([_, _] = events, session) do
    send(session.pid, {:socket_dispatch, {:Bulk, %{type: :Bulk, v: events}}})
  end

  def dispatch_maybe_bulk([], _), do: nil

  def filter_sessions_by_role(sessions, role_id) do
    Enum.filter(sessions, fn session ->
      roles = Map.get(session, :roles, [])
      Enum.member?(roles, role_id)
    end)
  end

  def fanout(event, sessions) do
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
