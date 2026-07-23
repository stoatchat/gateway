defmodule StoatGateway.Server do
  use GenServer
  require Logger

  defstruct id: nil,
            data: %{},
            channels: %{},
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
        case DynamicSupervisor.start_child(
               Stoat.Servers.Supervisor,
               {StoatGateway.Server, %{id: id}}
             ) do
          {:ok, pid} -> {:ok, pid}
          {:error, {:already_started, pid}} -> {:ok, pid}
        end
    end
  end

  def init(state) do
    channels =
      Stoat.Server.fetch_channels(state.id)
      |> Map.new(fn %{"_id" => id} = channel -> {id, channel} end)

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
    new_state = push_state_changes(event, payload, state)
    sessions = filtered_sessions_for_event(event, payload, new_state)
    fanout({event, payload}, sessions)
    {:noreply, new_state}
  end

  def handle_cast({:dispatch_typing, event, channel_id, user_id}, state) do
    GenServer.cast(
      self(),
      {:dispatch, event, %{type: Atom.to_string(event), id: channel_id, user: user_id}}
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
        :ServerUpdate,
        %{"data" => new_data, "clear" => clear},
        %__MODULE__{} = state
      ) do
    data = Map.merge(state.data, new_data)

    updated =
      Enum.reduce(clear, data, fn key, server ->
        Map.put(server, String.downcase(key), "")
      end)

    updated_state = %{state | data: updated}

    if Map.has_key?(new_data, "default_permissions") do
      update_visibility_for_sessions(state.linked_sessions, state, updated_state)
    end

    updated_state
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
    affected_sessions = filter_sessions_by_role(state.linked_sessions, role_id)

    new_data =
      Map.update(state.data, "roles", %{role_id => %{"permissions" => permissions}}, fn roles ->
        Map.update(roles, role_id, %{"permissions" => permissions}, fn old ->
          %{old | "permissions" => permissions}
        end)
      end)

    updated_state = %{state | data: new_data}
    update_visibility_for_sessions(affected_sessions, state, updated_state)
    updated_state
  end

  def push_state_changes(:ServerRoleDelete, %{"role_id" => role_id}, state) do
    affected_sessions = filter_sessions_by_role(state.linked_sessions, role_id)

    data =
      Map.update(state.data, "roles", %{}, fn roles ->
        Enum.filter(roles, fn {id, _} -> id != role_id end)
      end)

    new_sessions =
      Enum.map(state.linked_sessions, fn session ->
        Map.update!(session, :roles, fn roles ->
          Enum.filter(roles, fn role -> role != role_id end)
        end)
      end)

    updated_state = %{state | data: data, linked_sessions: new_sessions}
    update_visibility_for_sessions(affected_sessions, state, updated_state)
    updated_state
  end

  def push_state_changes(:ChannelCreate, %{"_id" => channel_id} = data, %__MODULE__{} = state) do
    :ets.insert(:channel_server_refs, {channel_id, state.id})
    final_channel = Map.take(data, ["_id", "channel_type", "name"])
    %{state | channels: Map.put(state.channels, channel_id, final_channel)}
  end

  def push_state_changes(
        :ChannelUpdate,
        %{"id" => channel_id, "clear" => clear, "data" => data},
        state
      ) do
    Logger.debug("server: push_state_changes: :ChannelUpdate")

    new_channels =
      Map.update!(state.channels, channel_id, fn channel ->
        Enum.reduce(clear, channel, fn key, channel ->
          Map.delete(
            channel,
            case key do
              "Description" -> "description"
              "Icon" -> "icon"
              "DefaultPermissions" -> "default_permissions"
              "Voice" -> "voice"
            end
          )
        end)
        |> Map.merge(data)
      end)

    default_permissions =
      Map.has_key?(data, "default_permissions") || Enum.member?(clear, "DefaultPermissions")

    role_permissions = Map.get(data, "role_permissions")

    affected_sessions =
      if default_permissions do
        state.linked_sessions
      else
        if role_permissions != nil do
          Map.keys(role_permissions)
          |> Enum.flat_map(fn role -> filter_sessions_by_role(state.linked_sessions, role) end)
          |> Enum.dedup_by(fn session -> session.session_id end)
        else
          []
        end
      end

    updated_state = %{state | channels: new_channels}
    update_visibility_for_sessions(affected_sessions, state, updated_state)
    updated_state
  end

  def push_state_changes(:ChannelDelete, %{"id" => channel_id}, state) do
    :ets.delete(:channel_server_refs, channel_id)

    %{state | channels: Map.delete(state.channels, channel_id)}
  end

  def push_state_changes(_, _, state), do: state

  def update_visibility_for_sessions(sessions, old_state, new_state) do
    Enum.each(sessions, fn session ->
      new_session = Enum.find(new_state.linked_sessions, fn s -> session.user_id == s.user_id end)
      update_visibility_for_session(session, new_session, old_state, new_state)
    end)
  end

  def update_visibility_for_session(old_session, new_session, old_state, new_state) do
    partial_member = partial_from_session(old_session)

    previous_viewable_channels =
      Map.values(old_state.channels)
      |> Enum.filter(fn channel ->
        Stoat.Permissions.permissions_for_channel(channel, partial_member, old_state.data)
        |> Stoat.Permissions.has_permission?(Stoat.Permissions.Bits.view_channel())
      end)
      |> Enum.map(fn %{"_id" => id} -> id end)

    new_partial = Map.merge(partial_member, %{"roles" => new_session.roles})
    # Recalculated with updated-state e.g., new/deleted channels/roles/permissions for either
    updated_viewable_channels =
      Map.values(new_state.channels)
      |> Enum.filter(fn channel ->
        Stoat.Permissions.permissions_for_channel(channel, new_partial, new_state.data)
        |> Stoat.Permissions.has_permission?(Stoat.Permissions.Bits.view_channel())
      end)
      |> Enum.map(fn %{"_id" => id} -> id end)

    removed_channel_ids = previous_viewable_channels -- updated_viewable_channels
    added_channel_ids = updated_viewable_channels -- previous_viewable_channels

    # Slightly backwards but we'll calc visibility into ids and then filter by ids...
    added_channels =
      Map.filter(new_state.channels, fn {id, _} ->
        Enum.member?(added_channel_ids, id)
      end)
      |> Map.values()

    build_channel_deletes(removed_channel_ids) |> dispatch_maybe_bulk(new_session)
    build_channel_creates(added_channels) |> dispatch_maybe_bulk(new_session)

    build_server_channel_updates(updated_viewable_channels, new_state)
    |> dispach_session(new_session)
  end

  def build_channel_creates(channels) do
    Enum.map(channels, fn channel ->
      Map.put(channel, :type, :ChannelCreate)
    end)
  end

  def build_channel_deletes(channels) do
    Enum.map(channels, fn id ->
      %{type: :ChannelDelete, id: id}
    end)
  end

  def build_server_channel_updates(channels, state) do
    %{type: :ServerUpdate, id: state.id, data: %{channels: channels}}
  end

  def dispatch_maybe_bulk([], _), do: nil

  def dispatch_maybe_bulk([payload] = _events, session) do
    Logger.debug("server: dispatch_maybe_bulk single event #{session.user_id}")
    send(session.pid, {:socket_dispatch, {payload.type, payload}})
  end

  def dispatch_maybe_bulk(events, session) do
    send(session.pid, {:socket_dispatch, {:Bulk, %{type: :Bulk, v: events}}})
  end

  def dispach_session(payload, session) do
    send(session.pid, {:socket_dispatch, {payload.type, payload}})
  end

  def filter_sessions_by_role(sessions, role_id) do
    Enum.filter(sessions, fn session ->
      roles = Map.get(session, :roles, [])
      Enum.member?(roles, role_id)
    end)
  end

  def fanout(event, sessions) do
    Enum.each(sessions, &send(&1.pid, {:socket_dispatch, event}))
  end

  defp user_session_exists?(user, sessions) do
    Enum.any?(sessions, fn session ->
      user == session.user_id
    end)
  end

  def filter_sessions_by_id(sessions, user_id) do
    Enum.filter(sessions, fn session -> session.user_id == user_id end)
  end

  def filtered_sessions_for_event(event, data, %__MODULE__{} = state) do
    case StoatGateway.Events.Consumer.is_channel_event?(event) do
      true ->
        channel_id = StoatGateway.Events.Consumer.parse_channel_id(data)
        channel = Map.get(state.channels, channel_id)

        Enum.filter(state.linked_sessions, fn session ->
          partial_user = partial_from_session(session)

          Stoat.Permissions.permissions_for_channel(channel, partial_user, state.data)
          |> Stoat.Permissions.has_permission?(Stoat.Permissions.Bits.view_channel())
        end)

      false ->
        state.linked_sessions
    end
  end

  defp partial_from_session(session) do
    %{"_id" => %{"user" => session.user_id}, "roles" => session.roles}
  end

  defp build_channel_tuples(state) do
    channels = Map.get(state.data, "channels")

    Enum.map(channels, fn id ->
      {id, state.id}
    end)
  end
end
