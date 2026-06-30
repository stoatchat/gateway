defmodule StoatGateway.Presence do
  @moduledoc """
  User unique Genserver which handles Presence updates and DMs/Groups
  Designed to be a "channel" agnostic routing layer for events which do not belong to a Server
  Essentially the level above a Session but called presence as it mostly handles this
  """
  use GenServer, restart: :transient
  require Logger
  @maximum_previous_presences 25

  defstruct user_id: nil,
            dm_channels: [],
            relationships: [],
            sessions: [],
            current_presence: nil,
            current_status: %{},
            last_event_id: 0,
            previous_presences: []

  def start_link(%{user_id: user_id} = state) do
    GenServer.start_link(__MODULE__, state, name: {:via, Registry, {Stoat.Presence, user_id}})
  end

  def supervised_start(id, dm_channels, relationships) do
    state = %__MODULE__{
      user_id: id,
      dm_channels: dm_channels,
      relationships: relationships
    }

    DynamicSupervisor.start_child(Stoat.Presence.Supervisor, {StoatGateway.Presence, state})
  end

  @spec lookup(binary()) :: {:ok, pid()} | {:error, atom()}
  def lookup(id) do
    case Registry.lookup(Stoat.Presence, id) do
      [{presence_pid, nil}] ->
        {:ok, presence_pid}

      _ ->
        {:error, :not_found}
    end
  end

  def init(state) do
    {:ok, state, {:continue, :ensure_init}}
  end

  def handle_continue(:ensure_init, state) do
    ensure_gdm_subscriptions(state.dm_channels)
    ensure_friend_subscriptions(state.relationships)
    ensure_online_set_subscription(state)
    {:noreply, state}
  end

  def handle_cast({:session_link_async, session_id, type, pid, status}, state) do
    Logger.debug("presence: session link id=#{inspect(session_id)} pid=#{inspect(pid)}")
    ref = Process.monitor(pid)

    session = %{
      session_id: session_id,
      pid: pid,
      monitor: ref,
      type: type
    }

    {:noreply, %{state | sessions: [session | state.sessions], current_status: status}}
  end

  def handle_call(:fetch_presence_status, _from, state) do
    # NOTE: potential hot-path, calls could fail in a thundering herde scenario but unlikely
    {:reply, {true, state.current_status}, state}
  end

  def handle_call({:update_gdm_channels, _gdm_channels, _session_id}, state) do
    {:ok, state}
  end

  def handle_info({:dm_event_dispatch, {event, data} = payload}, state) do
    session_dispatch(payload, state)
    new_state = maybe_update_state(event, data, state)
    {:noreply, new_state}
  end

  def handle_info(
        {:presence_event_dispatch, {:UserUpdate, %{"id" => user_id} = data} = payload},
        state
      )
      when user_id == state.user_id do
    event_id = Map.get(data, "event_id")

    if event_id == state.last_event_id do
      {:noreply, state}
    else
      subscribers = :pg.get_members(:presence, state.user_id)
      Enum.each(subscribers, &send(&1, {:presence_update, payload}))
      {:noreply, %{state | last_event_id: event_id}}
    end
  end

  def handle_info({:presence_event_dispatch, {event, data} = payload}, state) do
    session_dispatch(payload, state)
    new_state = maybe_update_state(event, data, state)
    {:noreply, state}
  end

  def handle_info(
        {:presence_update, {_, %{"event_id" => event_id}} = payload},
        %{previous_presences: event_ids} = state
      ) do
    case Enum.member?(event_ids, event_id) do
      true ->
        {:noreply, state}

      _ ->
        session_dispatch(payload, state)
        {:noreply, %{state | previous_presences: ensure_presences_size([event_id | event_ids])}}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, _}, state) do
    new_sessions = Enum.reject(state.sessions, fn %{monitor: mref} -> mref == ref end)

    case new_sessions do
      [] -> {:stop, :normal, state}
      _ -> {:noreply, %{state | sessions: new_sessions}}
    end
  end

  defp maybe_update_state(:UserRelationship, _, state) do
    state
  end

  defp maybe_update_state(_, _, state), do: state

  defp ensure_gdm_subscriptions(channels) do
    Enum.each(channels, fn %{"_id" => channel} ->
      :pg.join(:gdm_channels, channel, self())
    end)
  end

  defp ensure_friend_subscriptions(relationships) do
    Enum.each(relationships, fn %{"_id" => friend_id, "status" => status} ->
      case status do
        "Friend" -> :pg.join(:presence, friend_id, self())
        _ -> nil
      end
    end)
  end

  defp ensure_online_set_subscription(state) do
    Redix.command(:redix, ["SADD", "online", state.user_id])
  end

  defp session_dispatch(payload, state) do
    Enum.each(state.sessions, &send(&1.pid, {:socket_dispatch, payload}))
  end

  def terminate(_, state) do
    Redix.command(:redix, ["SREM", "online", state.user_id])
  end

  defp ensure_presences_size(presences) when length(presences) > @maximum_previous_presences do
    Enum.slice(presences, 0, @maximum_previous_presences)
  end

  defp ensure_presences_size(presences), do: presences

  def code_change(_old_vsn, state, _extra), do: {:ok, state}
end
