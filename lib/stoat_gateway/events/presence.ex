defmodule StoatGateway.Presence do
  @moduledoc """
  User unique Genserver which handles Presence updates and DMs/Groups
  Designed to be a "channel" agnostic routing layer for events which do not belong to a Server
  Essentially the level above a Session but called presence as it mostly handles this
  """
  use GenServer, restart: :transient 
  require Logger

  defstruct user_id: nil,
    dm_channels: [],
    relationships: [],
    sessions: [],
    current_presence: nil,
    current_status: %{},
    subscribers: [],
    last_event_id: 0
  
  def start_link(%{user_id: user_id} = state) do
    GenServer.start_link(__MODULE__, state,
      name: {:via, Registry, {Stoat.Presence, user_id}}
    )
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
    ensure_friend_subscriptions(state.relationships, state.user_id)
    {:noreply, state}
  end

  def handle_cast({:session_link_async, session_id, type, pid}, state) do
    Logger.debug("presence: session link id=#{inspect(session_id)} pid=#{inspect(pid)}")
    ref = Process.monitor(pid)

    session = %{
      session_id: session_id,
      pid: pid,
      monitor: ref,
      type: type,
    }

    {:noreply, %{state | sessions: [session | state.sessions]}}
  end

  def handle_cast({:presence_link_friend, user_id, pid}, state) do
    {:noreply, %{state | subscribers: [{user_id, pid} | state.subscribers]}}
  end

  def handle_call({:update_gdm_channels, gdm_channels, session_id}, state) do
  end

  def handle_info({:dm_event_dispatch, payload}, state) do
    Enum.each(state.sessions, &send(&1.pid, {:socket_dispatch, payload}))
    {:noreply, state}
  end

  def handle_info({:presence_user_update, {:UserUpdate, data} = payload}, state) do
    event_id = Map.get(data, "event_id")
    if event_id == state.last_event_id do
      {:noreply, state}
    else
      {:noreply, %{state | last_event_id: event_id}}
    end 
  end

  def handle_info({:presence_user_update, payload}, state) do
    {:noreply, state}
  end

  defp ensure_gdm_subscriptions(channels) do
    subscriptions = Enum.map(channels, fn %{"_id" => channel} ->
      {channel, self()}  
    end)
    true = :ets.insert(:gdm_subscriptions, subscriptions)
  end

  defp ensure_friend_subscriptions(relationships, user_id) do
    Enum.each(relationships, fn %{"_id" => friend_id, "status" => status} -> 
      case status do
        "Friend" ->  
          case StoatGateway.Presence.lookup(friend_id) do
            {:ok, pid} -> GenServer.cast(pid, {:presence_link_friend, user_id, self()})
            _ -> nil
          end
        _ -> nil
      end 
    end)
  end

  def code_change(_old_vsn, state, _extra), do: {:ok, state}
end
