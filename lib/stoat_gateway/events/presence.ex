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
    subscriptions: []
  
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
    DynamicSupervisor.start_child(Stoat.Servers.Supervisor, {StoatGateway.Presence, state})
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
    {:noreply, state}
  end

  def handle_cast({:session_link_async, session_id, type, pid}, state) do
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
    {:noreply, state}
  end

  def handle_call({:update_gdm_channels, gdm_channels, session_id}, state) do
  end

  def handle_info({:dm_event_dispatch, payload}, state) do
    Enum.each(state.sessions, &send(&1.pid, {:socket_dispatch, payload}))
    {:noreply, state}
  end

  defp ensure_gdm_subscriptions(channels) do
    subscriptions = Enum.map(channels, fn %{"_id" => channel} ->
      {channel, self()}  
    end)
    true = :ets.insert(:gdm_subscriptions, subscriptions)
  end

  defp ensure_friend_subscriptions(relationships) do
  end

  def code_change(_old_vsn, state, _extra), do: {:ok, state}
end
