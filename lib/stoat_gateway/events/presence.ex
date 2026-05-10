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
    {:ok, state}
  end

  def handle_cast({:session_link_async, session_id, type, user_id, pid}, state) do
    ref = Process.monitor(pid)

    session = %{
      session_id: session_id,
      user_id: user_id,
      pid: pid,
      monitor: ref,
      type: type,
    }

    {:noreply, %{state | sessions: [session | state.sessions]}}
  end

  def ensure_gdm_subscriptions(state) do
  end
end
