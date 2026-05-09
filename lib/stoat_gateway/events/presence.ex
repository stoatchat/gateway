defmodule StoatGateway.Presence do
  @moduledoc """
  User unique Genserver which handles Presence updates and DMs/Groups
  Designed to be a "channel" agnostic routing layer for events which do not belong to a Server
  Essentially the level above a Session but called presence as it mostly handles this
  """
  alias ElixirLS.LanguageServer.Providers.Implementation
  use GenServer, restart: :temporary
  require Logger

  defstruct user_id: nil,
    dm_channels: [],
    sessions: [],
    current_presence: nil,
    current_status: %{}
  
  # TODO: Implementation
  def start_link(%{}) do

  end
  
  # TODO: Implementation
  def supervised_start() do
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
