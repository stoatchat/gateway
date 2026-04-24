defmodule StoatGateway.Server do
  use GenServer
  require Logger

  defstruct id: nil,
            data: %{},
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
    server = Stoat.Server.fetch_by_id(state.id)
    {:ok, %{state | data: server}, {:continue, :get_state}}
  end

  def handle_continue(:get_state, state) do
    {:noreply, state}
  end

  def dispatch_event(id, event, data) do
  end

  def handle_cast({:link_session, user_id, session_pid}, state) do
    Logger.debug("server:#{inspect(self())} add session:#{user_id}:#{inspect(session_pid)}")
    Process.monitor(session_pid)
    {:noreply, %{state | linked_sessions: state.linked_sessions ++ [{user_id, session_pid}]}}
  end

  def handle_cast({:dispatch_begin_typing, channel_id, user_id}, state) do
    Logger.debug("server:#{inspect(self())} dispatching typing by #{user_id} to #{channel_id}")
    {:noreply, state}
  end

  def handle_info({:DOWN, _ref, :process, pid, _}, state) do
    {:noreply,
     %{
       state
       | linked_sessions:
           Enum.reject(state.linked_sessions, fn {_, session_pid} -> session_pid == pid end)
     }}
  end
end
