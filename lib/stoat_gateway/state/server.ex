defmodule StoatGateway.Server do
  use GenServer
  require Logger

  defstruct id: nil,
            # monitor_ref: {user_id, socket_pid}?
            linked_sockets: []

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
    # TODO: Add into state, fully define struct first
    server = Stoat.Server.fetch_by_id(state.id)
    {:ok, state, {:continue, :get_state}}
  end

  def handle_continue(:get_state, state) do
    {:noreply, state}
  end

  def dispatch_event(id, event, data) do
  end

  def handle_cast({:link_session, user_id, session_pid}, state) do
    Logger.debug("server:#{inspect(pid)} add session:#{user_id}:#{inspect(session_pid)}")
    Process.monitor(session_pid)
    {:noreply, %{state | linked_sockets: state.linked_sockets ++ [{user_id, session_pid}]}}
  end

  def handle_info({:DOWN, _ref, :process, pid, _}, state) do
    {:noreply,
     %{
       state
       | linked_sockets:
           Enum.reject(state.linked_sockets, fn {u_id, session_pid} -> session_pid == pid end)
     }}
  end
end
