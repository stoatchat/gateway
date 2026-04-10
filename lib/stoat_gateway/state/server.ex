defmodule StoatGateway.Server do
  use GenServer

  defstruct id: nil,
            # monitor_ref: {socket_pid, user_id}?
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

  def link_session(id, user_id, session_pid) do
  end

  def dispatch_event(id, event, data) do
  end
end
