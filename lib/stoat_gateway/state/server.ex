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
    server = Stoat.Server.fetch_by_id(state.id)
    {:ok, %{state | data: server}, {:continue, :get_state}}
  end

  def handle_continue(:get_state, state) do
    {:noreply, state}
  end

  def dispatch_event(id, event, data) do
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
      bot: session_type,
      roles: Map.get(member_data, "roles", [])
    }

    {:noreply, %{state | linked_sessions: [session | state.linked_sessions]}}
  end

  # TODO: probably want a general dispatch catch and then another func for specific topics
  # say channel, overall
  def handle_cast({:dispatch_begin_typing, channel_id, user_id}, state) do
    fanout({:ChannelStartTyping, %{id: channel_id, user: user_id}}, state)
    Logger.debug("server:#{inspect(self())} dispatching typing by #{user_id} to #{channel_id}")
    {:noreply, state}
  end

  def fanout(event, %{linked_sessions: sessions} = _state) do
    # TODO: just take an enum of sessions and higher level functions can filter as needed
    Enum.each(sessions, &send(&1.pid, {:socket_dispatch, event}))
  end

  def handle_info({:DOWN, _ref, :process, pid, _}, state) do
    {:noreply,
     %{
       state
       | linked_sessions: Enum.reject(state.linked_sessions, fn session -> session.pid == pid end)
     }}
  end
end
