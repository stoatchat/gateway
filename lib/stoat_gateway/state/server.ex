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
    {:ok, %{state | data: server}, {:continue, :ensure_init_state}}
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
      bot: session_type,
      roles: Map.get(member_data, "roles", [])
    }

    if not user_session_exists?(session, state.linked_sessions) do
      :pg.join(:presence, user_id, self())
    end

    {:noreply, %{state | linked_sessions: [session | state.linked_sessions]}}
  end

  def handle_cast({:dispatch, event, payload}, state) do
    fanout({event, payload}, state.linked_sessions)
    {:noreply, state}
  end

  # TODO: probably want a general dispatch catch and then another func for specific topics
  # say channel, overall
  def handle_cast({:dispatch_begin_typing, channel_id, user_id}, state) do
    fanout(
      {:ChannelStartTyping, %{type: "ChannelStartTyping", id: channel_id, user: user_id}},
      state.linked_sessions
    )

    Logger.debug("server:#{inspect(self())} dispatching typing by #{user_id} to #{channel_id}")
    {:noreply, state}
  end

  def handle_info({:presence_update, payload}, state) do
    fanout(payload, state.linked_sessions)
    {:noreply, state}
  end

  def handle_info({:DOWN, _ref, :process, pid, _}, state) do
    user =
      Enum.find(state.linked_sessions, %{}, fn session ->
        session.pid == pid
      end)

    new_sessions =
      Enum.reject(state.linked_sessions, fn session ->
        session.pid == pid
      end)

    if not user_session_exists?(user, new_sessions) do
      :pg.leave(:presence, user.id)
    end

    {:noreply,
     %{
       state
       | linked_sessions: new_sessions
     }}
  end

  def fanout(event, sessions) do
    # TODO: just take an enum of sessions and higher level functions can filter as needed
    Enum.each(sessions, &send(&1.pid, {:socket_dispatch, event}))
  end

  defp user_session_exists?(user, sessions) do
    Enum.any?(sessions, fn session -> session.user_id == user.user_id end)
  end

  defp build_channel_tuples(state) do
    channels = Map.get(state.data, "channels")

    Enum.map(channels, fn id ->
      {id, state.id}
    end)
  end
end
