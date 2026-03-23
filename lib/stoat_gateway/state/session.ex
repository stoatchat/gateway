defmodule Stoat.State.Ready do
  @derive Jason.Encoder

  defstruct type: "Ready",
    users: [],
    servers: [],
    channels: [],
    members: [],
    emojis: [],
    voice_states: [],
    policy_changes: []
end

defmodule StoatGateway.Session do
  use GenServer

  defstruct [ready: false, user_id: nil, session: nil, linked_socket: nil, type: :user]
  @type t :: %__MODULE__{ready: boolean, user_id: String.t(), session: String.t(), linked_socket: pid(), type: atom()}

  def start_link(%{socket: socket, data: %{"user_id" => id, "_id" => session}}) do
    GenServer.start_link(__MODULE__, %__MODULE__{user_id: id, linked_socket: socket, session: session})
  end

  def init(state) do
    # TODO: Add metrics here for connected session 
    IO.puts("session: #{inspect(self())} with state: #{inspect(state)}")
    {:ok, state, {:continue, :ready}}
  end

  def handle_continue(:ready, state) do
    # TODO: Fill with data from Mongo
    ready_payload = %Stoat.State.Ready{}
    send(state.linked_socket, {:ready, ready_payload})
    {:noreply, state}
  end
end
