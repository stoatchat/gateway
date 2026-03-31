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

  defstruct ready: false, user_id: nil, session: nil, linked_socket: nil, type: :user

  @type t :: %__MODULE__{
          ready: boolean,
          user_id: String.t(),
          session: String.t(),
          linked_socket: pid(),
          type: atom()
        }

  def start_link(%{socket: socket, data: %{"user_id" => id, "_id" => session} = data}) do
    IO.inspect(data)

    GenServer.start_link(__MODULE__, %__MODULE__{
      user_id: id,
      linked_socket: socket,
      session: session
    })
  end

  def init(state) do
    # TODO: Add metrics here for connected session 
    IO.puts("session: #{inspect(self())} with state: #{inspect(state)}")
    {:ok, state, {:continue, :ready}}
  end

  def handle_continue(:ready, state) do
    server_ids = Stoat.User.fetch_server_memberships(state.user_id)
    servers = Stoat.Server.fetch_many(server_ids)

    channel_ids = servers |> Enum.to_list() |> Enum.map(& &1["channels"]) |> List.flatten()
    # TODO: calculate viewable channels with permissions
    channels = Mongo.find(:mongo_db, "channels", %{_id: %{"$in": channel_ids}}) |> Enum.to_list()

    ready_payload = %Stoat.State.Ready{servers: servers, channels: channels}
    send(state.linked_socket, {:ready, ready_payload})
    {:noreply, %{state | ready: true}}
  end
end
