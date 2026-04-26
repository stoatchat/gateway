defmodule StoatGateway.Web.SocketHandler do
  require Logger

  @behaviour WebSock

  defstruct ready: false, format: :json, linked_socket: nil
  @type t :: %__MODULE__{ready: boolean(), format: String.t(), linked_socket: pid()}

  @impl true
  def init(query_params) do
    format =
      case Map.get(query_params, "format") do
        "etf" -> :etf
        "msgpack" -> :msgpack
        _ -> :json
      end

    with {:ok, token} <- Map.fetch(query_params, "token"),
         {type, data} <- StoatGateway.Auth.find_by_token(token) do
      # Start the session process
      # TODO: Check for alive session process-
      {:ok, socket_pid} =
        DynamicSupervisor.start_child(
          Stoat.Sessions.Supervisor,
          {StoatGateway.Session, %{data: data, socket: self(), type: type}}
        )

      {:push, build_event(:Authenticated, format),
       %__MODULE__{ready: true, format: format, linked_socket: socket_pid}}
    else
      _ ->
        {:stop, :normal, 1007, build_error("InvalidSession", format),
         %__MODULE__{ready: false, format: format}}
    end
  end

  @impl true
  def handle_in({frame, opcode: _}, state) do
    data = decode_frame(frame, state.format)

    case data do
      {:ok, payload} -> handle_payload(payload, state)
      # TODO: Correct error format
      _ -> {:stop, :normal, 1007, build_error("InvalidPayload", state.format), state}
    end
  end

  # Not sure about pattern matching the whole thing yet-
  # Might be a bit ugly as we go- 
  # v2 proto should definitely have an Enum for type and a consistent data key
  # then we can just case a payload and extract those and handle each event as
  # def handle_payload(EVENT_TYPE, %Stoat.TypingEvent{} = data, state) do...

  def handle_payload(%{"type" => "Ping"} = _payload, state) do
    {:push, encode_frame(%{type: "Pong", data: System.os_time()}, state.format), state}
  end

  def handle_payload(%{"type" => "BeginTyping", "channel" => channel_id} = _payload, state) do
    GenServer.cast(state.linked_socket, {:event_begin_typing, channel_id})
    {:ok, state}
  end

  def handle_payload(%{"type" => "EndTyping", "channel" => _channel_id} = _payload, state) do
    {:ok, state}
  end

  def handle_payload(_, state) do
    {:stop, :normal, 1007, build_error("InvalidPayload", state.format), state}
  end

  @impl true
  def handle_info({:ready, payload}, state) do
    {:push, encode_frame(payload, state.format), state}
  end

  @impl true
  def handle_info(_, state) do
    {:ok, state}
  end

  @impl true
  def terminate(:timeout, state) do
    {:ok, state}
  end

  @impl true
  def terminate(:remote, state) do
    {:ok, state}
  end

  @impl true
  def terminate(:normal, state) do
    # Some sort of clean-up here
    {:ok, state}
  end

  @spec build_event(binary(), atom()) :: {:text | :binary, binary()}
  defp build_event(event, format) do
    encode_frame(%{type: event}, format)
  end

  @spec build_event(binary(), map(), atom()) :: {:text | :binary, binary()}
  defp build_event(event, payload, format) do
    encode_frame(%{type: event, data: payload}, format)
  end

  @spec build_error(binary(), atom()) :: {:text | :binary, binary()}
  defp build_error(detail, format) do
    encode_frame(%{type: "Error", data: %{type: detail}}, format)
  end

  @spec encode_frame(map(), :json | :etf | :msgpack) :: {:text | :binary, binary()}
  defp encode_frame(frame, :json), do: {:text, Jason.encode!(frame)}
  defp encode_frame(frame, :etf), do: {:binary, :erlang.term_to_binary(frame)}
  defp encode_frame(frame, :msgpack), do: {:binary, :msgpack.pack(frame)}

  defp decode_frame(frame, :json), do: Jason.decode(frame)
  defp decode_frame(frame, :etf), do: :erlang.binary_to_term(frame)
  defp decode_frame(frame, :msgpack), do: :msgpack.unpack(frame)
end
