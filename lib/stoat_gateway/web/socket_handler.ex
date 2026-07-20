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

    with {:ok, token} <- Map.fetch(query_params, "token") do
      handle_auth(token, format)
    else
      _ ->
        {:ok, %__MODULE__{ready: false, format: format}}
    end
  end

  @impl true
  def handle_in({frame, opcode: _}, state) do
    data = decode_frame(frame, state.format)

    case data do
      {:ok, %{"type" => type} = payload} ->
        handle_payload(String.downcase(type), payload, state)

      _ ->
        {:stop, :normal, 1007,
         build_error(
           "InvalidPayload",
           "Incoming payload does not match required format",
           state.format
         ), state}
    end
  end

  def handle_payload(
        "authenticate",
        %{"token" => token} = _payload,
        %{ready: false} = state
      ) do
    handle_auth(token, state.format)
  end

  def handle_payload("ping", %{"data" => data} = _payload, %{ready: true} = state) do
    {:push, encode_frame(%{type: "Pong", data: data}, state.format), state}
  end

  def handle_payload(
        "begintyping",
        %{"channel" => channel_id} = _payload,
        %{ready: true} = state
      ) do
    GenServer.cast(state.linked_socket, {:event_typing, :ChannelStartTyping, channel_id})
    {:ok, state}
  end

  def handle_payload(
        "endtyping",
        %{"channel" => channel_id} = _payload,
        %{ready: true} = state
      ) do
    GenServer.cast(state.linked_socket, {:event_typing, :ChannelStopTyping, channel_id})
    {:ok, state}
  end

  def handle_payload(_, _, %{ready: false} = state) do
    {:stop, :normal, 3000, build_error("InvalidSession", "Not Authenticated", state.format),
     state}
  end

  def handle_payload(_, _, state) do
    {:stop, :normal, 1007, build_error("InvalidPayload", state.format), state}
  end

  @impl true
  def handle_info({:ready, payload}, state) do
    {:push, encode_frame(payload, state.format), state}
  end

  @impl true
  def handle_info({:event_dispatch_raw, body}, state) do
    {:push, encode_frame(body, state.format), state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, _pid, _}, state) do
    {:stop, :shutdown, 1011, build_error("ServerError", state.format), state}
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

  @impl true
  def terminate(:shutdown, state) do
    {:ok, state}
  end

  @impl true
  def terminate({:error, reason}, state) do
    Logger.warning("Closing socket with error: #{inspect(reason)}")
    {:ok, state}
  end

  defp handle_auth(token, format) do
    with {type, data} <- StoatGateway.Auth.find_by_token(token) do
      # NOTE: replace this with lookup for SessionResume in the future
      {:ok, socket_pid} =
        DynamicSupervisor.start_child(
          Stoat.Sessions.Supervisor,
          {StoatGateway.Session, %{data: data, socket: self(), type: type}}
        )

      Process.monitor(socket_pid)

      {:push, build_event(:Authenticated, format),
       %__MODULE__{ready: true, format: format, linked_socket: socket_pid}}
    else
      _ ->
        {:push, build_error(:InvalidSession, "Invalid token provided", format),
         %__MODULE__{ready: false, format: format}}
    end
  end

  @spec build_event(binary(), atom()) :: {:text | :binary, binary()}
  defp build_event(event, format) do
    encode_frame(%{type: event}, format)
  end

  @spec _build_event(binary(), map(), atom()) :: {:text | :binary, binary()}
  defp _build_event(event, payload, format) do
    encode_frame(%{type: event, data: payload}, format)
  end

  @spec build_error(binary(), atom()) :: {:text | :binary, binary()}
  defp build_error(error_type, format) do
    encode_frame(%{type: "Error", data: %{type: error_type}}, format)
  end

  @spec build_error(binary(), binary(), atom()) :: {:text | :binary, binary()}
  defp build_error(error_type, detail, format) do
    encode_frame(%{type: "Error", data: %{type: error_type, detail: detail}}, format)
  end

  @spec encode_frame(map(), :json | :etf | :msgpack) :: {:text | :binary, binary()}
  defp encode_frame(frame, :json), do: {:text, Jason.encode!(frame)}
  defp encode_frame(frame, :etf), do: {:binary, :erlang.term_to_binary(frame)}
  defp encode_frame(frame, :msgpack), do: {:binary, :msgpack.pack(frame)}

  defp decode_frame(frame, :json), do: Jason.decode(frame)
  defp decode_frame(frame, :etf), do: :erlang.binary_to_term(frame)
  defp decode_frame(frame, :msgpack), do: :msgpack.unpack(frame)
end
