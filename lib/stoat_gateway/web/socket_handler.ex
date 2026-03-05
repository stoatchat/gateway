defmodule StoatGateway.Web.SocketHandler do
  require Logger

  defstruct [ready: false, format: :etf]
  @type t :: %__MODULE__{ready: boolean, format: String.t()}


  def init(query_params) do
    format = case Map.fetch(query_params, "format") do
      {:ok, "etf"} -> :etf
      {:ok, _} -> :json
      _ -> :json 
    end
    {:ok, %__MODULE__{ready: true, format: format}}
  end
  
  def handle_in({frame, opcode: _}, state) do
    data = decode_frame(frame, state.format)
    case data do
      {:ok, payload} -> handle_payload(payload, state)
      _ -> {:stop, :normal, 1007, encode_frame(%{error: "invalid"}, state.format), state} # TODO: Correct error format
    end
  end
  
  # Not sure about pattern matching the whole thing yet-
  # Might be a bit ugly as we go- 
  # v2 proto should definitely have an Enum for type and a consistent data key
  # then we can just case a payload and extract those and handle each event as
  # def handle_payload(EVENT_TYPE, %Stoat.TypingEvent{} = data, state) do...
  def handle_payload(%{"type" => "StartTyping"} = payload, state) do
    {:push, encode_frame(%{test: "test"}, state.format), state}
  end

  def handle_payload(_, state) do
    {:stop, :normal, "bruh?",state}
  end

  def handle_info(_, state) do
    {:ok, state}
  end

  def terminate(:timeout, state) do
    {:ok, state}  
  end

  def terminate(:remote, state) do
    {:ok, state}
  end 

  def terminate(:normal, state) do
    # Some sort of clean-up here
    {:ok, state}
  end

  defp encode_frame(frame, :json) do
    {:text, Jason.encode!(frame)}
  end

  defp encode_frame(frame, :etf) do
    {:binary, :erlang.term_to_binary(frame)}
  end

  defp decode_frame(frame, :json) do
    Jason.decode(frame)
  end

  defp decode_frame(frame, :etf) do
      :erlang.binary_to_term(frame)
  end
end
