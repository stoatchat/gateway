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
  
  def handle_in({text, opcode: :text}, state) do
    {:push, {:text, text}, state}
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
end
