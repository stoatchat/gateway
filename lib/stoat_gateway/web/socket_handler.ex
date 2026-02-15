defmodule StoatGateway.Web.SocketHandler do
  require Logger

  def init(state) do
    {:ok, state}
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
