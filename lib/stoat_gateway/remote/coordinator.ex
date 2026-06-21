defmodule StoatGateway.Remote.Coordinator do
  @moduledoc """
  GenServer which monitors node joins/leaves on the cluster to adjust the hash ring.
  Responsible for the folllwing:  
  - Updating the HashRing
  - Coordinating rebalancing of Servers onto new Nodes
  - Safely "decomissioning" a node by moving its Servers
  """
  use GenServer

  alias ExHashRing.Ring
  require Logger
  
  @impl true 
  def start_link(args) do
  end

  @impl true
  def init() do
  end
  
  @impl true
  def code_change() do
  end

  @impl true
  def terminate() do
  end
end
