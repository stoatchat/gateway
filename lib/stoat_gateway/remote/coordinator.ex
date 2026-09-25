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
  
  def start_link(state) do
    GenServer.start_link(__MODULE__, state)
  end

  @impl true
  def init(_) do
    :net_kernel.monitor_nodes(true)
    {:ok, ring} = Ring.start_link(name: :test_ring)
    Ring.add_node(ring, node())
    {:ok, %{ring: ring}}
  end
  
  @impl true
  def handle_info({:nodeup, node}, state) do
    Ring.add_node(state.ring, node)
    {:noreply, state}
  end
  
  @impl true
  def handle_info({:nodedown, node}, state) do
    Ring.remove_node(state.ring, node)
    {:noreply, state}
  end

  def code_change(_old_vsn, state) do
    {:ok, state}
  end

  def terminate() do
  end
end
