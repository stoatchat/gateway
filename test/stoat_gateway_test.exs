defmodule StoatGatewayTest do
  use ExUnit.Case
  doctest StoatGateway

  test "greets the world" do
    assert StoatGateway.hello() == :world
  end
end
