defmodule StoatGateway.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Bandit, plug: StoatGateway.Web.Router, scheme: :http, port: Application.get_env(:stoat_gateway, :ws_port)},
      StoatGateway.Events.Consumer
    ]

    opts = [strategy: :one_for_one, name: StoatGateway.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
