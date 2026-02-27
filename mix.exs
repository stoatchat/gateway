defmodule StoatGateway.MixProject do
  use Mix.Project

  def project do
    [
      app: :stoat_gateway,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :observer, :wx],
      mod: {StoatGateway.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:bandit, "~> 1.10"},
      {:websock_adapter, "~> 0.5.9"},
      {:broadway_rabbitmq, "~> 0.8.2"},
      {:jason, "~> 1.4"}
    ]
  end
end
