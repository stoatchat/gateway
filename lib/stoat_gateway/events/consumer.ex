defmodule StoatGateway.Events.Consumer do
  use Broadway

  def start_link(_) do
    Broadway.start_link(__MODULE__,
      name: StoatGateway.Events.Consumer,
      producer: [
        module:
          {BroadwayRabbitMQ.Producer,
           connection: Application.get_env(:stoat_gateway, :rabbit),
           queue: "stoat_events",
           qos: [prefetch_count: 10],
           on_failure: :reject},
        concurrency: 1
      ],
      processors: [default: [concurrency: 100]]
    )
  end

  defp decode_message!(data) when is_binary(data) do
    data |> Jason.decode()
  end

  @impl true
  def handle_message(_, message, _) do
    message
    |> Broadway.Message.update_data(&decode_message!/1)
    |> process_message()
  end

  defp process_message(%Broadway.Message{data: {:ok, data}} = message) do
    IO.inspect(message)
    message
  end

  defp process_message(%Broadway.Message{data: {:error, reason}} = message) do
    IO.inspect(reason)
    message
  end
end
