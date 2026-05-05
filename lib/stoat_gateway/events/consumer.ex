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
    process_event(data)
    message
  end

  defp process_message(%Broadway.Message{data: {:error, reason}} = message) do
    IO.inspect(reason)
    message
  end

  # TODO: Determine nicest way, ideally we do map pattern matching once, easier readability
  def process_event(%{"type" => event_type} = data) do
    # TODO: wrap telemetry and otel context around this
    # Can also add future pushnotif decisions here?
    handle_event(event_type, data)
  end

  def handle_event("Message", %{"member" => %{"_id" => %{"server" => server_id}}} = payload) do
    {:ok, server} = StoatGateway.Server.lookup_or_start(server_id)
    StoatGateway.Server.dispatch(server, "Message", payload)
  end

  def handle_event("UserSettingsUpdate", %{"id" => user_id} = data) do
    sessions = Registry.lookup(Stoat.Sessions, user_id)

    Enum.each(sessions, fn {pid, _session_id} ->
      send(pid, {:socket_dispatch, {:UserSettingsUpdate, data}})
    end)
  end
end
