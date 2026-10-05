defmodule StoatGateway.Web.Router do
  require OpenTelemetry.Tracer
  alias OpenTelemetry.Tracer
  use Plug.Router

  plug(Peep.Plug, path: "/metrics", peep_worker: Stoat.Metrics.Peep)
  plug(:match)
  plug(:dispatch)

  get "/" do
    upgrade_header = get_req_header(conn, "upgrade")

    case upgrade_header do
      ["websocket"] ->
        headers = StoatGateway.Web.HeaderMap.from_conn(conn)

        Tracer.with_span :websocket_init do
          trace_id = Tracer.current_span_ctx() |> OpenTelemetry.Span.hex_trace_id()

          conn
          |> put_resp_header("x-gateway-trace", trace_id)
          |> WebSockAdapter.upgrade(
            StoatGateway.Web.SocketHandler,
            {headers, Tracer.current_span_ctx()},
            timeout: 45_000
          )
          |> halt()
        end

      _ ->
        conn
        |> put_resp_header("content-type", "application/json")
        |> send_resp(:ok, Jason.encode!(%{version: version()}))
    end
  end

  get _ do
    send_resp(conn, 404, "Not Found")
  end

  @version Mix.Project.config()[:version]
  def version(), do: @version
end
