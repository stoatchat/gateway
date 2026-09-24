defmodule StoatGateway.Web.Router do
  use Plug.Router

  plug(Plug.Logger)
  plug Peep.Plug, path: "/metrics", peep_worker: Stoat.Metrics.Peep
  plug(:match)
  plug(:dispatch)

  get "/" do
    upgrade_header = get_req_header(conn, "upgrade")

    case upgrade_header do
      ["websocket"] ->
        headers = StoatGateway.Web.HeaderMap.from_conn(conn)

        conn
        |> WebSockAdapter.upgrade(StoatGateway.Web.SocketHandler, headers, timeout: 35_000)
        |> halt()

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
