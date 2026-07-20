defmodule StoatGateway.Web.Router do
  use Plug.Router

  plug(Plug.Logger)
  plug(:match)
  plug(:dispatch)

  get "/" do
    upgrade_header = get_req_header(conn, "upgrade")

    case upgrade_header do
      ["websocket"] ->
        headers = StoatGateway.Web.HeaderMap.from_conn(conn)

        conn
        |> WebSockAdapter.upgrade(StoatGateway.Web.SocketHandler, headers,
          timeout: 35_000
        )
        |> halt()

      _ ->
        send_resp(conn, 400, "No upgrade headers")
    end
  end

  get _ do
    send_resp(conn, 404, "Not Found")
  end
end
