defmodule StoatGateway.Web.Router do
  use Plug.Router

  plug(Plug.Logger)
  plug(:match)
  plug(:dispatch)

  get "/ws" do
    upgrade_header = get_req_header(conn, "upgrade")

    case upgrade_header do
      ["websocket"] ->
        conn = fetch_query_params(conn)

        conn
        |> WebSockAdapter.upgrade(StoatGateway.Web.SocketHandler, conn.query_params,
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
