defmodule StoatGateway.Web.Router do
  use Plug.Router
  
  plug(Plug.Logger)
  plug(:match)
  plug (:dispatch)
  
  get "/ws" do
    conn  # Double check this is OK, need to check for upgrade headers.
    |> WebSockAdapter.upgrade(StoatGateway.Web.SocketHandler, [], timeout: 10_000)
    |> halt()
  end


  get _ do 
    send_resp(conn, 404, "Not Found")
  end
end
