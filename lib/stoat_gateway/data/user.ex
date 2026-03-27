defmodule Stoat.User do
  def fetch_server_memberships(user_id) do
     Mongo.find(:mongo_db, "server_members", %{"_id.user": user_id}) |> Enum.to_list |> Enum.map(& &1["_id"]["server"]) 
  end
end
