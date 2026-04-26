defmodule Stoat.User do
  def fetch_server_memberships(user_id) do
    Mongo.find(:mongo_db, "server_members", %{"_id.user": user_id, pending_deleation_at: %{"$exists": false}})
    |> Enum.to_list()
  end

  def server_ids_from_memberships(memberships) when is_list(memberships) do
    Enum.map(memberships, fn member -> 
      member["_id"]["server"]
    end)
  end
end
