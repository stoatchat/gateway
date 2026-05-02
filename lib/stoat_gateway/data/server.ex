defmodule Stoat.Server do
  # TODO: some sort of caching with ETS- Server GenServers can then keep this updated as they handle events
  # 1) Ideally only the Server makes calls to Mongo regarding its data, 
  # 2) We can just send the client a list of server ids on ready
  # 3) 
  def fetch_by_id(id) when is_binary(id), do: Mongo.find_one(:mongo_db, "servers", %{"_id" => id})

  def fetch_many(ids) when is_list(ids),
    do: Mongo.find(:mongo_db, "servers", %{_id: %{"$in": ids}}) |> Enum.to_list()

  def fetch_by_channel_id(id) when is_binary(id) do
    case Mongo.find_one(:mongo_db, "channels", %{_id: id}) do
      %{"server" => server_id} -> server_id
      _ -> nil
    end
  end

  def find_emojis_by_many(server_ids) when is_list(server_ids) do
    Mongo.find(:mongo_db, "emojis", %{"parent.id": %{"$in": server_ids}}) |> Enum.to_list()
  end
end
