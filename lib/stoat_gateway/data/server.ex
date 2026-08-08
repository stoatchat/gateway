defmodule Stoat.Server do
  def fetch_by_id(id) when is_binary(id), do: Mongo.find_one(:mongo_db, "servers", %{"_id" => id})

  def fetch_many(ids) when is_list(ids),
    do: Mongo.find(:mongo_db, "servers", %{_id: %{"$in": ids}}) |> Enum.to_list()

  def fetch_by_channel_id(id) when is_binary(id) do
    case Mongo.find_one(:mongo_db, "channels", %{_id: id}) do
      %{"server" => server_id} -> server_id
      _ -> nil
    end
  end

  def fetch_channels(id) when is_binary(id) do
    Mongo.find(:mongo_db, "channels", %{"server" => id}) |> Enum.to_list()
  end

  def find_emojis_by_many(server_ids) when is_list(server_ids) do
    Mongo.find(:mongo_db, "emojis", %{"parent.id": %{"$in": server_ids}}) |> Enum.to_list()
  end

  @spec fetch_approximate_user_count(binary()) :: binary()
  def fetch_approximate_user_count(id) when is_binary(id) do
    case Redix.command(:redix, ["GET", "member_count:#{id}"]) do
      {:ok, nil} ->
        {:ok, count} =
          Mongo.count_documents(:mongo_db, "server_members", %{
            "_id.server" => id,
            "pending_deletion_at" => %{"$exists" => false}
          })

        Redix.command(:redix, ["SET", "member_count:#{id}", count, "EX", "3600"])
        count

      {:ok, value} when is_binary(value) ->
        String.to_integer(value)
    end
  end
end
