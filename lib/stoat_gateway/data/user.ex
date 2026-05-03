defmodule Stoat.User do
  def fetch_server_memberships(user_id) do
    Mongo.find(:mongo_db, "server_members", %{
      "_id.user": user_id,
      pending_deleation_at: %{"$exists": false}
    })
    |> Enum.to_list()
  end

  def server_ids_from_memberships(memberships) when is_list(memberships) do
    Enum.map(memberships, fn member ->
      member["_id"]["server"]
    end)
  end

  def fetch_user_channels(user_id) do
    Mongo.find(:mongo_db, "channels", %{
      "$or": [
        %{
          recipients: user_id,
          "$or": [
            %{channel_type: "DirectMessage"},
            %{channel_type: "Group"}
          ]
        },
        %{channel_type: "SavedMessages", user: user_id}
      ]
    })
    |> Enum.to_list()
  end

  def fetch_user_settings(user_id) do
    Mongo.find_one(:mongo_db, "user_settings", %{_id: user_id})
  end
end
