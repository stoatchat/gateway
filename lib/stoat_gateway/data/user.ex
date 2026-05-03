defmodule Stoat.PublicUser do
  @derive Jason.Encoder

  defstruct _id: nil,
            username: nil,
            discriminator: nil,
            display_name: nil,
            avatar: %{},
            badges: nil,
            relationship: nil,
            online: nil
end

defmodule Stoat.User do
  def fetch_by_id(user_id) do
    Mongo.find_one(:mongo_db, "users", %{_id: user_id})
  end

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

  def fetch_unreads(user_id) do
    Mongo.find(:mongo_db, "channel_unreads", %{"_id.user": user_id}) |> Enum.to_list()
  end

  def fetch_policy_changes(last_acknowledged) do
    Mongo.find(:mongo_db, "policy_changes", %{"policy.created_time": %{gt: last_acknowledged}})
    |> Enum.to_list()
  end
end
