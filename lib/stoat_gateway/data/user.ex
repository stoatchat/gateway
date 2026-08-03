defmodule Stoat.PublicUser do
  use Stoat.Model

  defstruct _id: nil,
            username: nil,
            discriminator: nil,
            display_name: nil,
            avatar: %{},
            badges: nil,
            relationship: nil,
            online: nil,
            status: %{}
end

defmodule Stoat.User do
  def fetch_by_id(user_id) do
    Mongo.find_one(:mongo_db, "users", %{_id: user_id})
  end

  def fetch_by_ids(user_ids) when is_list(user_ids) do
    Mongo.find(:mongo_db, "users", %{"_id" => %{"$in" => user_ids}})
    |> Map.new(fn %{"_id" => id} = data -> {id, data} end)
  end

  def fetch_server_memberships(user_id) do
    Mongo.find(:mongo_db, "server_members", %{
      "_id.user": user_id,
      pending_deletion_at: %{"$exists": false}
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

  def fetch_user_settings(user_id, keys) do
    Mongo.find_one(:mongo_db, "user_settings", %{_id: user_id},
      projection: Map.put(Map.new(keys, fn key -> {key, 1} end), "_id", 0)
    )
  end

  def fetch_unreads(user_id) do
    Mongo.find(:mongo_db, "channel_unreads", %{"_id.user": user_id}) |> Enum.to_list()
  end

  def fetch_policy_changes(last_acknowledged) do
    Mongo.find(:mongo_db, "policy_changes", %{"policy.created_time": %{gt: last_acknowledged}})
    |> Enum.to_list()
  end

  def transform_badges(user_id, badges) do
    cutoff =
      get_in(Application.get_env(:stoat_gateway, :revolt), [
        "api",
        "users",
        "early_adopter_cutoff"
      ])

    case Needle.ULID.timestamp(user_id) do
      {:ok, timestamp} when timestamp < cutoff ->
        badges + 256

      _ ->
        badges
    end
  end
end
