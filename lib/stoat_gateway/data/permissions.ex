defmodule Stoat.Permissions.Bits do
  import Bitwise
  def manage_channel, do: 1 <<< 0
  def manage_server, do: 1 <<< 1
  def manage_permissions, do: 1 <<< 2
  def manage_roles, do: 1 <<< 3
  def manage_customization, do: 1 <<< 4

  def kick_members, do: 1 <<< 6
  def ban_members, do: 1 <<< 7
  def timeout_members, do: 1 <<< 8
  def assign_roles, do: 1 <<< 9
  def change_nickname, do: 1 <<< 10
  def manage_nicknames, do: 1 <<< 11
  def change_avatar, do: 1 <<< 12
  def remove_avatars, do: 1 <<< 13

  def view_channel, do: 1 <<< 20
  def read_message_history, do: 1 <<< 21
  def send_message, do: 1 <<< 22
  def manage_messages, do: 1 <<< 23
  def manage_webhooks, do: 1 <<< 24
  def invite_others, do: 1 <<< 25
  def send_embeds, do: 1 <<< 26
  def upload_files, do: 1 <<< 27
  def masquerade, do: 1 <<< 28
  def react, do: 1 <<< 29

  def voice_connect, do: 1 <<< 30
  def voice_speak, do: 1 <<< 31
  def voice_video, do: 1 <<< 32
  def mute_members, do: 1 <<< 33
  def deafen_members, do: 1 <<< 34
  def move_members, do: 1 <<< 35
  def voice_listen, do: 1 <<< 36
  def mention_everyone, do: 1 <<< 37
  def mention_roles, do: 1 <<< 38

  def max_safe, do: 0x000FFFFFFFFFFFFF

  def view_only, do: view_channel() + read_message_history()

  def default do
    view_only() + send_message() + invite_others() + send_embeds() + upload_files() +
      voice_connect() + voice_speak() + voice_video() + voice_listen()
  end

  def server_default do
    default() + react() + change_nickname() + change_avatar()
  end
end

defmodule Stoat.Permissions do
  # TODO(twitch?):
  # Recreate Calculating viewable channels based on permissions
  # https://github.com/stoatchat/stoatchat/blob/main/crates/core/permissions/src/impl.rs#L81

  def filter_inaccessible_channels() do
  end

  # Impl of https://github.com/stoatchat/stoatchat/blob/main/crates/bonfire/src/events/impl.rs#L20
  def permissions_for_channel(channel, %{"_id" => member_id} = member, server) do
    case member_id == Map.get(server, "owner") do
      true ->
        Stoat.Permissions.Bits.max_safe()

      _ ->
        calculated = calculate_member_permissions(member, server)
    end
  end

  defp calculate_member_permissions(member, server) do
    calculated = Map.get(server, "default_permissions", Stoat.Permissions.Bits.server_default())

    Enum.each(Map.get(member, "roles", []), fn role_id ->
      nil
    end)
  end
end
