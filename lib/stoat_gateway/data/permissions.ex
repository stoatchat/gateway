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
  # TODO(twitch): unsure why but i'm just not happy with any of this lol
  # Essentially an impl of https://github.com/stoatchat/for-android/blob/dev/app/src/main/java/chat/stoat/api/internals/Roles.kt#L75
  def filter_inaccessible_channels() do
  end

  def permissions_for_channel(
        %{"role_permissions" => role_permissions} = channel,
        %{"_id" => member_id} = member,
        server
      ) do
    case member_id == Map.get(server, "owner") do
      true ->
        Stoat.Permissions.Bits.max_safe()

      _ ->
        calculated = permissions_for_member(member, server)
        default_permissisons = Map.get(channel, "default_permissions", default_permissions_map())
        calculated = Bitwise.band(calculated, calculate_permissions(default_permissisons))

        role_overrides =
          Enum.map(Map.get(member, "roles", []), fn role_id ->
            role = Map.get(role_permissions, role_id, default_permissions_map())
            calculate_permissions(role)
          end)

        calculate_final_permissions(calculated, role_overrides)
    end
  end

  def permissions_for_member(member, %{"roles" => roles} = server) do
    default_permissions =
      Map.get(server, "default_permissions", Stoat.Permissions.Bits.server_default())

    permissions =
      Enum.map(Map.get(member, "roles", []), fn role_id ->
        permissions =
          Map.get(roles, role_id, %{}) |> Map.get("permissions", default_permissions_map())

        calculate_permissions(permissions)
      end)

    calculate_final_permissions(default_permissions, permissions)
  end

  defp calculate_final_permissions(default, roles) when length(roles) > 0,
    do: Bitwise.bor(default, Enum.max(roles))

  defp calculate_final_permissions(default, []), do: default

  defp calculate_permissions(%{"a" => a, "d" => d}), do: Bitwise.band(a, Bitwise.bnot(d))
  defp default_permissions_map(), do: %{"a" => 0, "d" => 0}
end
