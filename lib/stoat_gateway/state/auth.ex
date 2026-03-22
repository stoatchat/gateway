defmodule StoatGateway.Auth do

  def find_by_token(token) when is_binary(token) do 
    # Potentially rethink?
    case user_from_token(token) do
      %{"user_id" => _user_id} = payload -> {:user, payload}
      _ -> case bot_from_token(token) do
        %{"_id" => _bot_id} = payload -> {:bot, payload}
        _ -> nil
      end
    end
  end
  
  # Avoid calling db if nil
  def find_by_token(nil), do: nil


  defp user_from_token(token), do: Mongo.find_one(:mongo_db, "sessions", %{"token" => token})
  defp bot_from_token(token), do: Mongo.find_one(:mongo_db, "bots", %{"token" => token})
end
