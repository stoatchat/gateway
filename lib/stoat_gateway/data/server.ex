defmodule Stoat.Server do
  def fetch_by_id(id) when is_binary(id), do: Mongo.find_one(:mongo_db, "servers", %{"_id" => id})
end
