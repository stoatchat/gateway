alias Plug.Conn

defmodule StoatGateway.Web.HeaderMap do
  @opaque t :: %{String.t() => list(String.t())}

  def from_conn(%Conn{query_string: query}) do
    decode(query)
  end

  def decode("") do
    %{}
  end

  def decode(query) when is_binary(query) do
    parts = :binary.split(query, "&", [:global])
    Enum.reduce(parts, %{}, &decode_param(&1, &2))
  end

  defp decode_param(param, headers) do
    decode_pair(:binary.split(param, "="), headers)
  end

  defp decode_pair([key, value], headers) do
    key = URI.decode_www_form(key)
    value = URI.decode_www_form(value)

    Map.update(headers, key, [value], fn values ->
      [value | values]
    end)
  end

  defp decode_pair(_, headers) do
    headers
  end

  def get_singular(headers, key, default \\ nil) do
    values = Map.get(headers, key, default)

    if values do
      List.first(values, default)
    else
      default
    end
  end

  def fetch_singular(headers, key) do
    values = Map.get(headers, key)

    if values do
      case List.first(values) do
        nil -> :error
        value -> {:ok, value}
      end
    else
      :error
    end
  end
end
