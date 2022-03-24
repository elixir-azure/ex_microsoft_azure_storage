defmodule ExMicrosoftAzureStorage.Storage.ConnectionString do
  @moduledoc """
  ExMicrosoftAzureStorage Storage connection string utilities.
  """

  @doc """
  Parses an ExMicrosoftAzureStorage storage connection string into a plain map.

  Keys are normalised into lower case with underscores for convenience.
  """
  @spec parse(connection_string :: String.t()) :: map()
  def parse(connection_string) do
    connection_string
    |> String.split(";")
    |> Enum.reduce(%{}, fn key_value_string, acc ->
      {key, value} = parse_connection_string_item(key_value_string)

      Map.put(acc, key, value)
    end)
  end

  defp parse_connection_string_item(item) do
    # The value part of the item can contain `=` (esp the account key which is base64-encoded), so
    # `parts: 2` is essential.
    [k, v] = item |> String.split("=", parts: 2)

    {key_for(k), v}
  end

  defp key_for("DefaultEndpointsProtocol"), do: :default_endpoints_protocol
  defp key_for("AccountName"), do: :account_name
  defp key_for("AccountKey"), do: :account_key
  defp key_for("EndpointSuffix"), do: :endpoint_suffix
end
