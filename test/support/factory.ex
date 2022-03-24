defmodule ExMicrosoftAzureStorage.Factory do
  @moduledoc """
  Provides test data factories.
  """

  use ExMachina

  def blob_data_factory(_attrs), do: sequence("blob_data")

  def blob_name_factory(_attrs), do: sequence("blob_name")

  def connection_string_factory(attrs) do
    [
      [
        "DefaultEndpointsProtocol",
        Map.get(attrs, :default_endpoints_protocol, sequence("default_endpoints_protocol"))
      ],
      ["AccountName", Map.get(attrs, :account_name, sequence("account_name"))],
      ["AccountKey", Map.get(attrs, :account_key, sequence("account_key"))],
      ["EndpointSuffix", Map.get(attrs, :endpoint_suffix, sequence("endpoint_suffix"))]
    ]
    |> Enum.map_join(";", fn kv -> Enum.join(kv, "=") end)
  end

  def content_type_factory(_attrs), do: sequence("application/type")

  def content_md5_factory(_attrs), do: sequence("md5") |> Base.encode64()

  def storage_context_factory do
    ExMicrosoftAzureStorage.Storage.development_factory()
  end

  def value_factory(_attrs) do
    sequence("value")
  end
end
