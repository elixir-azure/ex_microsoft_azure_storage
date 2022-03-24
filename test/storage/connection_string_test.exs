defmodule ExMicrosoftAzureStorage.Storage.ConnectionStringTest do
  @moduledoc false

  use ExUnit.Case, async: true

  import ExMicrosoftAzureStorage.Factory

  alias ExMicrosoftAzureStorage.Storage.ConnectionString

  describe "parse" do
    test "parses a connection string" do
      default_endpoints_protocol = "https"
      account_name = "my_account_name"
      account_key = "my_account_key"
      endpoint_suffix = "my_endpoint_suffix"

      attrs = %{
        default_endpoints_protocol: default_endpoints_protocol,
        account_name: account_name,
        account_key: account_key,
        endpoint_suffix: endpoint_suffix
      }

      connection_string = build(:connection_string, attrs)

      assert attrs == ConnectionString.parse(connection_string)
    end
  end
end
