defmodule ExMicrosoftAzureStorage.StorageTest do
  @moduledoc false

  use ExUnit.Case, async: true

  import ExMicrosoftAzureStorage.Factory

  alias ExMicrosoftAzureStorage.Storage

  describe "new" do
    test "can be created from an Azure connection string" do
      storage = build(:connection_string) |> Storage.new()

      assert is_binary(storage.account_name)
      assert is_binary(storage.account_key)
      assert is_binary(storage.default_endpoints_protocol)
      assert is_binary(storage.endpoint_suffix)

      assert is_nil(storage.host)
      assert is_nil(storage.aad_token_provider)

      assert false == storage.is_development_factory
    end
  end
end
