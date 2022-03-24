defmodule ExMicrosoftAzureStorage.Storage.BlobStorageTest do
  @moduledoc false

  use ExUnit.Case, async: true

  @moduletag :external

  import ExMicrosoftAzureStorage.Factory

  alias ExMicrosoftAzureStorage.Storage.BlobStorage
  alias ExMicrosoftAzureStorage.Storage.BlobStorage.ServiceProperties

  setup do
    storage_context = build(:storage_context)

    %{storage_context: storage_context}
  end

  describe "get_blob_service_stats" do
    test "gets blob service stats", %{storage_context: storage_context} do
      assert {:ok, %{geo_replication: %{last_sync_time: last_sync_time, status: "live"}}} =
               storage_context |> BlobStorage.get_blob_service_stats()

      assert last_sync_time
    end
  end

  describe "get_blob_service_properties" do
    test "gets blob service properties", %{storage_context: storage_context} do
      assert {:ok, %{service_properties: %ServiceProperties{}}} =
               storage_context |> BlobStorage.get_blob_service_properties()
    end
  end

  describe "put_blob_service_properties" do
    test "sets CORS rules", %{storage_context: storage_context} do
      rule = %{
        allowed_origins: ["https://google.com"],
        allowed_methods: ["GET"],
        max_age_in_seconds: 600,
        exposed_headers: [""],
        allowed_headers: [""]
      }

      cors_rule = rule |> ServiceProperties.CorsRule.to_struct()

      {:ok, %{service_properties: service_properties}} =
        storage_context |> BlobStorage.get_blob_service_properties()

      service_properties = Map.put(service_properties, :cors_rules, [cors_rule])

      storage_context
      |> BlobStorage.set_blob_service_properties(service_properties)

      {:ok, %{service_properties: service_properties_after_update}} =
        storage_context |> BlobStorage.get_blob_service_properties()

      assert service_properties == service_properties_after_update
    end
  end
end
