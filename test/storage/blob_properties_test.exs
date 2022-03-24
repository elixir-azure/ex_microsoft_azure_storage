defmodule ExMicrosoftAzureStorage.Storage.BlobPropertiesTest do
  @moduledoc false

  use ExUnit.Case, async: true

  alias ExMicrosoftAzureStorage.Storage.BlobProperties

  describe "deserialise" do
    test "deserialises blob properties from headers" do
      headers = [
        {"server", "Azurite-Blob/3.11.0"},
        {"last-modified", "Mon, 12 Jul 2021 18:18:21 GMT"},
        {"x-ms-creation-time", "Mon, 12 Jul 2021 18:18:21 GMT"},
        {"x-ms-blob-type", "BlockBlob"},
        {"x-ms-lease-state", "available"},
        {"x-ms-lease-status", "unlocked"},
        {"content-length", "12"},
        {"content-type", "application/octet-stream"},
        {"etag", "\"0x198B53AAAB848F0\""},
        {"content-md5", "h/Fps4ugBcqAcVVmEmMG/w=="},
        {"x-ms-request-id", "f7022735-4acd-49b1-ae93-de9389874274"},
        {"x-ms-version", "2020-06-12"},
        {"date", "Mon, 12 Jul 2021 18:18:21 GMT"},
        {"accept-ranges", "bytes"},
        {"x-ms-server-encrypted", "true"},
        {"x-ms-access-tier", "Hot"},
        {"x-ms-access-tier-inferred", "true"},
        {"x-ms-access-tier-change-time", "Mon, 12 Jul 2021 18:18:21 GMT"},
        {"connection", "keep-alive"},
        {"keep-alive", "timeout=5"}
      ]

      assert headers |> BlobProperties.deserialise()
    end
  end
end
