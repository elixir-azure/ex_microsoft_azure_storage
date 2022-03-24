defmodule ExMicrosoftAzureStorage.Storage.SharedAccessSignatureTest do
  @moduledoc false

  use ExUnit.Case, async: true

  @moduletag :external

  import ExMicrosoftAzureStorage.Factory

  alias ExMicrosoftAzureStorage.Storage.SharedAccessSignature, as: SAS
  alias ExMicrosoftAzureStorage.Storage.{Blob, Container}

  @blob_name "my_blob"
  @blob_data "blob_data"

  setup_all do
    storage_context = build(:storage_context)
    container_context = storage_context |> Container.new("sas-test")

    {:ok, _response} = Container.ensure_container(container_context)

    blob = container_context |> Blob.new(@blob_name)

    {:ok, %{status: 201}} =
      blob
      |> Blob.put_blob(@blob_data)

    %{storage_context: storage_context, container_context: container_context, blob: blob}
  end

  describe "encode" do
    test "encodes values" do
      value = build(:value)
      now = DateTime.utc_now()

      assert {"sv", value} == SAS.encode({:service_version, value})
      assert {"st", SAS.as_time(now)} == SAS.encode({:start_time, now})
      assert {"se", SAS.as_time(now)} == SAS.encode({:expiry_time, now})
      assert {"cr", value} == SAS.encode({:canonicalized_resource, value})

      assert {"sr", "b"} == SAS.encode({:resource, [:blob]})
      assert {"sr", "c"} == SAS.encode({:resource, [:container]})
      assert {"sr", "s"} == SAS.encode({:resource, [:share]})
      assert {"sr", "f"} == SAS.encode({:resource, [:file]})

      assert {"sip", value} == SAS.encode({:ip_range, value})
      assert {"spr", value} == SAS.encode({:protocol, value})

      assert {"ss", "b"} == SAS.encode({:services, [:blob]})
      assert {"ss", "q"} == SAS.encode({:services, [:queue]})
      assert {"ss", "t"} == SAS.encode({:services, [:table]})
      assert {"ss", "f"} == SAS.encode({:services, [:file]})

      assert {"srt", "s"} == SAS.encode({:resource_type, [:service]})
      assert {"srt", "o"} == SAS.encode({:resource_type, [:object]})
      assert {"srt", "c"} == SAS.encode({:resource_type, [:container]})

      assert {"sp", "r"} == SAS.encode({:permissions, [:read]})
      assert {"sp", "w"} == SAS.encode({:permissions, [:write]})
      assert {"sp", "d"} == SAS.encode({:permissions, [:delete]})
      assert {"sp", "l"} == SAS.encode({:permissions, [:list]})
      assert {"sp", "a"} == SAS.encode({:permissions, [:add]})
      assert {"sp", "c"} == SAS.encode({:permissions, [:create]})
      assert {"sp", "u"} == SAS.encode({:permissions, [:update]})
      assert {"sp", "p"} == SAS.encode({:permissions, [:process]})

      assert {"rscc", value} == SAS.encode({:cache_control, value})
      assert {"rscd", value} == SAS.encode({:content_disposition, value})
      assert {"rsce", value} == SAS.encode({:content_encoding, value})
      assert {"rscl", value} == SAS.encode({:content_language, value})
      assert {"rsct", value} == SAS.encode({:content_type, value})

      assert {nil, nil} == SAS.encode({:not, "a value"})
    end
  end
end
