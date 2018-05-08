defmodule Microsoft.Azure.Storage.BlobStorage do
  import SweetXml
  alias Microsoft.Azure.Storage.ApiVersion.Models.BlobStorageSignedFields, as: SignedData
  alias Microsoft.Azure.Storage.BlobClient
  alias Microsoft.Azure.Storage.DateTimeUtils

  defp hdr(headers, name), do: "#{name}:" <> headers[name]

  def list_containers(accountname, accountkey) do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/authentication-for-the-azure-storage-services

    cloudEnvironmentSuffix = "core.windows.net"
    host = "#{accountname}.blob.#{cloudEnvironmentSuffix}"
    resourcePath = "/"
    query = %{comp: "list"} |> URI.encode_query()
    uri = "https://#{host}#{resourcePath}?#{query}" |> URI.parse()

    headers = %{
      "x-ms-date" => DateTimeUtils.utc_now(),
      "x-ms-version" => "2015-04-05"
    }

    signature =
      SignedData.new()
      |> Map.put(:verb, "GET")
      |> Map.put(:canonicalizedHeaders, hdr(headers, "x-ms-date") <> "\n" <> hdr(headers, "x-ms-version"))
      |> Map.put(:canonicalizedResource, "/#{accountname}#{uri.path}\n" <>
          (uri.query
           |> URI.decode_query()
           |> Enum.sort_by(& &1)
           |> Enum.map_join("\n", fn {k, v} -> "#{k}:#{v}" end))
      )
      |> SignedData.sign(accountkey)

    client =
      BlobClient.new(
        uri |> (fn x -> "#{x.scheme}://#{x.host}:#{x.port}#{x.path}" end).(),
        headers |> Map.put("Authorization", "SharedKey #{accountname}:#{signature}")
      )

    %{status: 200, body: bodyXml} = client |> BlobClient.get("?#{uri.query}")

    {:ok,
     bodyXml
     |> xmap(
       containers: [
         ~x"/EnumerationResults/Containers/Container"l,
         name: ~x"./Name/text()"s,
         properties: [
           ~x"./Properties",
           lastModified: ~x"./Last-Modified/text()"s,
           eTag: ~x"./Etag/text()"s,
           leaseStatus: ~x"./LeaseStatus/text()"s,
           leaseState: ~x"./LeaseState/text()"s
         ]
       ]
     )}
  end
end
