defmodule ExMicrosoftAzureStorage do
  import SweetXml
  alias Microsoft.Azure.Storage.ApiVersion.Models.BlobStorageSignedFields

  defmodule BlobClient do
    use Tesla

    adapter(:ibrowse)

    def new(base_url, headers) do
      Tesla.build_client([
        {Tesla.Middleware.BaseUrl, base_url},
        {Tesla.Middleware.Headers, headers},
        {Tesla.Middleware.Opts, [proxy_host: '127.0.0.1', proxy_port: 8888]}
      ])
    end
  end

  def hello do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/authentication-for-the-azure-storage-services
    accountkey = "SAMPLE_STORAGE_ACCOUNT_KEY" |> System.get_env()
    accountname = "SAMPLE_STORAGE_ACCOUNT_NAME" |> System.get_env()
    cloudEnvironmentSuffix = "core.windows.net"

    host = "#{accountname}.blob.#{cloudEnvironmentSuffix}"
    resourcePath = "/"
    query = %{comp: "list"} |> URI.encode_query()
    url = "https://#{host}#{resourcePath}?#{query}"
    xMsDate = DateTime.utc_now() |> Microsoft.Azure.Storage.DateTimeUtils.datetime_to_string()
    xMsVersion = "2015-04-05"

    uri = url |> URI.parse()

    signature =
      BlobStorageSignedFields.new()
      |> Map.put(:verb, "GET")
      |> Map.put(:canonicalizedHeaders, "x-ms-date:#{xMsDate}\nx-ms-version:#{xMsVersion}")
      |> Map.put(
        :canonicalizedResource,
        "/#{accountname}#{uri.path}\n" <>
          (uri.query
           |> URI.decode_query()
           |> Enum.sort_by(& &1)
           |> Enum.map_join("\n", fn {k, v} -> "#{k}:#{v}" end))
      )
      |> BlobStorageSignedFields.sign(accountkey)

    client =
      BlobClient.new(uri |> (fn x -> "#{x.scheme}://#{x.host}:#{x.port}#{x.path}" end).(), %{
        "x-ms-date" => xMsDate,
        "x-ms-version" => xMsVersion,
        "Authorization" => "SharedKey #{accountname}:#{signature}"
      })

    %{status: 200, body: bodyXml} =
      client
      |> BlobClient.get("?#{uri.query}")

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
    )
  end
end
