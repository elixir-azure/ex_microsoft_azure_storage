defmodule Microsoft.Azure.Storage.BlobStorage do
  import SweetXml

  alias Microsoft.Azure.Storage.ApiVersion.Models.BlobStorageSignedFields, as: SignedData
  alias Microsoft.Azure.Storage.BlobClient
  alias Microsoft.Azure.Storage.DateTimeUtils
  alias Microsoft.Azure.Storage.AzureStorageContext

  def create_container(context = %AzureStorageContext{}, container_name) do
    container_name = container_name |> String.downcase()

    host = context |> AzureStorageContext.blob_endpoint()
    resourcePath = "/#{container_name}/"
    query = %{restype: "container"} |> URI.encode_query()
    uri = "https://#{host}#{resourcePath}?#{query}" |> URI.parse()
    base_uri = "#{uri.scheme}://#{uri.host}:#{uri.port}"
    request_path = "#{uri.path}?#{uri.query}"

    headers = %{
      "x-ms-date" => DateTimeUtils.utc_now(),
      "x-ms-version" => "2015-04-05"
    }

    hdr = fn headers, name -> "#{name}:" <> headers[name] end

    signature =
      SignedData.new()
      |> Map.put(:verb, "PUT")
      |> Map.put(
        :canonicalizedHeaders,
        hdr.(headers, "x-ms-date") <> "\n" <> hdr.(headers, "x-ms-version")
      )
      |> Map.put(
        :canonicalizedResource,
        "/#{context.account_name}#{uri.path}\n" <>
          (uri.query
           |> URI.decode_query()
           |> Enum.sort_by(& &1)
           |> Enum.map_join("\n", fn {k, v} -> "#{k}:#{v}" end))
      )
      |> SignedData.sign(context.account_key)

    client =
      BlobClient.new(
        base_uri,
        headers |> Map.put("Authorization", "SharedKey #{context.account_name}:#{signature}")
      )

    %{
      status: 201,
      url: url,
      headers: %{
        "etag" => etag,
        "last-modified" => last_modified,
        "x-ms-request-id" => request_id
      }
    } = client |> BlobClient.put(request_path, "")

    {:ok,
     %{
       url: url,
       etag: etag,
       last_modified: last_modified,
       request_id: request_id
     }}
  end

  def list_containers(context = %AzureStorageContext{}) do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/authentication-for-the-azure-storage-services

    host = context |> AzureStorageContext.blob_endpoint()
    resourcePath = "/"
    query = %{comp: "list"} |> URI.encode_query()
    uri = "https://#{host}#{resourcePath}?#{query}" |> URI.parse()
    base_uri = "#{uri.scheme}://#{uri.host}:#{uri.port}"
    request_path = "#{uri.path}?#{uri.query}"

    headers = %{
      "x-ms-date" => DateTimeUtils.utc_now(),
      "x-ms-version" => "2015-04-05"
    }

    hdr = fn headers, name -> "#{name}:" <> headers[name] end

    signature =
      SignedData.new()
      |> Map.put(:verb, "GET")
      |> Map.put(
        :canonicalizedHeaders,
        hdr.(headers, "x-ms-date") <> "\n" <> hdr.(headers, "x-ms-version")
      )
      |> Map.put(
        :canonicalizedResource,
        "/#{context.account_name}#{uri.path}\n" <>
          (uri.query
           |> URI.decode_query()
           |> Enum.sort_by(& &1)
           |> Enum.map_join("\n", fn {k, v} -> "#{k}:#{v}" end))
      )
      |> SignedData.sign(context.account_key)

    client =
      BlobClient.new(
        base_uri,
        headers |> Map.put("Authorization", "SharedKey #{context.account_name}:#{signature}")
      )

    %{status: 200, body: bodyXml} = client |> BlobClient.get(request_path)

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

  def get_container_properties(context = %AzureStorageContext{}, container_name) do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/get-container-properties

    host = context |> AzureStorageContext.blob_endpoint()
    resourcePath = "/#{container_name}"
    query = %{restype: "container"} |> URI.encode_query()
    uri = "https://#{host}#{resourcePath}?#{query}" |> URI.parse()
    base_uri = "#{uri.scheme}://#{uri.host}:#{uri.port}"
    request_path = "#{uri.path}?#{uri.query}"

    headers = %{
      "x-ms-date" => DateTimeUtils.utc_now(),
      "x-ms-version" => "2015-04-05"
    }

    hdr = fn headers, name -> "#{name}:" <> headers[name] end

    signature =
      SignedData.new()
      |> Map.put(:verb, "GET")
      |> Map.put(
        :canonicalizedHeaders,
        hdr.(headers, "x-ms-date") <> "\n" <> hdr.(headers, "x-ms-version")
      )
      |> Map.put(
        :canonicalizedResource,
        "/#{context.account_name}#{uri.path}\n" <>
          (uri.query
           |> URI.decode_query()
           |> Enum.sort_by(& &1)
           |> Enum.map_join("\n", fn {k, v} -> "#{k}:#{v}" end))
      )
      |> SignedData.sign(context.account_key)

    client =
      BlobClient.new(
        base_uri,
        headers |> Map.put("Authorization", "SharedKey #{context.account_name}:#{signature}")
      )

    %{
      status: 200,
      url: url,
      headers: %{
        "etag" => etag,
        "last-modified" => last_modified,
        "x-ms-lease-state" => lease_state,
        "x-ms-lease-status" => lease_status,
        "x-ms-request-id" => request_id
      }
    } = client |> BlobClient.get(request_path)

    {:ok,
     %{
       url: url,
       etag: etag,
       last_modified: last_modified,
       lease_state: lease_state,
       lease_status: lease_status,
       request_id: request_id
     }}
  end

  def list_blobs(context = %AzureStorageContext{}, container_name, opts \\ []) do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/list-blobs

    host = context |> AzureStorageContext.blob_endpoint()
    resourcePath = "/#{container_name}"

    query =
      [comp: "list", restype: "container"]
      |> Keyword.merge(opts)
      |> Map.new()
      |> URI.encode_query()

    uri = "https://#{host}#{resourcePath}?#{query}" |> URI.parse()
    base_uri = "#{uri.scheme}://#{uri.host}:#{uri.port}"
    request_path = "#{uri.path}?#{uri.query}"

    headers = %{
      "x-ms-date" => DateTimeUtils.utc_now(),
      "x-ms-version" => "2015-04-05"
    }

    hdr = fn headers, name -> "#{name}:" <> headers[name] end

    signature =
      SignedData.new()
      |> Map.put(:verb, "GET")
      |> Map.put(
        :canonicalizedHeaders,
        hdr.(headers, "x-ms-date") <> "\n" <> hdr.(headers, "x-ms-version")
      )
      |> Map.put(
        :canonicalizedResource,
        "/#{context.account_name}#{uri.path}\n" <>
          (uri.query
           |> URI.decode_query()
           |> Enum.sort_by(& &1)
           |> Enum.map_join("\n", fn {k, v} -> "#{k}:#{v}" end))
      )
      |> SignedData.sign(context.account_key)

    client =
      BlobClient.new(
        base_uri,
        headers |> Map.put("Authorization", "SharedKey #{context.account_name}:#{signature}")
      )

    %{
      status: 200,
      body: bodyXml,
      url: url,
      headers: %{
        "x-ms-request-id" => request_id
      }
    } = client |> BlobClient.get(request_path) |> IO.inspect()

    {:ok,
     bodyXml
     |> xmap(
       max_results: ~x"/EnumerationResults/MaxResults/text()"i,
       next_marker: ~x"/EnumerationResults/NextMarker/text()"s,
       blobs: [
         ~x"/EnumerationResults/Blobs/Blob"l,
         name: ~x"./Name/text()"s,
         properties: [
           ~x"./Properties",
           last_modified: ~x"./Last-Modified/text()"s,
           etag: ~x"./Etag/text()"s,
           content_length: ~x"./Content-Length/text()"i,
           content_type: ~x"./Content-Type/text()"s,
           content_encoding: ~x"./Content-Encoding/text()"s,
           content_language: ~x"./Content-Language/text()"s,
           content_md5: ~x"./Content-MD5/text()"s,
           cache_control: ~x"./Cache-Control/text()"s,
           content_disposition: ~x"./Content-Disposition/text()"s,
           blob_type: ~x"./BlobType/text()"s,
           lease_status: ~x"./LeaseStatus/text()"s,
           lease_state: ~x"./LeaseState/text()"s
         ]
       ]
     )
     |> Map.put(:url, url)
     |> Map.put(:request_id, request_id)}
  end
end
