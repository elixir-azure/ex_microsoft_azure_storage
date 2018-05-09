defmodule Microsoft.Azure.Storage.BlobStorage do
  import SweetXml
  use NamedArgs

  alias Microsoft.Azure.Storage.Models.BlobStorageSignedFields, as: SignedData
  alias Microsoft.Azure.Storage.RestClient
  alias Microsoft.Azure.Storage.DateTimeUtils
  alias Microsoft.Azure.Storage.AzureStorageContext
  alias Microsoft.Azure.Storage.RequestBuilder

  @storage_api_version "2015-04-05"

  def create_container(context = %AzureStorageContext{}, container_name) do
    connection =
      context
      |> AzureStorageContext.blob_endpoint_url()
      |> RestClient.new()

    %{
      status: 201,
      url: url,
      headers: %{
        "etag" => etag,
        "last-modified" => last_modified,
        "x-ms-request-id" => request_id
      }
    } =
      %{}
      |> RequestBuilder.method(:put)
      |> RequestBuilder.url("/#{container_name |> String.downcase()}")
      |> RequestBuilder.add_storage_context(context)
      |> RequestBuilder.add_param(:query, :restype, "container")
      |> RequestBuilder.add_header("x-ms-date", DateTimeUtils.utc_now())
      |> RequestBuilder.add_header("x-ms-version", @storage_api_version)
      |> RequestBuilder.body("")
      |> RequestBuilder.add_signature()
      |> Enum.into([])
      |> (&RestClient.request(connection, &1)).()

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
      "x-ms-version" => @storage_api_version
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
      RestClient.new(
        base_uri,
        headers |> Map.put("Authorization", "SharedKey #{context.account_name}:#{signature}")
      )

    %{status: 200, body: bodyXml} = client |> RestClient.get(request_path)

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
      "x-ms-version" => @storage_api_version
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
      RestClient.new(
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
    } = client |> RestClient.get(request_path)

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

  def list_blobs(
        context = %AzureStorageContext{},
        container_name,
        opts \\ [
          prefix: nil,
          delimiter: nil,
          marker: nil,
          maxresults: nil,
          timeout: nil,
          include: "snapshots"
        ]
      ) do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/list-blobs

    host = context |> AzureStorageContext.blob_endpoint()
    resourcePath = "/#{container_name}"
    main_query = [comp: "list", restype: "container"]

    query =
      opts
      |> Keyword.merge(main_query)
      |> Enum.filter(fn {_, value} -> value != nil && value != "" end)
      |> Map.new()
      |> URI.encode_query()
      |> IO.inspect()

    uri = "https://#{host}#{resourcePath}?#{query}" |> URI.parse()
    base_uri = "#{uri.scheme}://#{uri.host}:#{uri.port}"
    request_path = "#{uri.path}?#{uri.query}"

    headers = %{
      "x-ms-date" => DateTimeUtils.utc_now(),
      "x-ms-version" => @storage_api_version
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
      RestClient.new(
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
    } = client |> RestClient.get(request_path) |> IO.inspect()

    {:ok,
     bodyXml
     |> xmap(
       max_results: ~x"/EnumerationResults/MaxResults/text()"s,
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
