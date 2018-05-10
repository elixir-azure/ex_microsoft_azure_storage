defmodule Microsoft.Azure.Storage.BlobStorage do
  import Microsoft.Azure.Storage.RequestBuilder
  import SweetXml
  use NamedArgs

  alias Microsoft.Azure.Storage.DateTimeUtils
  alias Microsoft.Azure.Storage.AzureStorageContext

  @storage_api_version "2015-04-05"

  def create_container(context = %AzureStorageContext{}, container_name) do
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
      |> method(:put)
      |> url("/#{container_name |> String.downcase()}")
      |> add_param(:query, :restype, "container")
      |> body("")
      |> add_ms_context(context, DateTimeUtils.utc_now(), @storage_api_version)
      |> sign_and_call(:blob_service)

    {:ok,
     %{
       url: url,
       etag: etag,
       last_modified: last_modified,
       request_id: request_id
     }}
  end

  def list_containers(context = %AzureStorageContext{}) do
    %{status: 200, body: bodyXml} =
      %{}
      |> method(:get)
      |> url("/")
      |> add_param(:query, :comp, "list")
      |> add_ms_context(context, DateTimeUtils.utc_now(), @storage_api_version)
      |> sign_and_call(:blob_service)

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
    } =
      %{}
      |> method(:get)
      |> url("/#{container_name}")
      |> add_param(:query, :restype, "container")
      |> add_ms_context(context, DateTimeUtils.utc_now(), @storage_api_version)
      |> sign_and_call(:blob_service)

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

    # query =
    #   opts
    #   |> Keyword.merge(main_query)
    #   |> Enum.filter(fn {_, value} -> value != nil && value != "" end)
    #   |> Map.new()
    #   |> URI.encode_query()

    %{
      status: 200,
      body: bodyXml,
      url: url,
      headers: %{
        "x-ms-request-id" => request_id
      }
    } =
      %{}
      |> method(:get)
      |> url("/#{container_name}")
      |> add_param(:query, :comp, "list")
      |> add_param(:query, :restype, "container")
      |> add_ms_context(context, DateTimeUtils.utc_now(), @storage_api_version)
      |> sign_and_call(:blob_service)

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
