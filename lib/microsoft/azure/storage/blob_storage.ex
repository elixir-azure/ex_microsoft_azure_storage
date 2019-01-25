defmodule Microsoft.Azure.Storage.BlobStorage do
  use NamedArgs
  import SweetXml
  import Microsoft.Azure.Storage.RequestBuilder
  alias Microsoft.Azure.Storage.{DateTimeUtils, BlobPolicy, AzureStorageContext}
  alias Microsoft.Azure.Storage.AzureStorageContext.Container, as: Container

  def list_containers(context = %AzureStorageContext{}) do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/list-containers2
    response =
      new_azure_storage_request()
      |> method(:get)
      |> url("/")
      |> add_param(:query, :comp, "list")
      |> add_ms_context(context, DateTimeUtils.utc_now(), :storage)
      |> sign_and_call(:blob_service)

    case response do
      %{status: status} when 400 <= status and status < 500 ->
        response |> create_error_response()

      %{status: 200} ->
        {:ok,
         response.body
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
         |> Map.put(:headers, response.headers)
         |> Map.put(:url, response.url)
         |> Map.put(:status, response.status)
         |> Map.put(:request_id, response.headers["x-ms-request-id"])}
    end
  end

  defp to_bool("true"), do: true
  defp to_bool("false"), do: false
  defp to_bool(_), do: false

  def get_blob_service_stats(context = %AzureStorageContext{}) do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/get-blob-service-stats
    response =
      new_azure_storage_request()
      |> method(:get)
      |> url("/")
      |> add_param(:query, :restype, "service")
      |> add_param(:query, :comp, "stats")
      |> add_ms_context(
        context |> AzureStorageContext.secondary(),
        DateTimeUtils.utc_now(),
        :storage
      )
      |> sign_and_call(:blob_service)

    case response do
      %{status: status} when 400 <= status and status < 500 ->
        response |> create_error_response()

      %{status: 200} ->
        {:ok,
         response.body
         |> xmap(
           geo_replication: [
             ~x"/StorageServiceStats/GeoReplication",
             status: ~x"./Status/text()"s,
             last_sync_time: ~x"./LastSyncTime/text()"s
           ]
         )
         |> Map.put(:headers, response.headers)
         |> Map.put(:url, response.url)
         |> Map.put(:status, response.status)
         |> Map.put(:request_id, response.headers["x-ms-request-id"])}
    end
  end

  def get_blob_service_properties(context = %AzureStorageContext{}) do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/get-blob-service-properties
    response =
      new_azure_storage_request()
      |> method(:get)
      |> url("/")
      |> add_param(:query, :restype, "service")
      |> add_param(:query, :comp, "properties")
      |> add_ms_context(
        context,
        DateTimeUtils.utc_now(),
        :storage
      )
      |> sign_and_call(:blob_service)

    case response do
      %{status: status} when 400 <= status and status < 500 ->
        response |> create_error_response()

      %{status: 200} ->
        {:ok,
         response.body
         |> xmap(
           logging: [
             ~x"/StorageServiceProperties/Logging",
             version: ~x"./Version/text()"s,
             delete: ~x"./Delete/text()"s |> transform_by(&to_bool/1),
             read: ~x"./Read/text()"s |> transform_by(&to_bool/1),
             write: ~x"./Write/text()"s |> transform_by(&to_bool/1),
             retention_policy: [
               ~x"./RetentionPolicy",
               enabled: ~x"./Enabled/text()"s |> transform_by(&to_bool/1),
               days: ~x"./Days/text()"I
             ]
           ],
           hour_metrics: [
             ~x"/StorageServiceProperties/HourMetrics",
             version: ~x"./Version/text()"s,
             enabled: ~x"./Enabled/text()"s |> transform_by(&to_bool/1),
             include_apis: ~x"./IncludeAPIs/text()"s |> transform_by(&to_bool/1),
             retention_policy: [
               ~x"./RetentionPolicy",
               enabled: ~x"./Enabled/text()"s |> transform_by(&to_bool/1),
               days: ~x"./Days/text()"I
             ]
           ],
           minute_metrics: [
             ~x"/StorageServiceProperties/MinuteMetrics",
             version: ~x"./Version/text()"s,
             enabled: ~x"./Enabled/text()"s |> transform_by(&to_bool/1),
             include_apis: ~x"./IncludeAPIs/text()"s |> transform_by(&to_bool/1),
             retention_policy: [
               ~x"./RetentionPolicy",
               enabled: ~x"./Enabled/text()"s |> transform_by(&to_bool/1),
               days: ~x"./Days/text()"I
             ]
           ],
           cors_rules: [
             ~x"/StorageServiceProperties/Cors/CorsRule"l,
             max_age_in_seconds: ~x"./MaxAgeInSeconds/text()"I,
             allowed_origins:
               ~x"./AllowedOrigins/text()"s |> transform_by(&(&1 |> String.split(","))),
             allowed_methods:
               ~x"./AllowedMethods/text()"s |> transform_by(&(&1 |> String.split(","))),
             exposed_headers:
               ~x"./ExposedHeaders/text()"s |> transform_by(&(&1 |> String.split(","))),
             allowed_headers:
               ~x"./AllowedHeaders/text()"s |> transform_by(&(&1 |> String.split(",")))
           ],
           default_service_version: ~x"/StorageServiceProperties/DefaultServiceVersion/text()"s,
           delete_retention_policy: [
             ~x"/StorageServiceProperties/DeleteRetentionPolicy",
             enabled: ~x"./Enabled/text()"s |> transform_by(&to_bool/1),
             days: ~x"./Days/text()"I
           ]
         )
         |> Map.put(:headers, response.headers)
         |> Map.put(:url, response.url)
         |> Map.put(:status, response.status)
         |> Map.put(:request_id, response.headers["x-ms-request-id"])}
    end
  end

  def set_blob_service_properties(context = %AzureStorageContext{}, service_properties) do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/set-blob-service-properties
    response =
      new_azure_storage_request()
      |> method(:put)
      |> url("/")
      |> add_param(:query, :restype, "service")
      |> add_param(:query, :comp, "properties")
      |> add_header("Content-Type", "application/xml")
      |> body(
        service_properties
        |> Microsoft.Azure.Storage.Serialization.BlobServiceProperties.xml_blob_service_properties()
        |> XmlBuilder.generate(format: :none)
      )
      |> add_ms_context(context, DateTimeUtils.utc_now(), :storage)
      |> sign_and_call(:blob_service)

    response
  end

  def create_container(%Container{storage_context: context, container_name: container_name}) do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/create-container
    response =
      new_azure_storage_request()
      |> method(:put)
      |> url("/#{container_name}")
      |> add_param(:query, :restype, "container")
      # |> body(nil)
      |> add_ms_context(context, DateTimeUtils.utc_now(), :storage)
      |> sign_and_call(:blob_service)

    case response do
      %{status: status} when 400 <= status and status < 500 ->
        response |> create_error_response()

      %{status: 201} ->
        {:ok,
         %{
           headers: response.headers,
           url: response.url,
           status: response.status,
           request_id: response.headers["x-ms-request-id"],
           etag: response.headers["etag"],
           last_modified: response.headers["last-modified"]
         }}
    end
  end

  def get_container_properties(%Container{
        storage_context: context,
        container_name: container_name
      }) do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/get-container-properties
    response =
      new_azure_storage_request()
      |> method(:get)
      |> url("/#{container_name}")
      |> add_param(:query, :restype, "container")
      |> add_ms_context(context, DateTimeUtils.utc_now(), :storage)
      |> sign_and_call(:blob_service)

    case response do
      %{status: status} when 400 <= status and status < 500 ->
        response |> create_error_response()

      %{status: 200} ->
        {:ok,
         %{
           headers: response.headers,
           url: response.url,
           status: response.status,
           request_id: response.headers["x-ms-request-id"],
           etag: response.headers["etag"],
           last_modified: response.headers["last-modified"],
           lease_state: response.headers["x-ms-lease-state"],
           lease_status: response.headers["x-ms-lease-status"]
         }}
    end
  end

  def get_container_metadata(%Container{storage_context: context, container_name: container_name}) do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/get-container-metadata

    response =
      new_azure_storage_request()
      |> method(:head)
      |> url("/#{container_name}")
      |> add_param(:query, :restype, "container")
      |> add_param(:query, :comp, "metadata")
      |> add_ms_context(context, DateTimeUtils.utc_now(), :storage)
      |> sign_and_call(:blob_service)

    case response do
      %{status: status} when 400 <= status and status < 500 ->
        response |> create_error_response()

      %{status: 200} ->
        {:ok,
         %{
           headers: response.headers,
           url: response.url,
           status: response.status,
           request_id: response.headers["x-ms-request-id"],
           etag: response.headers["etag"],
           last_modified: response.headers["last-modified"],
           lease_state: response.headers["x-ms-lease-state"],
           lease_status: response.headers["x-ms-lease-status"]
         }}
    end
  end

  def get_container_acl(%Container{storage_context: context, container_name: container_name}) do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/get-container-acl

    response =
      new_azure_storage_request()
      |> method(:get)
      |> url("/#{container_name}")
      |> add_param(:query, :restype, "container")
      |> add_param(:query, :comp, "acl")
      |> add_ms_context(context, DateTimeUtils.utc_now(), :storage)
      |> sign_and_call(:blob_service)

    case response do
      %{status: status} when 400 <= status and status < 500 ->
        response |> create_error_response()

      %{status: 200} ->
        {:ok,
         %{
           headers: response.headers,
           url: response.url,
           status: response.status,
           request_id: response.headers["x-ms-request-id"],
           etag: response.headers["etag"],
           last_modified: response.headers["last-modified"],
           blob_public_access: response.headers["x-ms-blob-public-access"],
           body: response.body,
           policies: response.body |> process_body([], &BlobPolicy.deserialize/1)
         }}
    end
  end

  def set_container_acl_public_access_off(container = %Container{}),
    do: container |> set_container_acl(:off)

  def set_container_acl_public_access_blob(container = %Container{}),
    do: container |> set_container_acl(:blob)

  def set_container_acl_public_access_container(container = %Container{}),
    do: container |> set_container_acl(:container)

  def set_container_acl(
        %Container{storage_context: context, container_name: container_name},
        access_level
      )
      when access_level |> is_atom() and access_level in [:off, :blob, :container] do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/set-container-acl#remarks

    response =
      new_azure_storage_request()
      |> method(:put)
      |> url("/#{container_name}")
      |> add_param(:query, :restype, "container")
      |> add_param(:query, :comp, "acl")
      |> (fn r ->
            case access_level do
              :off -> r
              :blob -> r |> add_header("x-ms-blob-public-access", "blob")
              :container -> r |> add_header("x-ms-blob-public-access", "container")
            end
          end).()
      |> add_ms_context(context, DateTimeUtils.utc_now(), :storage)
      |> sign_and_call(:blob_service)

    case response do
      %{status: status} when 400 <= status and status < 500 ->
        response |> create_error_response()

      %{status: 200} ->
        {:ok,
         %{
           headers: response.headers,
           url: response.url,
           status: response.status,
           request_id: response.headers["x-ms-request-id"],
           etag: response.headers["etag"],
           last_modified: response.headers["last-modified"],
           body: response.body
         }}
    end
  end

  def set_container_acl(
        %Container{storage_context: context, container_name: container_name},
        access_policies
      )
      when access_policies |> is_list() do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/set-container-acl#remarks

    response =
      new_azure_storage_request()
      |> method(:put)
      |> url("/#{container_name}")
      |> add_param(:query, :restype, "container")
      |> add_param(:query, :comp, "acl")
      |> add_header("Content-Type", "application/xml")
      |> body(access_policies |> BlobPolicy.serialize())
      |> add_ms_context(context, DateTimeUtils.utc_now(), :storage)
      |> sign_and_call(:blob_service)

    case response do
      %{status: status} when 400 <= status and status < 500 ->
        response |> create_error_response()

      %{status: 200} ->
        {:ok,
         %{
           headers: response.headers,
           url: response.url,
           status: response.status,
           request_id: response.headers["x-ms-request-id"],
           etag: response.headers["etag"],
           last_modified: response.headers["last-modified"],
           body: response.body
         }}
    end
  end

  def delete_container(%Container{storage_context: context, container_name: container_name}) do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/delete-container
    response =
      new_azure_storage_request()
      |> method(:delete)
      |> url("/#{container_name}")
      |> add_param(:query, :restype, "container")
      |> add_ms_context(context, DateTimeUtils.utc_now(), :storage)
      |> sign_and_call(:blob_service)

    case response do
      %{status: status} when 400 <= status and status < 500 ->
        response |> create_error_response()

      %{status: 202} ->
        {:ok,
         %{
           headers: response.headers,
           url: response.url,
           status: response.status,
           request_id: response.headers["x-ms-request-id"]
         }}
    end
  end

  def list_blobs(
        %Container{storage_context: context, container_name: container_name},
        opts \\ [
          prefix: nil,
          delimiter: nil,
          marker: nil,
          maxresults: nil,
          timeout: nil,
          include: nil
        ]
      ) do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/list-blobs

    response =
      new_azure_storage_request()
      |> method(:get)
      |> url("/#{container_name}")
      |> add_param(:query, :comp, "list")
      |> add_param(:query, :restype, "container")
      |> add_param(:query, opts)
      |> add_ms_context(context, DateTimeUtils.utc_now(), :storage)
      |> sign_and_call(:blob_service)

    case response do
      %{status: status} when 400 <= status and status < 500 ->
        response |> create_error_response()

      %{status: 200} ->
        {:ok,
         response.body
         |> xmap(
           max_results: ~x"/EnumerationResults/MaxResults/text()"s,
           next_marker: ~x"/EnumerationResults/NextMarker/text()"s,
           blobs: [
             ~x"/EnumerationResults/Blobs/Blob"l,
             name: ~x"./Name/text()"s,
             properties: [
               ~x"./Properties",
               etag: ~x"./Etag/text()"s,
               last_modified: ~x"./Last-Modified/text()"s,
               content_length: ~x"./Content-Length/text()"i,
               content_type: ~x"./Content-Type/text()"s,
               content_encoding: ~x"./Content-Encoding/text()"s,
               content_language: ~x"./Content-Language/text()"s,
               content_md5: ~x"./Content-MD5/text()"s,
               content_disposition: ~x"./Content-Disposition/text()"s,
               cache_control: ~x"./Cache-Control/text()"s,
               blob_type: ~x"./BlobType/text()"s,
               lease_status: ~x"./LeaseStatus/text()"s,
               lease_state: ~x"./LeaseState/text()"s
             ]
           ]
         )
         |> Map.put(:headers, response.headers)
         |> Map.put(:url, response.url)
         |> Map.put(:status, response.status)
         |> Map.put(:request_id, response.headers["x-ms-request-id"])}
    end
  end

  defp process_body(nil, default_value, _process_fn), do: default_value
  defp process_body("", default_value, _process_fn), do: default_value
  defp process_body(body, _default_value, process_fn), do: body |> process_fn.()
end
