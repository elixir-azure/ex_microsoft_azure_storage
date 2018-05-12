defmodule Microsoft.Azure.Storage.BlobStorage do
  import Microsoft.Azure.Storage.RequestBuilder
  import SweetXml
  use NamedArgs
  alias Microsoft.Azure.Storage.DateTimeUtils
  alias Microsoft.Azure.Storage.BlobPolicy
  alias Microsoft.Azure.Storage.AzureStorageContext

  @storage_api_version "2015-04-05"

  def list_containers(context = %AzureStorageContext{}) do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/list-containers2
    response =
      new_azure_storage_request()
      |> method(:get)
      |> url("/")
      |> add_param(:query, :comp, "list")
      |> add_ms_context(context, DateTimeUtils.utc_now(), @storage_api_version)
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
        @storage_api_version
      )
      |> sign_and_call(:blob_service)

    case response do
      %{status: status} when 400 <= status and status < 500 ->
        response |> create_error_response()

      %{status: 200} ->
        {:ok,
         response.body
         |> xmap(
           status: ~x"/StorageServiceStats/GeoReplication/Status/text()"s,
           last_sync_time: ~x"/StorageServiceStats/GeoReplication/LastSyncTime/text()"s
         )
         |> Map.put(:headers, response.headers)
         |> Map.put(:url, response.url)
         |> Map.put(:status, response.status)
         |> Map.put(:request_id, response.headers["x-ms-request-id"])}
    end
  end

  def create_container(context = %AzureStorageContext{}, container_name) do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/create-container
    response =
      new_azure_storage_request()
      |> method(:put)
      |> url("/#{container_name}")
      |> add_param(:query, :restype, "container")
      # |> body(nil)
      |> add_ms_context(context, DateTimeUtils.utc_now(), @storage_api_version)
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

  def get_container_properties(context = %AzureStorageContext{}, container_name) do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/get-container-properties
    response =
      new_azure_storage_request()
      |> method(:get)
      |> url("/#{container_name}")
      |> add_param(:query, :restype, "container")
      |> add_ms_context(context, DateTimeUtils.utc_now(), @storage_api_version)
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

  def get_container_metadata(context = %AzureStorageContext{}, container_name) do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/get-container-metadata

    response =
      new_azure_storage_request()
      |> method(:head)
      |> url("/#{container_name}")
      |> add_param(:query, :restype, "container")
      |> add_param(:query, :comp, "metadata")
      |> add_ms_context(context, DateTimeUtils.utc_now(), @storage_api_version)
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

  def get_container_acl(context = %AzureStorageContext{}, container_name) do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/get-container-acl

    response =
      new_azure_storage_request()
      |> method(:get)
      |> url("/#{container_name}")
      |> add_param(:query, :restype, "container")
      |> add_param(:query, :comp, "acl")
      |> add_ms_context(context, DateTimeUtils.utc_now(), @storage_api_version)
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

  def set_container_acl_public_access_off(context = %AzureStorageContext{}, container_name),
    do: set_container_acl(context, container_name, :off)

  def set_container_acl_public_access_blob(context = %AzureStorageContext{}, container_name),
    do: set_container_acl(context, container_name, :blob)

  def set_container_acl_public_access_container(context = %AzureStorageContext{}, container_name),
    do: set_container_acl(context, container_name, :container)

  def set_container_acl(context = %AzureStorageContext{}, container_name, access_level)
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
      |> add_ms_context(context, DateTimeUtils.utc_now(), @storage_api_version)
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

  def set_container_acl(context = %AzureStorageContext{}, container_name, access_policies)
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
      |> add_ms_context(context, DateTimeUtils.utc_now(), @storage_api_version)
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

  def delete_container(context = %AzureStorageContext{}, container_name) do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/delete-container
    response =
      new_azure_storage_request()
      |> method(:delete)
      |> url("/#{container_name}")
      |> add_param(:query, :restype, "container")
      |> add_ms_context(context, DateTimeUtils.utc_now(), @storage_api_version)
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

  # ChangeLease string proposedLeaseId,
  # ReleaseLease
  #
  # "x-ms-lease-action" acquire/renew/change/release/break
  # "x-ms-lease-id"     Required for renew/change/release
  # "x-ms-lease-break-period"  optional 0..60
  # "x-ms-lease-duration" required for acquire. -1, 15..60
  # "x-ms-proposed-lease-id" Optional for acquire, required for change
  defp container_lease_handler(
         context = %AzureStorageContext{},
         container_name,
         expected_status_code,
         fn_prepare_request,
         fn_prepare_response
       ) do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/lease-container

    response =
      new_azure_storage_request()
      |> method(:put)
      |> url("/#{container_name}")
      |> add_param(:query, :comp, "lease")
      |> add_param(:query, :restype, "container")
      |> fn_prepare_request.()
      |> add_ms_context(context, DateTimeUtils.utc_now(), @storage_api_version)
      |> sign_and_call(:blob_service)

    case response do
      %{status: status} when 400 <= status and status < 500 ->
        response |> create_error_response()

      %{status: ^expected_status_code} ->
        {:ok,
         %{
           headers: response.headers,
           url: response.url,
           status: response.status,
           request_id: response.headers["x-ms-request-id"],
           lease_id: response.headers["x-ms-lease-id"]
         }
         |> fn_prepare_response.(response)}
    end
  end

  defp pass_result_as_is(result, _response), do: result

  # AcquireLease TimeSpan? leaseTime, string proposedLeaseId
  def container_lease_acquire(
        context = %AzureStorageContext{},
        container_name,
        lease_duration,
        proposed_lease_id \\ nil
      )
      when lease_duration |> is_integer() and (lease_duration == -1 or lease_duration in 15..60) do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/lease-container

    fn_prepare_request = fn request ->
      request
      |> add_header("x-ms-lease-action", "acquire")
      |> add_header("x-ms-lease-duration", "#{lease_duration}")
      |> add_header("x-ms-proposed-lease-id", "#{proposed_lease_id}")
    end

    context
    |> container_lease_handler(
      container_name,
      201,
      fn_prepare_request,
      &pass_result_as_is/2
    )
  end

  # RenewLease
  def container_lease_renew(
        context = %AzureStorageContext{},
        container_name,
        lease_id
      ) do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/lease-container

    fn_prepare_request = fn request ->
      request
      |> add_header("x-ms-lease-action", "renew")
      |> add_header("x-ms-lease-id", "#{lease_id}")
    end

    context
    |> container_lease_handler(
      container_name,
      200,
      fn_prepare_request,
      &pass_result_as_is/2
    )
  end

  # BreakLease   TimeSpan? breakPeriod
  def container_lease_break(
        context = %AzureStorageContext{},
        container_name,
        lease_id,
        break_period \\ -1
      )
      when break_period |> is_integer() and break_period in -1..60 do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/lease-container

    fn_prepare_request = fn request ->
      request
      |> add_header("x-ms-lease-action", "break")
      |> add_header("x-ms-lease-id", "#{lease_id}")
      |> add_header("x-ms-lease-break-period", "#{break_period}")
    end

    fn_prepare_response = fn result, response ->
      result
      |> Map.put(:lease_time, response.headers["x-ms-lease-time"] |> Integer.parse() |> elem(0))
    end

    context
    |> container_lease_handler(
      container_name,
      202,
      fn_prepare_request,
      fn_prepare_response
    )
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
      |> add_ms_context(context, DateTimeUtils.utc_now(), @storage_api_version)
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

  defp create_error_response(response = %{}) do
    {:error,
     response.body
     |> xmap(
       code: ~x"/Error/Code/text()"s,
       message: ~x"/Error/Message/text()"s,
       authnErrDetail: ~x"/Error/AuthenticationErrorDetail/text()"s
     )
     |> Map.update!(:message, &String.split(&1, "\n"))
     |> Map.put(:http_status, response.status)
     |> Map.put(:url, response.url)
     |> Map.put(:body, response.body)
     |> Map.put(:request_id, response.headers["x-ms-request-id"])}
  end

  defp process_body(nil, default_value, _process_fn), do: default_value
  defp process_body("", default_value, _process_fn), do: default_value
  defp process_body(body, _default_value, process_fn), do: body |> process_fn.()
end
