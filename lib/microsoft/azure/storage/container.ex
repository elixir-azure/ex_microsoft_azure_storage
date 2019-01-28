defmodule Microsoft.Azure.Storage.Container do
  import SweetXml
  import Microsoft.Azure.Storage.RequestBuilder

  alias Microsoft.Azure.Storage
  alias Microsoft.Azure.Storage.{DateTimeUtils, BlobPolicy}

  @enforce_keys [:storage_context, :container_name]
  defstruct [:storage_context, :container_name]

  def new(storage_context = %Storage{}, container_name)
      when is_binary(container_name),
      do: %__MODULE__{storage_context: storage_context, container_name: container_name}

  defmodule Responses do
    def list_containers_response(),
      do: [
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
      ]

    def list_blobs_response(),
      do: [
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
            access_tier: ~x"./AccessTier/text()"s,
            access_tier_inferred: ~x"./AccessTierInferred/text()"s,
            lease_status: ~x"./LeaseStatus/text()"s,
            lease_state: ~x"./LeaseState/text()"s,
            server_encrypted: ~x"./ServerEncrypted/text()"s
          ]
        ]
      ]
  end

  def list_containers(context = %Storage{}) do
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
         |> xmap(__MODULE__.Responses.list_containers_response())
         |> Map.put(:headers, response.headers)
         |> Map.put(:url, response.url)
         |> Map.put(:status, response.status)
         |> Map.put(:request_id, response.headers["x-ms-request-id"])}
    end
  end

  def create_container(%__MODULE__{storage_context: context, container_name: container_name}) do
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

  def get_container_properties(%__MODULE__{
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

  def get_container_metadata(%__MODULE__{storage_context: context, container_name: container_name}) do
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

  def get_container_acl(%__MODULE__{storage_context: context, container_name: container_name}) do
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

  def set_container_acl_public_access_off(container = %__MODULE__{}),
    do: container |> set_container_acl(:off)

  def set_container_acl_public_access_blob(container = %__MODULE__{}),
    do: container |> set_container_acl(:blob)

  def set_container_acl_public_access_container(container = %__MODULE__{}),
    do: container |> set_container_acl(:container)

  def set_container_acl(
        %__MODULE__{storage_context: context, container_name: container_name},
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
        %__MODULE__{storage_context: context, container_name: container_name},
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

  def delete_container(%__MODULE__{storage_context: context, container_name: container_name}) do
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
        %__MODULE__{storage_context: context, container_name: container_name},
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
      |> add_param_if(opts[:prefix] != nil, :query, :prefix, opts[:prefix])
      |> add_param_if(opts[:delimiter] != nil, :query, :delimiter, opts[:delimiter])
      |> add_param_if(opts[:marker] != nil, :query, :marker, opts[:marker])
      |> add_param_if(opts[:maxresults] != nil, :query, :maxresults, opts[:maxresults])
      |> add_param_if(opts[:timeout] != nil, :query, :timeout, opts[:timeout])
      |> add_param_if(opts[:include] != nil, :query, :include, opts[:include])
      |> add_ms_context(context, DateTimeUtils.utc_now(), :storage)
      |> sign_and_call(:blob_service)

    case response do
      %{status: status} when 400 <= status and status < 500 ->
        response |> create_error_response()

      %{status: 200} ->
        {:ok,
         response.body
         |> xmap(__MODULE__.Responses.list_blobs_response())
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
