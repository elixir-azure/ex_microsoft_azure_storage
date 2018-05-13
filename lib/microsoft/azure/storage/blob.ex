defmodule Microsoft.Azure.Storage.Blob do
  use NamedArgs
  # import SweetXml
  import Microsoft.Azure.Storage.RequestBuilder
  alias Microsoft.Azure.Storage.{DateTimeUtils, AzureStorageContext}

  @max_block_size_100MB 104_857_600

  def to_block_id(block_id) when is_binary(block_id), do: block_id |> Base.encode64()
  def to_block_id(block_id) when is_integer(block_id), do: <<block_id::120>> |> Base.encode64()

  @doc """
  The `put_block` operation creates a new block to be committed as part of a blob.
  """
  def put_block(context = %AzureStorageContext{}, container_name, blob_name, block_id, content)
    when byte_size(content) <= @max_block_size_100MB do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/put-block

    response =
      new_azure_storage_request()
      |> method(:put)
      |> url("/#{container_name}/#{blob_name}")
      |> add_param(:query, :comp, "block")
      |> add_param(:query, :blockid, block_id |> to_block_id())
      |> body(content)
      |> add_header_content_md5()
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
           last_modified: response.headers["last-modified"],
           server_encrypted: response.headers["x-ms-request-server-encrypted"],
           content_md5: response.headers["x-ms-request-server-encrypted"],
           body: response.body
         }}
    end
  end
end
