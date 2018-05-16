defmodule Microsoft.Azure.Storage.Blob do
  import SweetXml
  use NamedArgs
  # import SweetXml
  import Microsoft.Azure.Storage.RequestBuilder
  alias Microsoft.Azure.Storage.{DateTimeUtils, AzureStorageContext}

  @max_block_size_100MB 104_857_600

  # |> Base.encode64()
  def to_block_id(block_id) when is_binary(block_id), do: block_id
  def to_block_id(block_id) when is_integer(block_id), do: <<block_id::120>> |> Base.encode64()

  @doc """
  The `put_block` operation creates a new block to be committed as part of a blob.
  """
  def put_block(context = %AzureStorageContext{}, container_name, blob_name, block_id, content)
      when is_binary(block_id) and byte_size(content) <= @max_block_size_100MB do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/put-block

    response =
      new_azure_storage_request()
      |> method(:put)
      |> url("/#{container_name}/#{blob_name}")
      |> add_param(:query, :comp, "block")
      # |> to_block_id())
      |> add_param(:query, :blockid, block_id)
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
           content_md5: response.headers["Content-MD5"],
           body: response.body
         }}
    end
  end

  @doc """
  The `put_block_list` operation writes a blob by specifying the list of block IDs that make up the blob.
  """
  def put_block_list(context = %AzureStorageContext{}, container_name, blob_name, block_list)
      when is_list(block_list) do
    response =
      new_azure_storage_request()
      |> method(:put)
      |> url("/#{container_name}/#{blob_name}")
      |> add_param(:query, :comp, "blocklist")
      |> body(block_list |> serialize_block_list())
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
           etag: response.headers["etag"],
           request_id: response.headers["x-ms-request-id"],
           last_modified: response.headers["last-modified"],
           server_encrypted: response.headers["x-ms-request-server-encrypted"],
           content_md5: response.headers["Content-MD5"],
           body: response.body
         }}
    end
  end

  @template_block_list """
  <?xml version="1.0" encoding="utf-8"?>
  <BlockList>
    <%= for block <- @block_list do %>
    <Latest><%= block |> Microsoft.Azure.Storage.Blob.to_block_id() %></Latest>
    <% end %>
  </BlockList>
  """

  defp serialize_block_list(block_list),
    do: @template_block_list |> EEx.eval_string(assigns: [block_list: block_list])

  defp deserialize_block_list(xml_body) do
    deserialize_block = fn node ->
      %{
        name: node |> xpath(~x"./Name/text()"s),
        size:
          node
          |> xpath(
            ~x"./Size/text()"s
            |> transform_by(fn t -> t |> Integer.parse() |> elem(0) end)
          )
      }
    end

    %{
      committed_blocks:
        xml_body
        |> xpath(~x"/BlockList/CommittedBlocks/Block"l)
        |> Enum.map(deserialize_block),
      uncommitted_blocks:
        xml_body
        |> xpath(~x"/BlockList/UncommittedBlocks/Block"l)
        |> Enum.map(deserialize_block)
    }
  end

  def get_block_list(
        context = %AzureStorageContext{},
        container_name,
        blob_name,
        block_list_type \\ :all,
        snapshot \\ nil
      )
      when block_list_type in [:all, :committed, :uncommitted] do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/get-block-list

    response =
      new_azure_storage_request()
      |> method(:get)
      |> url("/#{container_name}/#{blob_name}")
      |> add_param(:query, :comp, "blocklist")
      |> add_param(:query, :blocklisttype, block_list_type |> Atom.to_string())
      |> add_param_if(snapshot != nil, :query, :snapshot, snapshot)
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
           server_encrypted: response.headers["x-ms-request-server-encrypted"],
           content_md5: response.headers["Content-MD5"],
           body: response.body
         }
         |> Map.merge(response.body |> deserialize_block_list())}
    end
  end

  def upload_file(context = %AzureStorageContext{}, container_name, filename) do
    block_size = 1024 * 1024
    max_concurrency = 3
    blob_name = String.replace(filename, Path.dirname(filename) <> "/", "") |> URI.encode()

    existing_block_ids =
      case context |> get_block_list(container_name, blob_name, :all) do
        {:error, %{code: "BlobNotFound", http_status: 404}} ->
          []

        {:ok, %{uncommitted_blocks: uncommitted_blocks, committed_blocks: committed_blocks}} ->
          (uncommitted_blocks ++ committed_blocks)
          |> Enum.map(fn %{name: name} -> name end)
          |> Enum.uniq()
      end

    block_ids =
      filename
      |> File.stream!([:raw, :read_ahead, :binary], block_size)
      |> Stream.zip(1..50_000)
      |> Task.async_stream(
        fn {content, i} ->
          block_id = i |> to_block_id()

          if !(block_id in existing_block_ids) do
            IO.puts("Upload block #{block_id}")
            {:ok, _} =
              context
              |> put_block(container_name, blob_name, block_id, content)
            IO.puts("Done   block #{block_id}")
          end

          block_id
        end,
        max_concurrency: max_concurrency,
        ordered: true,
        timeout: :infinity
      )
      |> Stream.map(fn {:ok, block_id} -> block_id end)
      |> Enum.to_list()

    context
    |> put_block_list(container_name, blob_name, block_ids)
  end
end
