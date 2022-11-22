defmodule ExMicrosoftAzureStorage.Storage.Blob do
  @moduledoc """
  Blob
  """
  require Logger

  import ExMicrosoftAzureStorage.Storage.RequestBuilder
  import SweetXml

  alias ExMicrosoftAzureStorage.Storage.BlobProperties
  alias ExMicrosoftAzureStorage.Storage.Container

  @enforce_keys [:container, :blob_name]
  @max_concurrency 3
  @max_number_of_blocks 50_000
  @mega_byte 1024 * 1024
  @max_block_size 4 * @mega_byte
  @max_block_size_100_mega_byte 100 * @mega_byte

  defstruct [:container, :blob_name]

  def new(%Container{} = container, blob_name)
      when is_binary(blob_name),
      do: %__MODULE__{container: container, blob_name: blob_name}

  def to_block_id(block_id) when is_binary(block_id), do: block_id
  def to_block_id(block_id) when is_integer(block_id), do: <<block_id::120>> |> Base.encode64()

  @doc """
  The `put_block` operation creates a new block to be committed as part of a blob.
  """
  def put_block(
        %__MODULE__{
          container: %Container{storage_context: context, container_name: container_name},
          blob_name: blob_name
        },
        block_id,
        content
      )
      when is_binary(block_id) and byte_size(content) <= @max_block_size_100_mega_byte do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/put-block

    response =
      context
      |> new_azure_storage_request()
      |> method(:put)
      |> url("/#{container_name}/#{blob_name}")
      |> add_param(:query, :comp, "block")
      # |> to_block_id())
      |> add_param(:query, :blockid, block_id)
      |> body(content)
      |> add_header_content_md5()
      |> sign_and_call(:blob_service)

    case response do
      %{status: status} when 400 <= status and status < 500 ->
        {:error, response |> create_error_response()}

      %{status: 201} ->
        {:ok,
         response
         |> create_success_response()}
    end
  end

  @doc """
  The `put_block_list` operation writes a blob by specifying the list of block IDs that make up the blob.
  """
  def put_block_list(
        %__MODULE__{
          container: %Container{storage_context: context, container_name: container_name},
          blob_name: blob_name
        },
        block_list,
        headers \\ []
      )
      when is_list(block_list) do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/put-block-list
    response =
      context
      |> new_azure_storage_request()
      |> method(:put)
      |> url("/#{container_name}/#{blob_name}")
      |> add_param(:query, :comp, "blocklist")
      |> body(block_list |> serialize_block_list())
      |> add_headers(headers)
      |> sign_and_call(:blob_service)

    case response do
      %{status: status} when 400 <= status and status < 500 ->
        {:error, response |> create_error_response()}

      %{status: 201} ->
        {:ok,
         response
         |> create_success_response()}
    end
  end

  @template_block_list """
  <?xml version="1.0" encoding="utf-8"?>
  <BlockList>
    <%= for block <- @block_list do %>
    <Latest><%= block |> ExMicrosoftAzureStorage.Storage.Blob.to_block_id() %></Latest>
    <% end %>
  </BlockList>
  """

  defp serialize_block_list(block_list),
    do: @template_block_list |> EEx.eval_string(assigns: [block_list: block_list]) |> to_string()

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
        %__MODULE__{
          container: %Container{storage_context: context, container_name: container_name},
          blob_name: blob_name
        },
        block_list_type \\ :all,
        snapshot \\ nil
      )
      when block_list_type in [:all, :committed, :uncommitted] do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/get-block-list

    response =
      context
      |> new_azure_storage_request()
      |> method(:get)
      |> url("/#{container_name}/#{blob_name}")
      |> add_param(:query, :comp, "blocklist")
      |> add_param(:query, :blocklisttype, block_list_type |> Atom.to_string())
      |> add_param_if(snapshot != nil, :query, :snapshot, snapshot)
      |> sign_and_call(:blob_service)

    case response do
      %{status: status} when 400 <= status and status < 500 ->
        {:error, response |> create_error_response()}

      %{status: 200} ->
        {:ok,
         response
         |> create_success_response()
         |> Map.merge(response.body |> deserialize_block_list())}
    end
  end

  def get_blob(blob, opts \\ [])

  def get_blob(
        %__MODULE__{
          container: %Container{storage_context: context, container_name: container_name},
          blob_name: blob_name
        },
        _opts
      ) do
    response =
      context
      |> new_azure_storage_request()
      |> method(:get)
      |> url("/#{container_name}/#{blob_name}")
      |> sign_and_call(:blob_service)

    case response do
      %{status: status} when 400 <= status and status < 500 ->
        {:error, response |> create_error_response()}

      %{status: 200} ->
        {:ok, response |> create_success_response()}
    end
  end

  def get_blob_properties(%__MODULE__{
        container: %Container{storage_context: context, container_name: container_name},
        blob_name: blob_name
      }) do
    response =
      context
      |> new_azure_storage_request()
      |> method(:head)
      |> url("/#{container_name}/#{blob_name}")
      |> sign_and_call(:blob_service)

    case response do
      %{status: status} when 400 <= status and status < 500 ->
        {:error, response |> create_error_response()}

      %{status: 200} ->
        {:ok,
         response
         |> create_success_response()
         |> Map.put(:properties, response.headers |> BlobProperties.deserialise())}
    end
  end

  @allowed_set_blob_headers [
    "x-ms-blob-cache-control",
    "x-ms-blob-content-type",
    "x-ms-blob-content-md5",
    "x-ms-blob-content-encoding",
    "x-ms-blob-content-language",
    "x-ms-blob-content-disposition"
  ]

  @doc """
  Sets blob properties.

  Follows the same behaviour as the underlying REST API where setting one property will also
  implicitly set others to nil, unless you explitly set them in this request.

  See <https://docs.microsoft.com/en-us/rest/api/storageservices/set-blob-properties>
  """
  def set_blob_properties(
        %__MODULE__{
          container: %Container{storage_context: context, container_name: container_name},
          blob_name: blob_name
        },
        %BlobProperties{} = blob_properties
      ) do
    headers =
      blob_properties
      |> BlobProperties.serialise()
      |> Enum.map(&transform_set_blob_property_header/1)
      |> Enum.filter(fn {header, _value} -> Enum.member?(@allowed_set_blob_headers, header) end)

    response =
      context
      |> new_azure_storage_request()
      |> method(:put)
      |> url("/#{container_name}/#{blob_name}")
      |> add_param(:query, :comp, "properties")
      |> add_headers(headers)
      |> sign_and_call(:blob_service)

    case response do
      %{status: status} when 400 <= status and status < 500 ->
        {:error, response |> create_error_response()}

      %{status: 200} ->
        {:ok, response |> create_success_response()}
    end
  end

  @doc """
  Updates blob properties.

  Similar to `set_blob_properties/2` but keeps existing values by first performing
  `get_blob_properties/2`, merging the result and handing it over to `set_blob_properties/2`.
  """
  def update_blob_properties(blob, blob_properties) do
    with {:ok, %{properties: existing_blob_properties}} <- blob |> get_blob_properties() do
      merged_properties = Map.merge(existing_blob_properties, blob_properties)
      blob |> set_blob_properties(merged_properties)
    end
  end

  defp transform_set_blob_property_header({"cache-control", value}),
    do: {"x-ms-blob-cache-control", value}

  defp transform_set_blob_property_header({"content-type", value}),
    do: {"x-ms-blob-content-type", value}

  defp transform_set_blob_property_header({"content-md5", value}),
    do: {"x-ms-blob-content-md5", value}

  defp transform_set_blob_property_header({"content-encoding", value}),
    do: {"x-ms-blob-content-encoding", value}

  defp transform_set_blob_property_header({"content-language", value}),
    do: {"x-ms-blob-content-language", value}

  defp transform_set_blob_property_header({"content-disposition", value}),
    do: {"x-ms-blob-content-disposition", value}

  defp transform_set_blob_property_header(header), do: header

  def put_blob(
        %__MODULE__{
          container: %Container{storage_context: context, container_name: container_name},
          blob_name: blob_name
        },
        blob_data,
        opts \\ []
      ) do
    opts =
      opts
      |> Keyword.put(:blob_type, "BlockBlob")

    response =
      context
      |> new_azure_storage_request()
      |> method(:put)
      |> url("/#{container_name}/#{blob_name}")
      |> body(blob_data)
      |> add_headers_from_opts(opts)
      |> add_header_content_md5()
      |> sign_and_call(:blob_service)

    case response do
      %{status: status} when 400 <= status and status < 500 ->
        {:error, response |> create_error_response()}

      %{status: 201} ->
        {:ok, response |> create_success_response()}
    end
  end

  defp add_headers(request, headers) do
    Enum.reduce(headers, request, fn {k, v}, request -> request |> add_header(k, v) end)
  end

  defp add_headers_from_opts(request, opts) do
    Enum.reduce(opts, request, fn {key, value}, request ->
      request |> add_header(header_for_opt(key), value)
    end)
  end

  defp header_for_opt(:blob_type), do: "x-ms-blob-type"
  defp header_for_opt(:copy_source), do: "x-ms-copy-source"
  defp header_for_opt(:content_type), do: "x-ms-blob-content-type"
  defp header_for_opt(:content_disposition), do: "x-ms-blob-content-disposition"
  defp header_for_opt(:content_encoding), do: "x-ms-blob-content-encoding"

  def put_blob_from_url(
        %__MODULE__{
          container: %Container{storage_context: context, container_name: container_name},
          blob_name: blob_name
        } = blob,
        url,
        opts \\ []
      ) do
    opts =
      opts
      |> Keyword.put(:blob_type, "BlockBlob")
      |> Keyword.put(:copy_source, url)

    {content_opts, opts} =
      opts
      |> Keyword.split([:content_type, :content_encoding, :content_disposition, :content_language])

    {content_type_workaround_enabled, opts} = opts |> Keyword.pop(:content_type_workaround, false)

    response =
      context
      |> new_azure_storage_request()
      |> method(:put)
      |> url("/#{container_name}/#{blob_name}")
      |> add_headers_from_opts(opts)
      |> sign_and_call(:blob_service)

    case response do
      %{status: status} when 400 <= status and status < 500 ->
        {:error, response |> create_error_response()}

      %{status: 201} ->
        with {:ok, _response} <-
               workaround_for_put_blob_from_url(
                 blob,
                 url,
                 content_opts,
                 content_type_workaround_enabled
               ) do
          {:ok, response |> create_success_response()}
        end
    end
  end

  # Workaround for a bug in Azure Storage where original content-type is lost on put_blob_from_url
  # requests https://github.com/joeapearson/elixir-azure/issues/2
  defp workaround_for_put_blob_from_url(_blob, _url, _content_opts, false) do
    unless suppress_workaround_for_put_blob_from_url_warning?() do
      Logger.warning("""
      Your blob's content-* metadata may not have been correctly copied.

      Set `content_type_workaround: true` when calling `Blob.put_blob_from_url/2` to work around.

      See https://github.com/joeapearson/elixir-azure/issues/2
      """)
    end

    {:ok, nil}
  end

  defp workaround_for_put_blob_from_url(blob, url, [], true) do
    # In this case we have to do the work of finding out what the original source content-type was
    # and then setting it on the blob.  Results in many requests and accordingly is less reliable.

    with {:ok, %{status: 200, headers: source_headers}} <- Tesla.head(url) do
      blob_properties = BlobProperties.deserialise(source_headers)
      update_blob_properties(blob, blob_properties)
    end
  end

  defp workaround_for_put_blob_from_url(blob, _url, content_type_attrs, true) do
    blob |> update_blob_properties(struct!(BlobProperties, content_type_attrs))
  end

  defp suppress_workaround_for_put_blob_from_url_warning? do
    Keyword.get(config(), :suppress_workaround_for_put_blob_from_url_warning?, false)
  end

  @spec upload_file(Container.t(), String.t(), String.t() | nil, map | nil) ::
          {:ok, map} | {:error, map}
  def upload_file(
        container,
        source_path,
        blob_name \\ nil,
        blob_properties \\ nil
      )

  def upload_file(
        %Container{} = container,
        source_path,
        blob_name,
        blob_properties
      )
      when is_map(blob_properties) do
    headers =
      BlobProperties
      |> struct(blob_properties)
      |> BlobProperties.serialise()
      |> Enum.map(&transform_set_blob_property_header/1)
      |> Enum.filter(fn {header, _value} -> Enum.member?(@allowed_set_blob_headers, header) end)

    container
    |> to_blob(source_path, blob_name)
    |> upload_async(source_path, headers)
  end

  def upload_file(%Container{} = container, source_path, blob_name, nil) do
    container
    |> to_blob(source_path, blob_name)
    |> upload_async(source_path, [])
  end

  defp to_blob(container, source_path, nil) do
    target_filename =
      source_path
      |> Path.basename()
      |> URI.encode()

    to_blob(container, source_path, target_filename)
  end

  defp to_blob(container, _source_filename, target_filename) do
    __MODULE__.new(container, target_filename)
  end

  defp upload_async(blob, filename, headers) do
    blob
    |> upload_stream(filename)
    |> stream_to_block_ids()
    |> case do
      {:error, _reason} = err ->
        err

      {:ok, ids} ->
        commit_block_ids(blob, ids, headers)
    end
  end

  defp upload_stream(blob, filename) do
    filename
    |> File.stream!([], @max_block_size)
    |> Stream.zip(1..@max_number_of_blocks)
    |> Task.async_stream(
      fn {content, i} ->
        block_id = to_block_id(i)

        case put_block(blob, block_id, content) do
          {:ok, _} ->
            block_id

          {:error, _resp} = error ->
            error
        end
      end,
      max_concurrency: @max_concurrency,
      ordered: true,
      timeout: :infinity
    )
    |> Enum.to_list()
  end

  defp stream_to_block_ids(results) do
    Enum.reduce_while(results, {:ok, []}, fn
      {_, {:error, reason}}, {_status, _ids} ->
        {:halt, {:error, reason}}

      {_, id}, {status, ids} ->
        {:cont, {status, [id | ids]}}
    end)
  end

  defp commit_block_ids(blob, ids, headers) do
    block_ids =
      1..@max_number_of_blocks
      |> Enum.map(&to_block_id/1)
      |> Enum.filter(&(&1 in ids))

    put_block_list(blob, block_ids, headers)
  end

  def delete_blob(
        %__MODULE__{
          container: %Container{storage_context: context, container_name: container_name},
          blob_name: blob_name
        },
        opts \\ []
      ) do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/delete-blob

    %{snapshot: snapshot, timeout: timeout} =
      case [snapshot: nil, timeout: -1]
           |> Keyword.merge(opts)
           |> Enum.into(%{}) do
        %{snapshot: snapshot, timeout: timeout} -> %{snapshot: snapshot, timeout: timeout}
      end

    response =
      context
      |> new_azure_storage_request()
      |> method(:delete)
      |> url("/#{container_name}/#{blob_name}")
      |> add_param_if(snapshot != nil, :query, :snapshot, snapshot)
      |> add_param_if(timeout > 0, :query, :timeout, timeout)
      |> sign_and_call(:blob_service)

    case response do
      %{status: status} when 400 <= status and status < 500 ->
        {:error, response |> create_error_response()}

      %{status: 202} ->
        {:ok, response |> create_success_response()}
    end
  end

  def copy_stream(
        %__MODULE__{} = source,
        %__MODULE__{
          container: %Container{storage_context: context, container_name: container_name},
          blob_name: blob_name
        } = target,
        opts \\ []
      ) do
    opts =
      opts
      |> Keyword.put(:copy_source, url(source))

    poll_interval = Keyword.get(opts, :poll_interval, 5000)

    Stream.resource(
      fn ->
        context
        |> new_azure_storage_request()
        |> method(:put)
        |> url("/#{container_name}/#{blob_name}")
        |> add_headers_from_opts(opts)
        |> sign_and_call(:blob_service)
      end,
      fn
        nil ->
          :timer.sleep(poll_interval)
          {[get_blob_properties(target)], nil}

        %{status: status} = response when 400 <= status and status < 500 ->
          {[{:error, response |> create_error_response()}], nil}

        %{status: status} = response when status < 300 ->
          {[{:ok, response |> create_success_response()}], nil}
      end,
      fn _ -> nil end
    )
    |> Stream.flat_map(fn
      {:ok, %{x_ms_copy_status: status}} = result
      when status != "success" and status != "failed" ->
        [result]

      result ->
        [result, :halt]
    end)
    |> Stream.take_while(fn
      :halt -> false
      _ -> true
    end)
  end

  def copy(
        %__MODULE__{} = source,
        %__MODULE__{} = target,
        opts \\ []
      ) do
    copy_stream(source, target, opts)
    |> Enum.reduce(nil, fn result, _ -> result end)
  end

  def url(%__MODULE__{
        container: %Container{
          storage_context: context,
          container_name: container
        },
        blob_name: blob_name
      }),
      do: ExMicrosoftAzureStorage.Storage.endpoint_url(context, :blob_service) <> "/#{container}/#{blob_name}"

  defp config, do: Application.get_env(:azure, __MODULE__, [])
end
