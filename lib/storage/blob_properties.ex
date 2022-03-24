defmodule ExMicrosoftAzureStorage.Storage.BlobProperties do
  @moduledoc """
  Blob properties.
  """

  import ExMicrosoftAzureStorage.Storage.DateTimeUtils
  import ExMicrosoftAzureStorage.Storage.Utilities

  defstruct [
    :last_modified,
    :creation_time,
    :tag_count,
    :blob_type,
    :copy_completion_time,
    :copy_status_description,
    :copy_id,
    :copy_progress,
    :copy_source,
    :copy_status,
    :incremental_copy,
    :copy_destination_snapshot,
    :lease_duration,
    :lease_state,
    :lease_status,
    :content_length,
    :content_type,
    :etag,
    :content_md5,
    :content_encoding,
    :content_language,
    :content_disposition,
    :cache_control,
    :blob_sequence_number,
    :accept_ranges,
    :blob_committed_block_count,
    :blob_server_encrypted,
    :encryption_key_sha256,
    :encryption_scope,
    :access_tier,
    :access_tier_inferred,
    :archive_status,
    :access_tier_change_time,
    :rehydrate_priority,
    :last_access_time,
    :blob_sealed
  ]

  @type t() :: %__MODULE__{
          last_modified: DateTime.t(),
          creation_time: DateTime.t(),
          tag_count: integer(),
          blob_type: String.t(),
          copy_completion_time: DateTime.t(),
          copy_status_description: String.t(),
          copy_id: String.t(),
          copy_progress: String.t(),
          copy_source: String.t(),
          copy_status: String.t(),
          incremental_copy: boolean(),
          copy_destination_snapshot: DateTime.t(),
          lease_duration: String.t(),
          lease_state: String.t(),
          lease_status: String.t(),
          content_length: non_neg_integer(),
          content_type: String.t(),
          etag: String.t(),
          content_md5: String.t(),
          content_encoding: String.t(),
          content_language: String.t(),
          content_disposition: String.t(),
          cache_control: String.t(),
          blob_sequence_number: non_neg_integer(),
          accept_ranges: String.t(),
          blob_committed_block_count: non_neg_integer(),
          blob_server_encrypted: boolean(),
          encryption_key_sha256: String.t(),
          encryption_scope: String.t(),
          access_tier: String.t(),
          access_tier_inferred: boolean(),
          archive_status: String.t(),
          access_tier_change_time: DateTime.t(),
          rehydrate_priority: String.t(),
          last_access_time: DateTime.t(),
          blob_sealed: boolean()
        }

  @type headers() :: [{String.t(), String.t()}]

  @headers [
    # { header, key, format}
    {"last-modified", :last_modified, :rfc1123_datetime},
    {"x-ms-creation-time", :creation_time, :rfc1123_datetime},
    {"x-ms-tag-count", :tag_count, :integer},
    {"x-ms-blob-type", :blob_type, :string},
    {"x-ms-copy-completion-time", :copy_completion_time, :rfc1123_datetime},
    {"x-ms-copy-status-description", :copy_status_description, :string},
    {"x-ms-copy-id", :copy_id, :string},
    {"x-ms-copy-progress", :copy_progress, :string},
    {"x-ms-copy-source", :copy_source, :string},
    {"x-ms-copy-status", :copy_status, :string},
    {"x-ms-incremental_copy", :incremental_copy, :boolean},
    {"x-ms-copy-destination-snapshot", :copy_destination_snapshot, :rfc1123_datetime},
    {"x-ms-lease-duration", :lease_duration, :string},
    {"x-ms-lease-state", :lease_state, :string},
    {"x-ms-lease-status", :lease_status, :string},
    {"content-length", :content_length, :integer},
    {"content-type", :content_type, :string},
    {"etag", :etag, :string},
    {"content-md5", :content_md5, :string},
    {"content-encoding", :content_encoding, :string},
    {"content-language", :content_language, :string},
    {"content-disposition", :content_disposition, :string},
    {"cache-control", :cache_control, :string},
    {"x-ms-blob-sequence-number", :blob_sequence_number, :integer},
    {"accept-ranges", :accept_ranges, :string},
    {"x-ms-blob-committed-block-count", :blob_committed_block_count, :integer},
    {"x-ms-blob-server-encrypted", :blob_server_encrypted, :boolean},
    {"x-ms-encryption-key-sha256", :encryption_key_sha256, :string},
    {"x-ms-encryption-scope", :encryption_scope, :string},
    {"x-ms-access-tier", :access_tier, :string},
    {"x-ms-access-tier-inferred", :access_tier_inferred, :boolean},
    {"x-ms-archive-status", :archive_status, :boolean},
    {"x-ms-access-tier-change-time", :access_tier_change_time, :rfc1123_datetime},
    {"x-ms-rehydrate-priority", :rehydrate_priority, :string},
    {"x-ms-last-access-time", :last_access_time, :rfc1123_datetime},
    {"x-ms-blob-sealed", :blob_sealed, :boolean}
  ]

  @doc """
  Serialises a `BlobProperties` into headers as a list of key / value tuples.
  """
  @spec serialise(properties :: __MODULE__.t()) :: headers()
  def serialise(%__MODULE__{} = properties) do
    @headers
    |> Enum.reduce([], fn {header, key, type}, acc ->
      case Map.get(properties, key) do
        nil ->
          acc

        value ->
          [{header, encode(value, type)} | acc]
      end
    end)
  end

  @doc """
  Accepts a list of key value tuples and converts them into a `BlobProperties` struct
  """
  @spec deserialise(headers :: headers()) :: __MODULE__.t()
  def deserialise(headers) do
    attrs =
      @headers
      |> Enum.reduce(%{}, fn
        {header, key, type}, acc ->
          value = headers |> header(header) |> decode(type)
          acc |> Map.put(key, value)
      end)

    struct!(__MODULE__, attrs)
  end

  defp header(headers, header) do
    case List.keyfind(headers, header, 0) do
      nil -> nil
      {^header, value} -> value
    end
  end

  defp encode(value, :rfc1123_datetime), do: value |> to_string_rfc1123()
  defp encode(value, :integer), do: value |> Integer.to_string()
  defp encode(value, :boolean), do: value |> to_string()
  defp encode(value, _format), do: value

  defp decode(nil, _decoder), do: nil
  defp decode(value, :rfc1123_datetime), do: value |> date_parse_rfc1123()
  defp decode(value, :integer), do: value |> String.to_integer()
  defp decode(value, :boolean), do: value |> to_bool()
  defp decode(value, _format), do: value
end
