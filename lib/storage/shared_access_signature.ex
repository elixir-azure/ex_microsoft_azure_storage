defmodule ExMicrosoftAzureStorage.Storage.SharedAccessSignature do
  @moduledoc """
  SharedAccessSignature
  """

  alias ExMicrosoftAzureStorage.Storage
  import ExMicrosoftAzureStorage.Storage.Utilities, only: [add_to: 3, set_to_string: 2]

  # https://docs.microsoft.com/en-us/rest/api/storageservices/delegating-access-with-a-shared-access-signature
  # https://docs.microsoft.com/en-us/azure/storage/common/storage-dotnet-shared-access-signature-part-1
  # https://github.com/chgeuer/private_gists/blob/76db1345142d25d3359af6ce4ba7b9eef1aeb769/azure/AccountSAS/AccountSas.cs

  defstruct [
    :service_version,
    :target_scope,
    :services,
    :resource_type,
    :permissions,
    :start_time,
    :expiry_time,
    :canonicalized_resource,
    :resource,
    :ip_range,
    :protocol,
    :cache_control,
    :content_disposition,
    :content_encoding,
    :content_language,
    :content_type
  ]

  def new, do: %__MODULE__{}

  def for_storage_account(%__MODULE__{target_scope: nil} = v),
    do: v |> Map.put(:target_scope, :account)

  def for_blob_service(%__MODULE__{target_scope: nil} = v), do: v |> Map.put(:target_scope, :blob)

  def for_table_service(%__MODULE__{target_scope: nil} = v),
    do: v |> Map.put(:target_scope, :table)

  def for_queue_service(%__MODULE__{target_scope: nil} = v),
    do: v |> Map.put(:target_scope, :queue)

  # https://docs.microsoft.com/en-us/rest/api/storageservices/constructing-an-account-sas#specifying-account-sas-parameters
  @services_map %{blob: "b", queue: "q", table: "t", file: "f"}
  def add_service_blob(%__MODULE__{target_scope: :account} = v), do: v |> add_to(:services, :blob)

  def add_service_queue(%__MODULE__{target_scope: :account} = v),
    do: v |> add_to(:services, :queue)

  def add_service_table(%__MODULE__{target_scope: :account} = v),
    do: v |> add_to(:services, :table)

  def add_service_file(%__MODULE__{target_scope: :account} = v), do: v |> add_to(:services, :file)

  @resource_types_map %{service: "s", object: "o", container: "c"}
  def add_resource_type_service(%__MODULE__{target_scope: :account} = v),
    do: v |> add_to(:resource_type, :service)

  def add_resource_type_container(%__MODULE__{target_scope: :account} = v),
    do: v |> add_to(:resource_type, :container)

  def add_resource_type_object(%__MODULE__{target_scope: :account} = v),
    do: v |> add_to(:resource_type, :object)

  @resource_map %{
    # https://docs.microsoft.com/en-us/rest/api/storageservices/constructing-a-service-sas#specifying-the-signed-resource-blob-service-only
    container: "c",
    blob: "b",
    # https://docs.microsoft.com/en-us/rest/api/storageservices/constructing-a-service-sas#specifying-the-signed-resource-file-service-only
    share: "s",
    file: "f"
  }

  def add_resource_blob_container(%__MODULE__{} = v), do: v |> add_to(:resource, :container)

  def add_resource_blob_blob(%__MODULE__{} = v), do: v |> add_to(:resource, :blob)

  @permissions_map %{
    read: "r",
    write: "w",
    delete: "d",
    list: "l",
    add: "a",
    create: "c",
    update: "u",
    process: "p"
  }
  def add_permission_read(%__MODULE__{} = v), do: add_to(v, :permissions, :read)
  def add_permission_write(%__MODULE__{} = v), do: add_to(v, :permissions, :write)
  def add_permission_delete(%__MODULE__{} = v), do: add_to(v, :permissions, :delete)
  def add_permission_list(%__MODULE__{} = v), do: add_to(v, :permissions, :list)
  def add_permission_add(%__MODULE__{} = v), do: add_to(v, :permissions, :add)
  def add_permission_create(%__MODULE__{} = v), do: add_to(v, :permissions, :create)
  def add_permission_update(%__MODULE__{} = v), do: add_to(v, :permissions, :update)
  def add_permission_process(%__MODULE__{} = v), do: add_to(v, :permissions, :process)

  def add_canonicalized_resource(%__MODULE__{} = v, resource_name) do
    %{v | canonicalized_resource: resource_name}
  end

  def as_time(t), do: t |> Timex.format!("{YYYY}-{0M}-{0D}T{0h24}:{0m}:{0s}Z")

  def service_version(%__MODULE__{} = v, service_version),
    do: %{v | service_version: service_version}

  def start_time(%__MODULE__{} = v, start_time), do: %{v | start_time: start_time}
  def expiry_time(%__MODULE__{} = v, expiry_time), do: %{v | expiry_time: expiry_time}
  def resource(%__MODULE__{} = v, resource), do: %{v | resource: resource}
  def ip_range(%__MODULE__{} = v, ip_range), do: %{v | ip_range: ip_range}
  def protocol(%__MODULE__{} = v, protocol), do: %{v | protocol: protocol}

  def cache_control(%__MODULE__{} = v, cache_control), do: %{v | cache_control: cache_control}

  def content_disposition(%__MODULE__{} = v, content_disposition),
    do: %{v | content_disposition: content_disposition}

  def content_encoding(%__MODULE__{} = v, content_encoding),
    do: %{v | content_encoding: content_encoding}

  def content_language(%__MODULE__{} = v, content_language),
    do: %{v | content_language: content_language}

  def content_type(%__MODULE__{} = v, content_type), do: %{v | content_type: content_type}

  def encode({:service_version, value}), do: {"sv", value}
  def encode({:start_time, value}), do: {"st", value |> as_time()}

  def encode({:expiry_time, value}), do: {"se", value |> as_time()}
  def encode({:canonicalized_resource, value}), do: {"cr", value}
  def encode({:resource, value}), do: {"sr", value |> set_to_string(@resource_map)}
  def encode({:ip_range, value}), do: {"sip", value}
  def encode({:protocol, value}), do: {"spr", value}
  def encode({:services, value}), do: {"ss", value |> set_to_string(@services_map)}
  def encode({:resource_type, value}), do: {"srt", value |> set_to_string(@resource_types_map)}
  def encode({:permissions, value}), do: {"sp", value |> set_to_string(@permissions_map)}
  def encode({:cache_control, value}), do: {"rscc", value}
  def encode({:content_disposition, value}), do: {"rscd", value}
  def encode({:content_encoding, value}), do: {"rsce", value}
  def encode({:content_language, value}), do: {"rscl", value}
  def encode({:content_type, value}), do: {"rsct", value}
  def encode(_), do: {nil, nil}

  # https://docs.microsoft.com/en-us/rest/api/storageservices/create-service-sas#version-2018-11-09-and-later
  # StringToSign = signedPermissions + "\n" +
  #              signedStart + "\n" +
  #              signedExpiry + "\n" +
  #              canonicalizedResource + "\n" +
  #              signedIdentifier + "\n" +
  #              signedIP + "\n" +
  #              signedProtocol + "\n" +
  #              signedVersion + "\n" +
  #              signedResource + "\n"
  #              signedSnapshotTime + "\n" +
  #              rscc + "\n" +
  #              rscd + "\n" +
  #              rsce + "\n" +
  #              rscl + "\n" +
  #              rsct
  defp string_to_sign(values, _account_name, :blob) do
    [
      # permissions
      values |> Map.get("sp", ""),
      # start date
      values |> Map.get("st", ""),
      # expiry date
      values |> Map.get("se", ""),
      # canonicalized resource
      values |> Map.get("cr", ""),
      # identifier
      "",
      # IP address
      values |> Map.get("sip", ""),
      # Protocol
      values |> Map.get("spr", ""),
      # Version
      values |> Map.get("sv", ""),
      # resource
      values |> Map.get("sr"),
      # snapshottime
      "",
      # rscc - Cache-Control
      values |> Map.get("rscc", ""),
      # rscd - Content-Disposition
      values |> Map.get("rscd", ""),
      # rsce - Content-Encoding
      values |> Map.get("rsce", ""),
      # rscl - Content-Language
      values |> Map.get("rscl", ""),
      # rsct - Content-Type
      values |> Map.get("rsct", "")
    ]
    |> Enum.join("\n")
  end

  defp string_to_sign(values, account_name, _) do
    [
      account_name,
      values |> Map.get("sp", ""),
      values |> Map.get("ss", ""),
      values |> Map.get("srt", ""),
      values |> Map.get("st", ""),
      values |> Map.get("se", ""),
      values |> Map.get("sip", ""),
      values |> Map.get("spr", ""),
      values |> Map.get("sv", ""),
      ""
    ]
    |> Enum.join("\n")
  end

  def sign(
        %__MODULE__{target_scope: target_scope} = sas,
        %Storage{account_name: account_name, account_key: account_key}
      )
      when is_atom(target_scope) and target_scope != nil do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/service-sas-examples
    values =
      sas
      |> Map.from_struct()
      |> Enum.filter(fn {_, val} -> val != nil end)
      |> Enum.map(&__MODULE__.encode/1)
      |> Enum.filter(fn {_, val} -> val != nil end)
      |> Map.new()

    string_to_sign = string_to_sign(values, account_name, target_scope)

    signature =
      Storage.Crypto.hmac(:sha256, account_key |> Base.decode64!(), string_to_sign)
      |> Base.encode64()

    values
    |> Map.put("sig", signature)
    |> Map.drop(["cr"])
    |> URI.encode_query()
  end
end
