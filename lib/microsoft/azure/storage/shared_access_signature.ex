defmodule Microsoft.Azure.Storage.SharedAccessSignature do
  alias Microsoft.Azure.Storage
  alias Microsoft.Azure.Storage.ApiVersion
  import Microsoft.Azure.Storage.Utilities, only: [add_to: 3, set_to_string: 2]

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
    :resource,
    :permissions,
    :ip_range,
    :protocol
  ]

  def new(), do: %__MODULE__{}

  def for_storage_account(v = %__MODULE__{target_scope: nil}),
    do: v |> Map.put(:target_scope, :account)

  def for_blob_service(v = %__MODULE__{target_scope: nil}), do: v |> Map.put(:target_scope, :blob)

  def for_table_service(v = %__MODULE__{target_scope: nil}),
    do: v |> Map.put(:target_scope, :table)

  def for_queue_service(v = %__MODULE__{target_scope: nil}),
    do: v |> Map.put(:target_scope, :queue)

  # https://docs.microsoft.com/en-us/rest/api/storageservices/constructing-an-account-sas#specifying-account-sas-parameters
  @services_map %{blob: "b", queue: "q", table: "t", file: "f"}
  def add_service_blob(v = %__MODULE__{target_scope: :account}), do: v |> add_to(:services, :blob)

  def add_service_queue(v = %__MODULE__{target_scope: :account}),
    do: v |> add_to(:services, :queue)

  def add_service_table(v = %__MODULE__{target_scope: :account}),
    do: v |> add_to(:services, :table)

  def add_service_file(v = %__MODULE__{target_scope: :account}), do: v |> add_to(:services, :file)

  @resource_types_map %{service: "s", object: "o", container: "c"}
  def add_resource_type_service(v = %__MODULE__{target_scope: :account}),
    do: v |> add_to(:resource_type, :service)

  def add_resource_type_container(v = %__MODULE__{target_scope: :account}),
    do: v |> add_to(:resource_type, :container)

  def add_resource_type_object(v = %__MODULE__{target_scope: :account}),
    do: v |> add_to(:resource_type, :object)


  @resource_map %{
    # https://docs.microsoft.com/en-us/rest/api/storageservices/constructing-a-service-sas#specifying-the-signed-resource-blob-service-only
    container: "c",
    blob: "b",
    # https://docs.microsoft.com/en-us/rest/api/storageservices/constructing-a-service-sas#specifying-the-signed-resource-file-service-only
    share: "s",
    file: "f"
  }

  def add_resource_blob_container(v = %__MODULE__{}), do: v |> add_to(:resource, :container)

  def add_resource_blob_blob(v = %__MODULE__{}), do: v |> add_to(:resource, :blob)


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
  def add_permission_read(v = %__MODULE__{target_scope: :account}),
    do: v |> add_to(:permissions, :read)

  def add_permission_write(v = %__MODULE__{target_scope: :account}),
    do: v |> add_to(:permissions, :write)

  def add_permission_delete(v = %__MODULE__{target_scope: :account}),
    do: v |> add_to(:permissions, :delete)

  def add_permission_list(v = %__MODULE__{target_scope: :account}),
    do: v |> add_to(:permissions, :list)

  def add_permission_add(v = %__MODULE__{target_scope: :account}),
    do: v |> add_to(:permissions, :add)

  def add_permission_create(v = %__MODULE__{target_scope: :account}),
    do: v |> add_to(:permissions, :create)

  def add_permission_update(v = %__MODULE__{target_scope: :account}),
    do: v |> add_to(:permissions, :update)

  def add_permission_process(v = %__MODULE__{target_scope: :account}),
    do: v |> add_to(:permissions, :process)

  defp as_time(t), do: t |> Timex.format!("{YYYY}-{0M}-{0D}T{0h24}:{0m}:{0s}Z")

  def service_version(v = %__MODULE__{}, service_version),
    do: %{v | service_version: service_version}

  def start_time(v = %__MODULE__{}, start_time), do: %{v | start_time: start_time}
  def expiry_time(v = %__MODULE__{}, expiry_time), do: %{v | expiry_time: expiry_time}
  def resource(v = %__MODULE__{}, resource), do: %{v | resource: resource}
  def ip_range(v = %__MODULE__{}, ip_range), do: %{v | ip_range: ip_range}
  def protocol(v = %__MODULE__{}, protocol), do: %{v | protocol: protocol}

  def encode({key, value}) do
    case key do
      :service_version -> {"sv", value}
      :start_time -> {"st", value |> as_time()}
      :expiry_time -> {"se", value |> as_time()}
      :resource -> {"sr", value |> set_to_string(@resource_map)}
      :ip_range -> {"sip", value}
      :protocol -> {"spr", value}
      :services -> {"ss", value |> set_to_string(@services_map)}
      :resource_type -> {"srt", value |> set_to_string(@resource_types_map)}
      :permissions -> {"sp", value |> set_to_string(@permissions_map)}
      _ -> {nil, nil}
    end
  end

  def sign(
        sas = %__MODULE__{target_scope: target_scope},
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

    stringToSign =
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
      |> IO.inspect(label: "stringToSign")

    signature =
      :crypto.hmac(:sha256, account_key |> Base.decode64!(), stringToSign)
      |> Base.encode64()

    values
    |> Map.put("sig", signature)
    |> URI.encode_query()
  end

  def sas1() do
    new()
    |> for_storage_account()
    |> add_service_table()
    |> add_service_queue()
    |> add_service_queue()
    |> add_service_queue()
    |> add_resource_type_service()
    |> add_resource_type_object()
    |> add_permission_read()
    |> ip_range("168.1.5.60-168.1.5.70")
    # |> for_blob_service()
    |> start_time(Timex.now())
    |> expiry_time(Timex.now() |> Timex.add(Timex.Duration.from_hours(1)))
    |> protocol("https")
  end

  def demo() do
    sas1()
    |> sign(%Storage{
      cloud_environment_suffix: "core.windows.net",
      account_name: "SAMPLE_STORAGE_ACCOUNT_NAME" |> System.get_env(),
      account_key: "SAMPLE_STORAGE_ACCOUNT_KEY" |> System.get_env()
    })
    |> URI.decode_query()
  end

  def d2() do
    new()
    |> service_version(ApiVersion.get_api_version(:storage))
    |> for_storage_account()
    |> add_service_blob()
    |> add_resource_type_container()
    |> add_resource_blob_container()
    |> add_permission_read()
    |> add_permission_process()
    |> add_permission_list()
    |> start_time(Timex.now())
    |> expiry_time(Timex.now()
    |> Timex.add(Timex.Duration.from_days(100)))
    |> sign(%Storage{
          cloud_environment_suffix: "core.windows.net",
          account_name: "SAMPLE_STORAGE_ACCOUNT_NAME" |> System.get_env(),
          account_key: "SAMPLE_STORAGE_ACCOUNT_KEY" |> System.get_env()
        }
      )
  end
end
