defmodule ExMicrosoftAzureStorage.Storage do
  @moduledoc """
  ExMicrosoftAzureStorage.Storage
  """

  alias ExMicrosoftAzureStorage.Storage.ConnectionString

  @derive {Inspect, except: [:account_key]}
  @enforce_keys []
  defstruct account_name: nil,
            account_key: nil,
            host: nil,
            aad_token_provider: nil,
            endpoint_suffix: nil,
            default_endpoints_protocol: "https",
            is_development_factory: false

  @endpoint_names %{
    blob_service: "blob",
    queue_service: "queue",
    table_service: "table",
    file_service: "file"
  }

  @development_account_name "devstoreaccount1"
  @development_account_key "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

  @doc """
  Creates a new `ExMicrosoftAzureStorage.Storage` struct from the specified `connection_string`.

  Your particular account connection string may be found in the Azure web portal.  They take this form:

      DefaultEndpointsProtocol=https;AccountName=YOUR_ACCOUNT_NAME;AccountKey=YOUR_ACCOUNT_KEY;EndpointSuffix=core.windows.net
  """
  def new(connection_string) when is_binary(connection_string) do
    struct!(__MODULE__, ConnectionString.parse(connection_string))
  end

  @doc """
  Returns the storage context for the Azure storage emulator.
  """
  def development_factory(host \\ "127.0.0.1") do
    %__MODULE__{
      # https://docs.microsoft.com/en-us/azure/storage/common/storage-use-emulator#authenticating-requests-against-the-storage-emulator
      account_name: @development_account_name,
      account_key: @development_account_key,
      host: host,
      default_endpoints_protocol: "http",
      is_development_factory: true
    }
  end

  @doc """
  Returns the storage context for a local Azure storage emulator.
  """
  def emulator(host \\ "127.0.0.1"), do: development_factory(host)

  def secondary(%__MODULE__{is_development_factory: true} = context), do: context

  def secondary(%__MODULE__{} = context),
    do:
      context
      |> Map.update!(:account_name, &(&1 <> "-secondary"))

  def endpoint_url(%__MODULE__{is_development_factory: true, host: host} = context, service)
      when is_atom(service) do
    port =
      case service do
        :blob_service -> 10_000
        :queue_service -> 10_001
        :table_service -> 10_002
      end

    %URI{
      scheme: default_endpoints_protocol(context),
      host: host,
      port: port,
      path: "/" <> context.account_name
    }
    |> URI.to_string()
  end

  def endpoint_url(%__MODULE__{} = context, service) when is_atom(service),
    do:
      %URI{scheme: default_endpoints_protocol(context), host: endpoint_hostname(context, service)}
      |> URI.to_string()

  def endpoint_hostname(%__MODULE__{} = context, service) when is_atom(service),
    do: "#{context.account_name}.#{@endpoint_names[service]}.#{context.endpoint_suffix}"

  def default_endpoints_protocol(%__MODULE__{
        default_endpoints_protocol: default_endpoints_protocol
      }),
      do: default_endpoints_protocol
end
