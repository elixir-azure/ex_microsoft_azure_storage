defmodule Microsoft.Azure.Storage do
  @derive {Inspect, except: [:account_key]}
  @enforce_keys []
  defstruct [
    :account_name,
    :account_key,
    :aad_token_provider,
    :cloud_environment_suffix,
    :is_development_factory
  ]

  @endpoint_names %{
    blob_service: "blob",
    queue_service: "queue",
    table_service: "table",
    file_service: "file"
  }

  @doc """
  Returns the storage context for the Azure storage emulator.
  """
  def development_factory(),
    do: %__MODULE__{
      # https://docs.microsoft.com/en-us/azure/storage/common/storage-use-emulator#authenticating-requests-against-the-storage-emulator
      account_name: "devstoreaccount1",
      account_key:
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
      is_development_factory: true
    }

  def secondary(context = %__MODULE__{}),
    do:
      context
      |> Map.update!(:account_name, &(&1 <> "-secondary"))

  def endpoint_url(context = %__MODULE__{is_development_factory: true}, service)
      when is_atom(service) do
    port =
      case service do
        :blob_service -> 10000
        :queue_service -> 10001
        :table_service -> 10002
      end

    "http://127.0.0.1:#{port}/#{context.account_name}/"
  end

  def endpoint_url(context = %__MODULE__{}, service) when is_atom(service),
    do: "https://" <> endpoint_hostname(context, service)

  defp endpoint_hostname(context = %__MODULE__{}, service) when is_atom(service),
    do: "#{context.account_name}.#{@endpoint_names[service]}.#{context.cloud_environment_suffix}"
end
