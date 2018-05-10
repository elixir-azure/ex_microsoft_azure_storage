defmodule Microsoft.Azure.Storage.AzureStorageContext do
  defstruct [:account_name, :account_key, :cloud_environment_suffix]

  @endpoint_names %{
    blob_service: "blob",
    queue_service: "queue",
    table_service: "table",
    file_service: "file"
  }

  def secondary(context = %__MODULE__{}),
    do:
      context
      |> Map.update!(:account_name, &(&1 <> "-secondary"))

  def endpoint_url(context = %__MODULE__{}, service) when is_atom(service),
    do: "https://" <> endpoint_hostname(context, service)

  def endpoint_hostname(context = %__MODULE__{}, service) when is_atom(service),
    do: "#{context.account_name}.#{@endpoint_names[service]}.#{context.cloud_environment_suffix}"
end
