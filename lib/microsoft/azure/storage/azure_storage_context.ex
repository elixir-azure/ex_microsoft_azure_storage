defmodule Microsoft.Azure.Storage.AzureStorageContext do
  defstruct [:account_name, :account_key, :cloud_environment_suffix]

  def endpoint_url(context = %__MODULE__{}, service) when is_atom(service),
    do: "https://" <> endpoint_hostname(context, service)

  def endpoint_hostname(context = %__MODULE__{}, :blob_service),
    do: context |> endpoint_hostname("blob") |> IO.inspect()

  def endpoint_hostname(context = %__MODULE__{}, :table_service),
    do: context |> endpoint_hostname("table")

  def endpoint_hostname(context = %__MODULE__{}, :queue_service),
    do: context |> endpoint_hostname("queue")

  def endpoint_hostname(context = %__MODULE__{}, :file_service),
    do: context |> endpoint_hostname("file")

  def endpoint_hostname(context = %__MODULE__{}, service_name) when is_binary(service_name),
    do: "#{context.account_name}.#{service_name}.#{context.cloud_environment_suffix}"
end
