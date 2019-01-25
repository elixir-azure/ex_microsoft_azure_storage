defmodule Microsoft.Azure.Storage.AzureStorageContext do
  alias __MODULE__
  alias __MODULE__.Container
  alias __MODULE__.Queue

  @enforce_keys [:account_name]
  defstruct [:account_name, :account_key, :cloud_environment_suffix]

  defmodule Container do
    @enforce_keys [:storage_context, :container_name]
    defstruct [:storage_context, :container_name]
  end

  defmodule Queue do
    @enforce_keys [:storage_context, :queue_name]
    defstruct [:storage_context, :queue_name]
  end

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

  def container(storage_context = %AzureStorageContext{}, container_name) when is_binary(container_name),
     do: %Container{storage_context: storage_context, container_name: container_name}

  def queue(storage_context = %AzureStorageContext{}, queue_name) when is_binary(queue_name),
    do: %Queue{ storage_context: storage_context, queue_name: queue_name}
end
