defmodule Microsoft.Azure.Storage.AzureStorageContext do
  defstruct [:account_name, :account_key, :cloud_environment_suffix]

  def blob_endpoint(context = %__MODULE__{}),
    do: "#{context.account_name}.blob.#{context.cloud_environment_suffix}"
end
