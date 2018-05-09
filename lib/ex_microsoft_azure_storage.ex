defmodule ExMicrosoftAzureStorage do
  alias Microsoft.Azure.Storage.BlobStorage
  alias Microsoft.Azure.Storage.AzureStorageContext

  defp storage_context(),
    do: %AzureStorageContext{
      account_name: "SAMPLE_STORAGE_ACCOUNT_NAME" |> System.get_env(),
      account_key: "SAMPLE_STORAGE_ACCOUNT_KEY" |> System.get_env(),
      cloud_environment_suffix: "core.windows.net"
    }

  def create_container(container_name),
    do: storage_context() |> BlobStorage.create_container(container_name)

  def list_containers(),
    do:
      BlobStorage.list_containers(
        "SAMPLE_STORAGE_ACCOUNT_NAME" |> System.get_env(),
        "SAMPLE_STORAGE_ACCOUNT_KEY" |> System.get_env()
      )

  def get_container_properties(container_name),
    do:
      BlobStorage.get_container_properties(
        "SAMPLE_STORAGE_ACCOUNT_NAME" |> System.get_env(),
        "SAMPLE_STORAGE_ACCOUNT_KEY" |> System.get_env(),
        container_name
      )
end
