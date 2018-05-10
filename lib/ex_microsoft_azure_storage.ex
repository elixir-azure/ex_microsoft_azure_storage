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

  def delete_container(container_name),
    do: storage_context() |> BlobStorage.delete_container(container_name)

  def list_containers(),
    do: storage_context() |> BlobStorage.list_containers()

  def list_blobs(container_name, opts \\ []),
    do: storage_context() |> BlobStorage.list_blobs(container_name, opts)

  def x() do
    list_blobs("philipp", maxresults: 1)
  end

  def get_container_properties(container_name),
    do: storage_context() |> BlobStorage.get_container_properties(container_name)
end
