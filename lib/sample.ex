defmodule Sample do
  use Timex
  alias Microsoft.Azure.Storage.BlobStorage
  alias Microsoft.Azure.Storage.BlobPolicy
  alias Microsoft.Azure.Storage.AzureStorageContext

  def storage_context(),
    do: %AzureStorageContext{
      account_name: "SAMPLE_STORAGE_ACCOUNT_NAME" |> System.get_env(),
      account_key: "SAMPLE_STORAGE_ACCOUNT_KEY" |> System.get_env(),
      cloud_environment_suffix: "core.windows.net"
    }

  def list_containers(),
    do: storage_context() |> BlobStorage.list_containers()

  def get_blob_service_stats(),
    do: storage_context() |> BlobStorage.get_blob_service_stats()

  def create_container(container_name),
    do: storage_context() |> BlobStorage.create_container(container_name)

  def delete_container(container_name),
    do: storage_context() |> BlobStorage.delete_container(container_name)

  def list_blobs(container_name, opts \\ []),
    do: storage_context() |> BlobStorage.list_blobs(container_name, opts)

  def get_container_properties(container_name),
    do: storage_context() |> BlobStorage.get_container_properties(container_name)

  def get_container_metadata(container_name),
    do: storage_context() |> BlobStorage.get_container_metadata(container_name)

  def get_container_acl(container_name),
    do: storage_context() |> BlobStorage.get_container_acl(container_name)

  def set_container_acl_public_access_off(container_name),
    do: storage_context() |> BlobStorage.set_container_acl_public_access_off(container_name)

  def set_container_acl_public_access_blob(container_name),
    do: storage_context() |> BlobStorage.set_container_acl_public_access_blob(container_name)

  def set_container_acl_public_access_container(container_name),
    do: storage_context() |> BlobStorage.set_container_acl_public_access_container(container_name)

  def set_container_acl(container_name),
    do:
      storage_context()
      |> BlobStorage.set_container_acl(container_name, [
        %BlobPolicy{
          id: "pol1",
          start: Timex.now() |> Timex.shift(minutes: -10),
          expiry: Timex.now() |> Timex.shift(years: 1),
          permission: [:list]
        }
      ])

  def container_lease_acquire(container_name) do
    lease_duration = 16

    storage_context()
    |> BlobStorage.container_lease_acquire(container_name, lease_duration, "00000000-1111-2222-3333-444444444444")
    |> IO.inspect()

    0..lease_duration
    |> Enum.each(
      fn (i) ->
        Process.sleep(1000)

        {:ok, %{ lease_state: lease_state, lease_status: lease_status, }} = get_container_properties(container_name)

        IO.puts("#{i}: lease_state=#{lease_state} lease_status=#{lease_status}")
      end
    )
  end
end
