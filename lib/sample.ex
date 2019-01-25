defmodule Sample do
  use Timex

  alias Microsoft.Azure.Storage.{
    BlobStorage,
    BlobPolicy,
    AzureStorageContext,
    AzureStorageContext.Container,
    ContainerLease,
    Blob,
    CorsRule
  }

  import XmlBuilder

  def person(id, first, last) do
    element(:person, %{id: id}, [
      element(:first, first),
      element(:last, last)
    ])
  end

  def storage_context(),
    do: %AzureStorageContext{
      account_name: "SAMPLE_STORAGE_ACCOUNT_NAME" |> System.get_env(),
      account_key: "SAMPLE_STORAGE_ACCOUNT_KEY" |> System.get_env(),
      cloud_environment_suffix: "core.windows.net"
    }

  def upload() do
    filename = "C:/Users/chgeuer/Desktop/files/magazines/capital 2018-09.pdf"
    # ""../../../Users/chgeuer/Videos/outbreak.mp4"

    container =
      storage_context()
      |> AzureStorageContext.container("videos")

    container
    |> BlobStorage.create_container()

    container
    |> Blob.upload_file(filename)
  end

  def get_blob_service_properties(),
    do: storage_context() |> BlobStorage.get_blob_service_properties()

  def re_set_blob_service_properties() do
    props =
      storage_context()
      |> BlobStorage.get_blob_service_properties()
      |> elem(1)

    storage_context()
    |> BlobStorage.set_blob_service_properties(props)
  end

  def list_containers(),
    do:
      storage_context()
      |> BlobStorage.list_containers()

  def get_blob_service_stats(),
    do: storage_context() |> BlobStorage.get_blob_service_stats()

  def create_container(container_name),
    do:
      storage_context()
      |> AzureStorageContext.container(container_name)
      |> BlobStorage.create_container()

  def delete_container(container_name),
    do:
      storage_context()
      |> AzureStorageContext.container(container_name)
      |> BlobStorage.delete_container()

  def list_blobs(container_name, opts \\ []),
    do:
      storage_context()
      |> AzureStorageContext.container(container_name)
      |> BlobStorage.list_blobs(opts)

  def get_container_properties(container_name),
    do:
      storage_context()
      |> AzureStorageContext.container(container_name)
      |> BlobStorage.get_container_properties()

  def get_container_metadata(container_name),
    do:
      storage_context()
      |> AzureStorageContext.container(container_name)
      |> BlobStorage.get_container_metadata()

  def get_container_acl(container_name),
    do:
      storage_context()
      |> AzureStorageContext.container(container_name)
      |> BlobStorage.get_container_acl()

  def set_container_acl_public_access_off(container_name),
    do:
      storage_context()
      |> AzureStorageContext.container(container_name)
      |> BlobStorage.set_container_acl_public_access_off()

  def set_container_acl_public_access_blob(container_name),
    do:
      storage_context()
      |> AzureStorageContext.container(container_name)
      |> BlobStorage.set_container_acl_public_access_blob()

  def set_container_acl_public_access_container(container_name),
    do:
      storage_context()
      |> AzureStorageContext.container(container_name)
      |> BlobStorage.set_container_acl_public_access_container()

  def set_container_acl(container_name),
    do:
      storage_context()
      |> AzureStorageContext.container(container_name)
      |> BlobStorage.set_container_acl([
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
    |> AzureStorageContext.container(container_name)
    |> ContainerLease.container_lease_acquire(
      lease_duration,
      "00000000-1111-2222-3333-444444444444"
    )

    0..lease_duration
    |> Enum.each(fn i ->
      Process.sleep(1000)

      {:ok, %{lease_state: lease_state, lease_status: lease_status}} =
        get_container_properties(container_name)

      IO.puts("#{i}: lease_state=#{lease_state} lease_status=#{lease_status}")
    end)
  end

  def container_lease_release(container_name) do
    lease_duration = 60

    storage_context()
    |> AzureStorageContext.container(container_name)
    |> ContainerLease.container_lease_acquire(
      lease_duration,
      "00000000-1111-2222-3333-444444444444"
    )

    0..3
    |> Enum.each(fn i ->
      Process.sleep(200)

      {:ok, %{lease_state: lease_state, lease_status: lease_status}} =
        get_container_properties(container_name)

      IO.puts("#{i}: lease_state=#{lease_state} lease_status=#{lease_status}")
    end)

    IO.puts("Call release now")

    storage_context()
    |> AzureStorageContext.container(container_name)
    |> ContainerLease.container_lease_release("00000000-1111-2222-3333-444444444444")

    0..3
    |> Enum.each(fn i ->
      Process.sleep(200)

      {:ok, %{lease_state: lease_state, lease_status: lease_status}} =
        get_container_properties(container_name)

      IO.puts("#{i}: lease_state=#{lease_state} lease_status=#{lease_status}")
    end)
  end

  def container_lease_renew(container_name) do
    lease_duration = 16

    {:ok,
     %{
       lease_id: lease_id
     }} =
      storage_context()
      |> AzureStorageContext.container(container_name)
      |> ContainerLease.container_lease_acquire(
        lease_duration,
        "00000000-1111-2222-3333-444444444444"
      )

    IO.puts("Acquired lease #{lease_id}")

    0..lease_duration
    |> Enum.each(fn _ ->
      Process.sleep(1000)

      storage_context()
      |> AzureStorageContext.container(container_name)
      |> ContainerLease.container_lease_renew(lease_id)
    end)
  end

  def container_lease_break(container_name) do
    lease_duration = 60

    {:ok,
     %{
       lease_id: lease_id
     }} =
      storage_context()
      |> AzureStorageContext.container(container_name)
      |> ContainerLease.container_lease_acquire(
        lease_duration,
        "00000000-1111-2222-3333-444444444444"
      )

    IO.puts("Acquired lease #{lease_id}")

    Process.sleep(1000)

    break_period = 5

    storage_context()
    |> AzureStorageContext.container(container_name)
    |> ContainerLease.container_lease_break(lease_id, break_period)
  end

  def container_lease_acquire_and_change(container_name) do
    lease_duration = 60

    storage_context()
    |> AzureStorageContext.container(container_name)
    |> ContainerLease.container_lease_acquire(
      lease_duration,
      "00000000-1111-2222-3333-444444444444"
    )

    Process.sleep(1000)

    IO.puts("Change to new lease ID ")

    storage_context()
    |> AzureStorageContext.container(container_name)
    |> ContainerLease.container_lease_change(
      "00000000-1111-2222-3333-444444444444",
      "00000000-1111-2222-3333-555555555555"
    )
  end
end
