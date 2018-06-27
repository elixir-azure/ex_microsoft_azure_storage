defmodule Sample do
  use Timex

  alias Microsoft.Azure.Storage.{
    BlobStorage,
    BlobPolicy,
    AzureStorageContext,
    ContainerLease,
    Blob,
    CorsRule
  }

  def storage_context(),
    do: %AzureStorageContext{
      account_name: "SAMPLE_STORAGE_ACCOUNT_NAME" |> System.get_env(),
      account_key: "SAMPLE_STORAGE_ACCOUNT_KEY" |> System.get_env(),
      cloud_environment_suffix: "core.windows.net"
    }

  def upload() do
    filename = "../../../Users/chgeuer/Videos/outbreak.mp4"

    container_name = "videos"

    storage_context()
    |> BlobStorage.create_container(container_name)

    storage_context()
    |> Blob.upload_file(container_name, filename)
  end

  def get_blob_service_properties(),
    do: storage_context() |> BlobStorage.get_blob_service_properties()

  def set_blob_service_properties() do
    service_properties = %{
      logging: %{
        version: "1.0",
        delete: false,
        read: false,
        write: false,
        retention_policy: %{enabled: false }
      },
      hour_metrics: %{
        version: "1.0",
        enabled: true,
        include_apis: true,
        retention_policy: %{enabled: true, days: 365}
      },
      minute_metrics: %{
        version: "1.0",
        enabled: false,
        include_apis: false,
        retention_policy: %{enabled: false }
      },
      cors_rules: [
        %{
          allowed_origins: ["http://localhost/"],
          allowed_methods: ["GET", "PUT", "DELETE"],
          max_age_in_seconds: 120,
          exposed_headers: ["Content-Type"],
          allowed_headers: ["Content-Type"]
        }
      ],
      default_service_version: "2017-07-29",
      delete_retention_policy: %{enabled: false }
    }

    storage_context() |> BlobStorage.set_blob_service_properties(service_properties)
  end

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
    |> ContainerLease.container_lease_acquire(
      container_name,
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
    |> ContainerLease.container_lease_acquire(
      container_name,
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
    |> ContainerLease.container_lease_release(
      container_name,
      "00000000-1111-2222-3333-444444444444"
    )

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
      |> ContainerLease.container_lease_acquire(
        container_name,
        lease_duration,
        "00000000-1111-2222-3333-444444444444"
      )

    IO.puts("Acquired lease #{lease_id}")

    0..lease_duration
    |> Enum.each(fn _ ->
      Process.sleep(1000)

      storage_context()
      |> ContainerLease.container_lease_renew(container_name, lease_id)
    end)
  end

  def container_lease_break(container_name) do
    lease_duration = 60

    {:ok,
     %{
       lease_id: lease_id
     }} =
      storage_context()
      |> ContainerLease.container_lease_acquire(
        container_name,
        lease_duration,
        "00000000-1111-2222-3333-444444444444"
      )

    IO.puts("Acquired lease #{lease_id}")

    Process.sleep(1000)

    break_period = 5

    storage_context()
    |> ContainerLease.container_lease_break(container_name, lease_id, break_period)
  end

  def container_lease_acquire_and_change(container_name) do
    lease_duration = 60

    storage_context()
    |> ContainerLease.container_lease_acquire(
      container_name,
      lease_duration,
      "00000000-1111-2222-3333-444444444444"
    )

    Process.sleep(1000)

    IO.puts("Change to new lease ID ")

    storage_context()
    |> ContainerLease.container_lease_change(
      container_name,
      "00000000-1111-2222-3333-444444444444",
      "00000000-1111-2222-3333-555555555555"
    )
  end
end
