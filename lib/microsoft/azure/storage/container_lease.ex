defmodule Microsoft.Azure.Storage.ContainerLease do
  import Microsoft.Azure.Storage.RequestBuilder
  alias Microsoft.Azure.Storage.AzureStorageContext

  # "x-ms-lease-action" acquire/renew/change/release/break
  # "x-ms-lease-id"     Required for renew/change/release
  # "x-ms-lease-break-period"  optional 0..60
  # "x-ms-lease-duration" required for acquire. -1, 15..60
  # "x-ms-proposed-lease-id" Optional for acquire, required for change
  defp container_lease_handler(
         context = %AzureStorageContext{},
         container_name,
         expected_status_code,
         fn_prepare_request,
         fn_prepare_response
       ) do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/lease-container

    response =
      new_azure_storage_request()
      |> method(:put)
      |> url("/#{container_name}")
      |> add_param(:query, :comp, "lease")
      |> add_param(:query, :restype, "container")
      |> fn_prepare_request.()
      |> add_ms_context(context, Microsoft.Azure.Storage.DateTimeUtils.utc_now(), :storage)
      |> sign_and_call(:blob_service)

    case response do
      %{status: status} when 400 <= status and status < 500 ->
        response |> create_error_response()

      %{status: ^expected_status_code} ->
        {:ok,
         %{
           headers: response.headers,
           url: response.url,
           status: response.status,
           request_id: response.headers["x-ms-request-id"],
           lease_id: response.headers["x-ms-lease-id"]
         }
         |> fn_prepare_response.(response)}
    end
  end

  defp pass_result_as_is(result, _response), do: result

  # AcquireLease TimeSpan? leaseTime, string proposedLeaseId
  def container_lease_acquire(
        context = %AzureStorageContext{},
        container_name,
        lease_duration,
        proposed_lease_id \\ nil
      )
      when lease_duration |> is_integer() and (lease_duration == -1 or lease_duration in 15..60) do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/lease-container

    fn_prepare_request = fn request ->
      request
      |> add_header("x-ms-lease-action", "acquire")
      |> add_header("x-ms-lease-duration", "#{lease_duration}")
      |> add_header("x-ms-proposed-lease-id", "#{proposed_lease_id}")
    end

    context
    |> container_lease_handler(
      container_name,
      201,
      fn_prepare_request,
      &pass_result_as_is/2
    )
  end

  # RenewLease
  def container_lease_renew(
        context = %AzureStorageContext{},
        container_name,
        lease_id
      ) do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/lease-container

    fn_prepare_request = fn request ->
      request
      |> add_header("x-ms-lease-action", "renew")
      |> add_header("x-ms-lease-id", "#{lease_id}")
    end

    context
    |> container_lease_handler(
      container_name,
      200,
      fn_prepare_request,
      &pass_result_as_is/2
    )
  end

  # BreakLease   TimeSpan? breakPeriod
  def container_lease_break(
        context = %AzureStorageContext{},
        container_name,
        lease_id,
        break_period \\ -1
      )
      when break_period |> is_integer() and break_period in -1..60 do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/lease-container

    fn_prepare_request = fn request ->
      request
      |> add_header("x-ms-lease-action", "break")
      |> add_header("x-ms-lease-id", "#{lease_id}")
      |> add_header("x-ms-lease-break-period", "#{break_period}")
    end

    fn_prepare_response = fn result, response ->
      result
      |> Map.put(:lease_time, response.headers["x-ms-lease-time"] |> Integer.parse() |> elem(0))
    end

    context
    |> container_lease_handler(
      container_name,
      202,
      fn_prepare_request,
      fn_prepare_response
    )
  end

  # ReleaseLease
  def container_lease_release(
        context = %AzureStorageContext{},
        container_name,
        lease_id
      ) do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/lease-container#remarks

    fn_prepare_request = fn request ->
      request
      |> add_header("x-ms-lease-action", "release")
      |> add_header("x-ms-lease-id", "#{lease_id}")
    end

    context
    |> container_lease_handler(
      container_name,
      200,
      fn_prepare_request,
      &pass_result_as_is/2
    )
  end

  # ChangeLease string proposedLeaseId,
  def container_lease_change(
        context = %AzureStorageContext{},
        container_name,
        lease_id,
        proposed_lease_id
      ) do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/lease-container#remarks

    fn_prepare_request = fn request ->
      request
      |> add_header("x-ms-lease-action", "release")
      |> add_header("x-ms-lease-id", "#{lease_id}")
      |> add_header("x-ms-proposed-lease-id", "#{proposed_lease_id}")
    end

    context
    |> container_lease_handler(
      container_name,
      200,
      fn_prepare_request,
      &pass_result_as_is/2
    )
  end
end
