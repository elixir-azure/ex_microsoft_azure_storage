defmodule Microsoft.Azure.Storage.RequestBuilder do
  alias Microsoft.Azure.Storage.AzureStorageContext
  alias Microsoft.Azure.Storage.RestClient

  def new_azure_storage_request, do: %{}

  def method(request, m), do: request |> Map.put_new(:method, m)

  def url(request, u), do: request |> Map.put_new(:url, u)

  def body(request, body), do: request |> Map.put(:body, body)

  # request |> Map.update!(:headers, &Map.merge(&1, headers))
  def add_header(request = %{headers: headers}, k, v) when headers != nil,
    do: request |> Map.put(:headers, headers |> Map.put(k, v))

  def add_header(request, k, v), do: request |> Map.put(:headers, %{k => v})

  def add_optional_params(request, _, []), do: request

  def add_optional_params(request, definitions, [{key, value} | tail]) do
    case definitions do
      %{^key => location} ->
        request
        |> add_param(location, key, value)
        |> add_optional_params(definitions, tail)

      _ ->
        add_optional_params(request, definitions, tail)
    end
  end

  def add_param(request, :body, :body, value), do: request |> Map.put(:body, value)

  def add_param(request, :body, key, value) do
    request
    |> Map.put_new_lazy(:body, &Tesla.Multipart.new/0)
    |> Map.update!(
      :body,
      &Tesla.Multipart.add_field(
        &1,
        key,
        Poison.encode!(value),
        headers: [{:"Content-Type", "application/json"}]
      )
    )
  end

  def add_param(request, :file, name, path) do
    request
    |> Map.put_new_lazy(:body, &Tesla.Multipart.new/0)
    |> Map.update!(:body, &(&1 |> Tesla.Multipart.add_file(path, name: name)))
  end

  def add_param(request, :form, name, value) do
    request
    |> Map.update(:body, %{name => value}, &(&1 |> Map.put(name, value)))
  end

  def add_param(request, location, key, value) do
    request
    |> Map.update(location, [{key, value}], &(&1 ++ [{key, value}]))
  end

  def add_param(request, :query, opts) when is_list(opts) do
    filtered_opts = opts |> only_non_empty_values

    new_q =
      case request[:query] do
        nil -> filtered_opts
        query -> query ++ filtered_opts
      end

    request
    |> Map.put(:query, new_q)
  end

  defp only_non_empty_values(opts) when is_list(opts),
    do:
      opts
      |> Enum.filter(fn {_, value} -> value != nil && value != "" end)
      |> Enum.into([])

  def add_storage_context(request, storage_context = %AzureStorageContext{}),
    do: request |> Map.put_new(:storage_context, storage_context)

  def add_ms_context(request, storage_context, date, api_version) do
    request
    |> add_storage_context(storage_context)
    |> add_header("x-ms-date", date)
    |> add_header("x-ms-version", api_version)
  end

  defp primary(account_name), do: account_name |> String.replace("-secondary", "")

  def add_signature(
        # https://docs.microsoft.com/en-us/rest/api/storageservices/authentication-for-the-azure-storage-services

        data = %{
          method: method,
          url: url,
          query: query,
          headers:
            headers = %{
              "x-ms-date" => x_ms_date,
              "x-ms-version" => x_ms_version
            },
          storage_context: storage_context = %AzureStorageContext{}
        }
      )
      when is_map(data) do
    canonicalizedHeaders = "x-ms-date:#{x_ms_date}" <> "\n" <> "x-ms-version:#{x_ms_version}"

    canonicalizedResource =
      "/#{storage_context.account_name |> primary()}#{url}\n" <>
        (query
         |> Enum.sort_by(& &1)
         |> Enum.map_join("\n", fn {k, v} -> "#{k}:#{v}" end))

    stringToSign =
      [
        method |> Atom.to_string() |> String.upcase(),
        headers |> Map.get("Content-Encoding"),
        headers |> Map.get("Content-Language"),
        headers |> Map.get("Content-Length"),
        headers |> Map.get("Content-MD5"),
        headers |> Map.get("ContentType"),
        headers |> Map.get("Date"),
        headers |> Map.get("ifModifiedSince"),
        headers |> Map.get("ifMatch"),
        headers |> Map.get("ifNoneMatch"),
        headers |> Map.get("ifUnmodifiedSince"),
        headers |> Map.get("Range"),
        canonicalizedHeaders,
        canonicalizedResource
      ]
      |> Enum.join("\n")

    signature =
      :crypto.hmac(:sha256, storage_context.account_key |> Base.decode64!(), stringToSign)
      |> Base.encode64()

    data
    |> add_header(
      "Authorization",
      "SharedKey #{storage_context.account_name}:#{signature}"
    )
  end

  def sign_and_call(request = %{storage_context: storage_context}, service)
      when is_atom(service) do
    connection =
      storage_context
      |> AzureStorageContext.endpoint_url(service)
      |> RestClient.new()

    request
    |> add_signature()
    |> Enum.into([])
    |> (&RestClient.request(connection, &1)).()
  end

  def decode(%Tesla.Env{status: 200, body: body}), do: Poison.decode(body)
  def decode(response), do: {:error, response}
  def decode(%Tesla.Env{status: 200} = env, false), do: {:ok, env}
  def decode(%Tesla.Env{status: 200, body: body}, struct), do: Poison.decode(body, as: struct)
  def decode(response, _struct), do: {:error, response}
end
