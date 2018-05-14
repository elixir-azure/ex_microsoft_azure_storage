defmodule Microsoft.Azure.Storage.RequestBuilder do
  import SweetXml
  alias Microsoft.Azure.Storage.AzureStorageContext
  alias Microsoft.Azure.Storage.RestClient
  alias Microsoft.Azure.Storage.ApiVersion

  def new_azure_storage_request, do: %{}

  def method(request, m), do: request |> Map.put_new(:method, m)

  def url(request, u), do: request |> Map.put_new(:url, u)

  def body(request, body),
    do:
      request
      |> add_header("Content-Length", "#{body |> byte_size()}")
      |> Map.put(:body, body)

  def add_header_content_md5(request) do
    body = request |> Map.get(:body)
    md5 = :crypto.hash(:md5, body) |> Base.encode64()

    request
    |> add_header("Content-MD5", md5)
  end

  def add_header_if(request, false, _k, _v), do: request
  def add_header_if(request, true, k, v), do: request |> add_header(k, v)

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

  def add_param_if(request, false, _location, _key, _value), do: request

  def add_param_if(request, true, location, key, value),
    do: request |> add_param(location, key, value)

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

  def add_ms_context(request, storage_context, date, service) do
    request
    |> add_storage_context(storage_context)
    |> add_header("x-ms-date", date)
    |> add_header("x-ms-version", service |> ApiVersion.get_api_version())
  end

  defp primary(account_name), do: account_name |> String.replace("-secondary", "")

  defp canonicalized_headers(headers = %{}),
    do:
      headers
      |> Enum.into([])
      |> Enum.map(fn {k, v} -> {k |> String.downcase(), v} end)
      |> Enum.filter(fn {k, _} -> k |> String.starts_with?("x-ms-") end)
      |> Enum.sort()
      |> Enum.map(fn {k, v} -> "#{k}:#{v}" end)
      |> Enum.join("\n")

  def remove_empty_headers(request = %{headers: headers = %{}}) do
    new_headers =
      headers
      |> Enum.into([])
      |> Enum.filter(fn {_k, v} -> v != nil && String.length(v) > 0 end)
      |> Enum.into(%{})

    request
    |> Map.put(:headers, new_headers)
  end

  defp get_header(headers, name) do
    headers
    |> Map.get(name)
  end

  def add_signature(
        # https://docs.microsoft.com/en-us/rest/api/storageservices/authentication-for-the-azure-storage-services

        data = %{
          method: method,
          url: url,
          query: query,
          headers: headers = %{},
          storage_context: storage_context = %AzureStorageContext{}
        }
      )
      when is_map(data) do
    canonicalizedHeaders = headers |> canonicalized_headers()

    canonicalizedResource =
      "/#{storage_context.account_name |> primary()}#{url}\n" <>
        (query
         |> Enum.sort_by(& &1)
         |> Enum.map_join("\n", fn {k, v} -> "#{k}:#{v}" end))

    stringToSign =
      [
        method |> Atom.to_string() |> String.upcase(),
        headers |> get_header("Content-Encoding"),
        headers |> get_header("Content-Language"),
        headers |> get_header("Content-Length"),
        headers |> get_header("Content-MD5"),
        headers |> get_header("Content-Type"),
        headers |> get_header("Date"),
        headers |> get_header("If-Modified-Since"),
        headers |> get_header("If-Match"),
        headers |> get_header("If-None-Match"),
        headers |> get_header("If-Unmodified-Since"),
        headers |> get_header("Range"),
        canonicalizedHeaders,
        canonicalizedResource
      ]
      |> Enum.join("\n")

    # |> IO.inspect()

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
    |> remove_empty_headers()
    |> add_signature()
    |> Enum.into([])
    |> (&RestClient.request(connection, &1)).()
  end

  def decode(%Tesla.Env{status: 200, body: body}), do: Poison.decode(body)
  def decode(response), do: {:error, response}
  def decode(%Tesla.Env{status: 200} = env, false), do: {:ok, env}
  def decode(%Tesla.Env{status: 200, body: body}, struct), do: Poison.decode(body, as: struct)
  def decode(response, _struct), do: {:error, response}

  def create_error_response(response = %{}) do
    {:error,
     response.body
     |> xmap(
       code: ~x"/Error/Code/text()"s,
       message: ~x"/Error/Message/text()"s,
       authentication_error_detail: ~x"/Error/AuthenticationErrorDetail/text()"s,
       query_parameter_name: ~x"/Error/QueryParameterName/text()"s,
       query_parameter_value: ~x"/Error/QueryParameterValue/text()"s
     )
     |> Map.update!(:message, &String.split(&1, "\n"))
     |> Map.put(:http_status, response.status)
     |> Map.put(:url, response.url)
     |> Map.put(:body, response.body)
     |> Map.put(:request_id, response.headers["x-ms-request-id"])}
  end
end
