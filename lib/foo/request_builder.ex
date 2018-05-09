defmodule Foo.RequestBuilder do
  def method(request, m), do: request |> Map.put_new(:method, m)

  def url(request, u), do: request |> Map.put_new(:url, u)

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
    |> Map.update!(:body, &(Tesla.Multipart.add_field(&1, key, Poison.encode!(value),
      headers: [{:"Content-Type", "application/json"}])))
  end
  def add_param(request, :file, name, path) do
    request
    |> Map.put_new_lazy(:body, &Tesla.Multipart.new/0)
    |> Map.update!(:body, &(Tesla.Multipart.add_file(&1, path, name: name)))
  end
  def add_param(request, :form, name, value) do
    request
    |> Map.update(:body, %{name => value}, &(Map.put(&1, name, value)))
  end
  def add_param(request, location, key, value) do
    Map.update(request, location, [{key, value}], &(&1 ++ [{key, value}]))
  end

  def decode(%Tesla.Env{status: 200, body: body}), do: Poison.decode(body)
  def decode(response), do: {:error, response}
  def decode(%Tesla.Env{status: 200} = env, false), do: {:ok, env}
  def decode(%Tesla.Env{status: 200, body: body}, struct), do: Poison.decode(body, as: struct)
  def decode(response, _struct), do: {:error, response}
end
