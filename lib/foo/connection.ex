defmodule Foo.Connection do
  use Tesla

  plug(Tesla.Middleware.BaseUrl, "https://httpbin.org/get")
  plug(Tesla.Middleware.Headers, %{"User-Agent" => "Elixir"})
  plug(Tesla.Middleware.EncodeJson)

  def new(token) when is_binary(token) do
    Tesla.build_client([
      {Tesla.Middleware.Headers, %{"Authorization" => "Bearer #{token}"}},
      {Tesla.Middleware.Opts, [proxy_host: '127.0.0.1', proxy_port: 8888]}
    ])
  end
end
