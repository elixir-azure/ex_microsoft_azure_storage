defmodule Microsoft.Azure.Storage.RestClient do
  use Tesla

  adapter(:ibrowse)

  def new(base_url) when is_binary(base_url) do
    Tesla.build_client([
      {Tesla.Middleware.BaseUrl, base_url},
      {Tesla.Middleware.Opts, [proxy_host: '127.0.0.1', proxy_port: 8888]}
    ])
    |> IO.inspect()
  end

  def new(base_url, headers) when is_binary(base_url) and is_map(headers) do
    Tesla.build_client([
      {Tesla.Middleware.BaseUrl, base_url},
      {Tesla.Middleware.Headers, headers},
      {Tesla.Middleware.Opts, [proxy_host: '127.0.0.1', proxy_port: 8888]}
    ])
  end
end
