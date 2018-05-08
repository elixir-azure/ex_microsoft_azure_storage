defmodule Microsoft.Azure.Storage.BlobClient do
  use Tesla

  adapter(:ibrowse)

  def new(base_url, headers) do
    Tesla.build_client([
      {Tesla.Middleware.BaseUrl, base_url},
      {Tesla.Middleware.Headers, headers},
      {Tesla.Middleware.Opts, [proxy_host: '127.0.0.1', proxy_port: 8888]}
    ])
  end
end
