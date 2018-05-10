defmodule Microsoft.Azure.Storage.RestClient do
  use Tesla

  adapter(:ibrowse)

  def proxy_middleware() do
    case System.get_env("http_proxy") do
      nil ->
        []

      "" ->
        []

      proxy_cfg ->
        proxy_cfg
        |> String.split(":")
        |> (fn [host, port] ->
              [
                {Tesla.Middleware.Opts,
                 [
                   proxy_host: host |> String.to_charlist(),
                   proxy_port: port |> Integer.parse() |> elem(0)
                 ]}
              ]
            end).()
    end
  end

  def new(base_url) when is_binary(base_url) do
    Tesla.build_client([{Tesla.Middleware.BaseUrl, base_url}] ++ proxy_middleware())
  end

  def new(base_url, headers) when is_binary(base_url) and is_map(headers) do
    Tesla.build_client(
      [
        {Tesla.Middleware.BaseUrl, base_url},
        {Tesla.Middleware.Headers, headers}
      ] ++ proxy_middleware()
    )
  end
end
