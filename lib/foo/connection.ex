defmodule Foo.Connection do
  use Tesla

  plug Tesla.Middleware.BaseUrl, "https://management.azure.com"
  plug Tesla.Middleware.Headers, %{"User-Agent" => "Elixir"}
  plug Tesla.Middleware.EncodeJson

  def new(token) when is_binary(token) do
    Tesla.build_client([
      {Tesla.Middleware.Headers,  %{"Authorization" => "Bearer #{token}"}}
    ])
  end
end
