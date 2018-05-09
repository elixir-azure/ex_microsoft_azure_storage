defmodule Foo.Api do
  alias Foo.Connection
  import Foo.RequestBuilder

  def subscriptions_get(connection, subscription_id, api_version, _opts \\ []) do
    %{}
    |> method(:get)
    |> url("/subscriptions/#{subscription_id}")
    |> add_param(:query, :"api-version", api_version)
    |> Enum.into([])
    |> (&Connection.request(connection, &1)).()
    |> decode(%Foo.Model.Outer{})
  end
end
