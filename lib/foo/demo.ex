defmodule Foo.Demo do
  import Foo.RequestBuilder


  def run() do
    subscription_id = "123"
    api_version = "2018-01-01"
    connection = Foo.Connection.new("test123")

    %{}
    |> method(:get)
    |> IO.inspect()
    |> url("/subscriptions/#{subscription_id}")
    |> IO.inspect()
    |> add_param(:query, :"api-version", api_version)
    |> IO.inspect()
    |> Enum.into([])
    |> IO.inspect()
    |> (&Foo.Connection.request(connection, &1)).()
    |> IO.inspect()
    # |> decode(%Foo.Model.Outer{})
    # |> IO.inspect()





  end
end
