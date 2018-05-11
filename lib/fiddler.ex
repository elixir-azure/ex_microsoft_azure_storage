defmodule Fiddler do
  def enable(), do: "http_proxy" |> System.put_env("127.0.0.1:8888")

  def disable(), do: "http_proxy" |> System.delete_env()
end
