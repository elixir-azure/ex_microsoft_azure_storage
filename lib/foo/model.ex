defmodule Foo.Model.Outer do
  @derive [Poison.Encoder]
  defstruct [:id, :inner]
end

defimpl Poison.Decoder, for: Foo.Model.Outer do
  import Foo.Deserializer
  def decode(value, options), do: value |> deserialize(:inner, :struct, Foo.Model.Inner, options)
end

defmodule Foo.Model.Inner do
  @derive [Poison.Encoder]
  defstruct [:q, :w, :e]
end

defimpl Poison.Decoder, for: Foo.Model.Inner do
  def decode(value, _options), do: value
end
