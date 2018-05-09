defmodule Foo.Deserializer do
  def deserialize(model, field, :list, mod, options),
    do:
      model
      |> Map.update!(field, &Poison.Decode.decode(&1, Keyword.merge(options, as: [struct(mod)])))

  def deserialize(model, field, :struct, mod, options),
    do:
      model
      |> Map.update!(field, &Poison.Decode.decode(&1, Keyword.merge(options, as: struct(mod))))

  def deserialize(model, field, :map, mod, options),
    do:
      model
      |> Map.update!(
        field,
        &Map.new(&1, fn {key, val} ->
          {key, Poison.Decode.decode(val, Keyword.merge(options, as: struct(mod)))}
        end)
      )

  def deserialize(model, field, :date, _, _options) do
    case DateTime.from_iso8601(Map.get(model, field)) do
      {:ok, datetime} ->
        Map.put(model, field, datetime)

      _ ->
        model
    end
  end
end
