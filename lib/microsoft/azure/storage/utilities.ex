defmodule Microsoft.Azure.Storage.Utilities do
  @doc ~S"""
  Adds a value to a list, which is a value in a dictionary.

  ## Examples

      iex> %{foo: nil} |> Microsoft.Azure.Storage.Utilities.add_to(:foo, :a)
      %{foo: [:a]}

      iex> %{foo: [:a]} |> Microsoft.Azure.Storage.Utilities.add_to(:foo, :b)
      %{foo: [:b, :a]}

      iex> %{foo: [:a]} |> Microsoft.Azure.Storage.Utilities.add_to(:foo, :b) |> Microsoft.Azure.Storage.Utilities.add_to(:foo, :c)
      %{foo: [:c, :b, :a]}
  """
  def add_to(v = %{}, key, value) when is_atom(key) and is_atom(value),
    do:
      v
      |> Map.update(
        key,
        value,
        &case &1 do
          nil -> [value]
          a -> [value | a] |> Enum.uniq()
        end
      )

  @doc ~S"""
  Converts a list of atoms to a representative string, based on a mapping table.

  ## Examples

      iex> [:read, :write] |> Microsoft.Azure.Storage.Utilities.set_to_string(%{read: "r", write: "w"})
      "rw"

      iex> [:read, :write, :update] |> Microsoft.Azure.Storage.Utilities.set_to_string(%{read: "r", write: "w", create: "c"})
      "rw"
  """
  def set_to_string(set, mapping) when is_list(set) and is_map(mapping),
    do:
      set
      |> Enum.uniq()
      |> Enum.map(&Map.get(mapping, &1))
      |> Enum.filter(&(&1 != nil))
      |> Enum.join("")

  @doc ~S"""
  Reverses a map

  ## Examples

      iex> %{read: "r", write: "w"} |> Microsoft.Azure.Storage.Utilities.reverse_map()
      %{"r" => :read, "w" => :write}

      iex> %{"r" => :read, "w" => :write} |> Microsoft.Azure.Storage.Utilities.reverse_map()
      %{write: "w", read: "r"}

      iex> %{"r" => :read, "w" => :write} |> Microsoft.Azure.Storage.Utilities.reverse_map()
      %{read: "r", write: "w"}
  """
  def reverse_map(mapping),
    do: mapping |> Enum.to_list() |> Enum.map(fn {k, v} -> {v, k} end) |> Map.new()

  @doc ~S"""
  Converts a string with shortcuts back into a list of atoms.

  ## Examples

      iex> "rw" |> Microsoft.Azure.Storage.Utilities.string_to_set(%{read: "r", write: "w", create: "c"})
      [:read, :write]
  """
  def string_to_set(string, mapping) when is_binary(string) and is_map(mapping) do
    reverse_mapping = mapping |> reverse_map()

    string
    |> String.graphemes()
    |> Enum.uniq()
    |> Enum.map(&Map.get(reverse_mapping, &1))
    |> Enum.filter(&(&1 != nil))
    |> Enum.to_list()
  end
end
