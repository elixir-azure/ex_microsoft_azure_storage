defmodule ExMicrosoftAzureStorage.Storage.Utilities do
  @moduledoc """
  Utilities
  """

  @doc """
  Adds a value to a list, which is a value in a dictionary.

  ## Examples

      iex> %{foo: nil} |> ExMicrosoftAzureStorage.Storage.Utilities.add_to(:foo, :a)
      %{foo: [:a]}

      iex> %{foo: [:a]} |> ExMicrosoftAzureStorage.Storage.Utilities.add_to(:foo, :b)
      %{foo: [:b, :a]}

      iex> %{foo: [:a]} |> ExMicrosoftAzureStorage.Storage.Utilities.add_to(:foo, :b) |> ExMicrosoftAzureStorage.Storage.Utilities.add_to(:foo, :c)
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

  @doc """
  Converts a list of atoms to a representative string, based on a mapping table.

  ## Examples

      iex> [:read, :write] |> ExMicrosoftAzureStorage.Storage.Utilities.set_to_string(%{read: "r", write: "w"})
      "rw"

      iex> [:read, :write, :update] |> ExMicrosoftAzureStorage.Storage.Utilities.set_to_string(%{read: "r", write: "w", create: "c"})
      "rw"
  """
  def set_to_string(set, mapping) when is_list(set) and is_map(mapping),
    do:
      set
      |> Enum.uniq()
      |> Enum.map(&Map.get(mapping, &1))
      |> Enum.filter(&(&1 != nil))
      |> Enum.join("")

  @doc """
  Reverses a map

  ## Examples

      iex> %{read: "r", write: "w"} |> ExMicrosoftAzureStorage.Storage.Utilities.reverse_map()
      %{"r" => :read, "w" => :write}

      iex> %{"r" => :read, "w" => :write} |> ExMicrosoftAzureStorage.Storage.Utilities.reverse_map()
      %{write: "w", read: "r"}

      iex> %{"r" => :read, "w" => :write} |> ExMicrosoftAzureStorage.Storage.Utilities.reverse_map()
      %{read: "r", write: "w"}
  """
  def reverse_map(mapping),
    do: mapping |> Enum.to_list() |> Enum.map(fn {k, v} -> {v, k} end) |> Map.new()

  @doc """
  Converts a string with shortcuts back into a list of atoms.

  ## Examples

      iex> "rw" |> ExMicrosoftAzureStorage.Storage.Utilities.string_to_set(%{read: "r", write: "w", create: "c"})
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

  @doc """
  Converts a string literal "true" or "false" into appropriate boolean.

  All other values return `false`.
  """
  def to_bool("true"), do: true
  def to_bool("false"), do: false
  def to_bool(_), do: false
end
