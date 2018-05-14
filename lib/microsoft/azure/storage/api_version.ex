defmodule Microsoft.Azure.Storage.ApiVersion do
  alias Microsoft.Azure.Storage.DateTimeUtils

  # "2015-04-05"
  def get_api_version(:storage), do: "2016-05-31"

  defstruct [:year, :month, :day]

  def parse(api_version) when is_binary(api_version),
    do: api_version |> String.graphemes() |> parse_impl

  defp parse_impl([y3, y2, y1, y0, "-", m1, m0, "-", d1, d0]),
    do: %__MODULE__{
      year: (y3 <> y2 <> y1 <> y0) |> String.to_integer(),
      month: (m1 <> m0) |> String.to_integer(),
      day: (d1 <> d0) |> String.to_integer()
    }

  def to_string(%__MODULE__{year: year, month: month, day: day}),
    do: "#{year}-#{month |> DateTimeUtils.two_digits()}-#{day |> DateTimeUtils.two_digits()}"

  def to_date(%__MODULE__{year: year, month: month, day: day}) do
    with {:ok, result} <- Date.new(year, month, day, Calendar.ISO) do
      result
    end
  end

  def compare(%__MODULE__{year: a}, %__MODULE__{year: b}) when a < b, do: :older
  def compare(%__MODULE__{year: a}, %__MODULE__{year: b}) when a > b, do: :newer
  def compare(%__MODULE__{month: a}, %__MODULE__{month: b}) when a < b, do: :older
  def compare(%__MODULE__{month: a}, %__MODULE__{month: b}) when a > b, do: :newer
  def compare(%__MODULE__{day: a}, %__MODULE__{day: b}) when a < b, do: :older
  def compare(%__MODULE__{day: a}, %__MODULE__{day: b}) when a > b, do: :newer

  def compare(%__MODULE__{year: y, month: m, day: d}, %__MODULE__{year: y, month: m, day: d}),
    do: :equal
end
