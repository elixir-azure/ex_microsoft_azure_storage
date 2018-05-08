defmodule Microsoft.Azure.Storage.DateTimeUtils do
  defp two_digits(i) when is_integer(i) and 0 <= i and i < 10, do: "0#{i}"
  defp two_digits(i) when is_integer(i) and 10 <= i and i < 100, do: "#{i}"

  @months_names [
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec"
  ]
  defp month_name(m) when is_integer(m) and 1 <= m and m <= 12,
    do: @months_names |> Enum.at(m - 1)

  @week_day_names ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
  defp day_name(y, m, d), do: @week_day_names |> Enum.at(Calendar.ISO.day_of_week(y, m, d) - 1)

  def utc_now(), do: DateTime.utc_now() |> datetime_to_string()

  def datetime_to_string(d),
    do:
      "#{day_name(d.year, d.month, d.day)}, " <>
        "#{d.day |> two_digits()} #{d.month |> month_name()} #{d.year} " <>
        "#{d.hour |> two_digits()}:#{d.minute |> two_digits()}:#{d.second |> two_digits()} " <>
        "GMT"
end
