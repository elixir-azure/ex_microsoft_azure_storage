defmodule Microsoft.Azure.Storage.DateTimeUtils do
  use Timex

  def utc_now(),
    # https://docs.microsoft.com/en-us/rest/api/storageservices/representation-of-date-time-values-in-headers
    do:
      Timex.now()
      |> Timex.format!("{RFC1123z}")
      |> String.replace(" Z", " GMT")

  def parse_rfc1123(str),
    do:
      str
      |> Timex.parse!("{RFC1123}")

  def to_string(timex_time),
    do:
      # https://docs.microsoft.com/en-us/rest/api/storageservices/representation-of-date-time-values-in-headers
      timex_time
      |> Timex.format!("{WDshort}, {0D} {Mshort} {YYYY} {0h24}:{0m}:{0s} GMT")
end
