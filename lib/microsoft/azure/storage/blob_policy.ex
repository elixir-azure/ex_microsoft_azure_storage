defmodule Microsoft.Azure.Storage.BlobPolicy do
  import SweetXml
  require EEx

  defstruct [:id, :start, :expiry, :permission]

  def permission_serialize(list) when is_list(list), do: list |> Enum.uniq() |> perm_ser("")

  defp perm_ser([], acc), do: acc
  defp perm_ser([:read | tail], acc), do: tail |> perm_ser("r" <> acc)
  defp perm_ser([:write | tail], acc), do: tail |> perm_ser("w" <> acc)
  defp perm_ser([:delete | tail], acc), do: tail |> perm_ser("d" <> acc)
  defp perm_ser([:list | tail], acc), do: tail |> perm_ser("l" <> acc)
  # @blob_permissions %{read: "r", write: "w", delete: "d", list: "l"}
  # defp perm_ser([a | tail], acc) when is_atom(a), do: tail |> perm_ser(Map.get(@blob_permissions, a, "") <> acc)

  defp perm_ser([unknown | _tail], _acc) when is_atom(unknown),
    do: raise("Received unknown permission #{inspect(unknown)}")

  def permission_parse(str) when is_binary(str), do: str |> perm([]) |> Enum.uniq()
  defp perm("", acc), do: acc
  defp perm("r" <> str, acc), do: str |> perm([:read | acc])
  defp perm("w" <> str, acc), do: str |> perm([:write | acc])
  defp perm("d" <> str, acc), do: str |> perm([:delete | acc])
  defp perm("l" <> str, acc), do: str |> perm([:list | acc])

  defp perm(unknown, _acc) when is_binary(unknown),
    do: raise("Received unknown permission #{inspect(unknown)}")

  defp date_parse(date) do
    {:ok, result, 0} = date |> DateTime.from_iso8601()
    result
  end

  def deserialize(xml_body) do
    xml_body
    |> xpath(~x"/SignedIdentifiers/SignedIdentifier"l)
    |> Enum.map(fn node ->
      %__MODULE__{
        id: node |> xpath(~x"./Id/text()"s),
        start: node |> xpath(~x"./AccessPolicy/Start/text()"s |> transform_by(&date_parse/1)),
        expiry: node |> xpath(~x"./AccessPolicy/Expiry/text()"s |> transform_by(&date_parse/1)),
        permission:
          node
          |> xpath(~x"./AccessPolicy/Permission/text()"s |> transform_by(&permission_parse/1))
      }
    end)
  end

  @template """
  <?xml version="1.0" encoding="utf-8"?>
  <SignedIdentifiers>
    <%= for policy <- @policies do %>
    <SignedIdentifier>
      <Id><%= policy.id %></Id>
      <AccessPolicy>
        <Start><%= policy.start |> Microsoft.Azure.Storage.BlobPolicy.time_serialize() %></Start>
        <Expiry><%= policy.expiry |> Microsoft.Azure.Storage.BlobPolicy.time_serialize() %></Expiry>
        <Permission><%= policy.permission |> Microsoft.Azure.Storage.BlobPolicy.permission_serialize() %></Permission>
      </AccessPolicy>
    </SignedIdentifier>
    <% end %>
  </SignedIdentifiers>
  """

  def serialize(policies) when is_list(policies),
    do: @template |> EEx.eval_string(assigns: [policies: policies])

  # def serialize(policies) when is_list(policies) do
  #   inner_xml =
  #     policies
  #     |> Enum.map(&serialize/1)
  #     |> Enum.join("")
  #
  #   bom = "\uFEFF"
  #
  #   bom <>
  #     "<?xml version=\"1.0\" encoding=\"utf-8\"?>" <>
  #     "<SignedIdentifiers>#{inner_xml}</SignedIdentifiers>"
  # end
  #
  # def serialize(policy = %__MODULE__{}) when is_map(policy),
  #   do:
  #     "<SignedIdentifier>" <>
  #       "<Id>#{policy.id}</Id>" <>
  #       "<AccessPolicy>" <>
  #       "<Start>#{policy.start |> time_serialize()}</Start>" <>
  #       "<Expiry>#{policy.expiry |> time_serialize()}</Expiry>" <>
  #       "<Permission>#{policy.permission |> permission_serialize()}</Permission>" <>
  #       "</AccessPolicy>" <> "</SignedIdentifier>"

  # Azure expects dates in YYYY-MM-DDThh:mm:ss.fffffffTZD,
  # where fffffff is the *seven*-digit millisecond representation.
  # &DateTime.to_iso8601/1 only generates six-digit millisecond
  def time_serialize(date_time),
    do:
      date_time
      |> DateTime.to_iso8601()
      |> String.replace_trailing("Z", "0Z")
end
