defmodule Microsoft.Azure.Storage.BlobPolicy do
  import SweetXml

  defstruct [:id, :start, :expiry, :permission]

  defp permission(str) when is_binary(str), do: str |> permission([])
  defp permission("", perms), do: perms
  defp permission("r" <> str, perms), do: str |> permission([ :read | perms ])
  defp permission("w" <> str, perms), do: str |> permission([ :write | perms ])
  defp permission("d" <> str, perms), do: str |> permission([ :delete | perms ])
  defp permission("l" <> str, perms), do: str |> permission([ :list | perms ])
  defp permission(unknown, _perms), do: raise("Received unknown permission #{unknown}")

  def deserialize(xml_body) do
    xml_body
    |> xpath(~x"/SignedIdentifiers/SignedIdentifier"l)
    |> Enum.map(fn node ->
      %__MODULE__{
        id: node |> xpath(~x"./Id/text()"),
        start: node |> xpath(~x"./AccessPolicy/Start/text()"),
        expiry: node |> xpath(~x"./AccessPolicy/Expiry/text()"),
        permission: node |> xpath(~x"./AccessPolicy/Permission/text()"s |> transform_by(&permission/1))
      }
    end)
  end

  def serialize(policy = %__MODULE__{}) when is_map(policy),
    do: "<SignedIdentifier><Id>#{policy.id}</Id><AccessPolicy>" <>
      "<Start>#{policy.start}</Start><Expiry>#{policy.expiry}</Expiry>"<>
      "<Permission>#{policy.permission}</Permission>" <>
      "</AccessPolicy></SignedIdentifier>"

  def serialize(policies) when is_list(policies) do
    inner_xml = policies
    |> Enum.map(&serialize/1)
    |> Enum.join("")

    "<SignedIdentifiers>#{inner_xml}</SignedIdentifiers>"
  end
end
