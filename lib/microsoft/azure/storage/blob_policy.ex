defmodule Microsoft.Azure.Storage.BlobPolicy do
  import SweetXml

  defstruct [:id, :start, :expiry, :permission]

  def deserialize(xml_body) do
    xml_body
    |> xpath(~x"/SignedIdentifiers/SignedIdentifier"l)
    |> Enum.map(fn node ->
      %__MODULE__{
        id: node |> xpath(~x"./Id/text()"),
        start: node |> xpath(~x"./AccessPolicy/Start/text()"),
        expiry: node |> xpath(~x"./AccessPolicy/Expiry/text()"),
        permission: node |> xpath(~x"./AccessPolicy/Permission/text()")
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
