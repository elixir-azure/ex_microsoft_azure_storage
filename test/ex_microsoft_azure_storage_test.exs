defmodule ExMicrosoftAzureStorageTest do
  use ExUnit.Case
  # doctest ExMicrosoftAzureStorage
  alias Microsoft.Azure.Storage.ApiVersion

  import SweetXml

  test "Tests HMAC SHA256" do
    # https://en.wikipedia.org/wiki/HMAC#Examples
    assert Base.encode16(
             :crypto.hmac(:sha256, "key", "The quick brown fox jumps over the lazy dog"),
             case: :lower
           ) == "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"
  end

  test "Try out SweetXml" do
    doc = """
    <?xml version="1.05" encoding="UTF-8"?>
    <game>
      <matchups>
        <matchup winner-id="1">
          <name>Match One</name>
          <teams>
            <team><id>1</id><name>Team One</name></team>
            <team><id>2</id><name>Team Two</name></team>
          </teams>
        </matchup>
        <matchup winner-id="2">
          <name>Match Two</name>
          <teams>
            <team><id>2</id><name>Team Two</name></team>
            <team><id>3</id><name>Team Three</name></team>
          </teams>
        </matchup>
        <matchup winner-id="1">
          <name>Match Three</name>
          <teams>
            <team><id>1</id><name>Team One</name></team>
            <team><id>3</id><name>Team Three</name></team>
          </teams>
        </matchup>
      </matchups>
    </game>
    """

    result = doc |> xpath(~x"//matchup/name/text()")
    assert result == 'Match One'

    result = doc |> xpath(~x"//matchup/@winner-id"l)
    assert result == ['1', '2', '1']

    result =
      doc
      |> xpath(
        ~x"//matchups/matchup"l,
        name: ~x"./name/text()",
        winner: [
          ~x".//team/id[.=ancestor::matchup/@winner-id]/..",
          name: ~x"./name/text()"
        ]
      )

    assert result == [
             %{name: 'Match One', winner: %{name: 'Team One'}},
             %{name: 'Match Two', winner: %{name: 'Team Two'}},
             %{name: 'Match Three', winner: %{name: 'Team One'}}
           ]

    result =
      doc
      |> xmap(
        matchups: [
          ~x"//matchups/matchup"l,
          name: ~x"./name/text()",
          winner: [
            ~x".//team/id[.=ancestor::matchup/@winner-id]/..",
            name: ~x"./name/text()"
          ]
        ],
        last_matchup: [
          ~x"//matchups/matchup[last()]",
          name: ~x"./name/text()",
          winner: [
            ~x".//team/id[.=ancestor::matchup/@winner-id]/..",
            name: ~x"./name/text()"
          ]
        ]
      )

    assert result == %{
             matchups: [
               %{name: 'Match One', winner: %{name: 'Team One'}},
               %{name: 'Match Two', winner: %{name: 'Team Two'}},
               %{name: 'Match Three', winner: %{name: 'Team One'}}
             ],
             last_matchup: %{name: 'Match Three', winner: %{name: 'Team One'}}
           }
  end

  test "api_version comparison" do
    old_y = "2017-01-20" |> ApiVersion.parse()
    new_y = "2018-01-20" |> ApiVersion.parse()
    old_m = "2018-01-20" |> ApiVersion.parse()
    new_m = "2018-03-20" |> ApiVersion.parse()
    old_d = "2018-03-19" |> ApiVersion.parse()
    new_d = "2018-03-20" |> ApiVersion.parse()

    assert :older == old_y |> ApiVersion.compare(new_y)
    assert :newer == new_y |> ApiVersion.compare(old_y)
    assert :older == old_m |> ApiVersion.compare(new_m)
    assert :newer == new_m |> ApiVersion.compare(old_m)
    assert :older == old_d |> ApiVersion.compare(new_d)
    assert :newer == new_d |> ApiVersion.compare(old_d)
    assert :equal == new_d |> ApiVersion.compare(new_d)
  end

  # test "ce" do
  #   "fR5pqJJzUC/H4rXDmkbQSL0JO94="
  #   |> Base.decode64!()
  #   |> Base.encode16()

  #   "7d1e69a89273502fc7e2b5c39a46d048bd093bde"
  #   |> Base.decode16!(case: :mixed)
  #   |> Base.encode64()
  # end
end
