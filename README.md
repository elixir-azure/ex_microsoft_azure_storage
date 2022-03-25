
Based off https://github.com/joeapearson/elixir-azure because of outdated dependencies 

# ExMicrosoftAzureStorage

A SDK to interact with Microsoft Azure Storage.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `ex_microsoft_azure_storage` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:ex_microsoft_azure_storage, "~> 1.0"},
    
    # Optional dependency, you can also add your own json_library dependency
    # and config with `config :ex_microsoft_azure_storage, json_library, YOUR_JSON_LIBRARY`
    {:jason, "~> 1.1"}
  ]
end
```
