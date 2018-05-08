defmodule ExMicrosoftAzureStorage.MixProject do
  use Mix.Project

  def project do
    [
      app: :ex_microsoft_azure_storage,
      version: "0.1.0",
      elixir: "~> 1.6",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :crypto]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ibrowse, "~> 4.4"},
      {:tesla, "~> 0.8"},
      {:sweet_xml, "~> 0.6.5"}
      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"},
      # {:poison, ">= 1.0.0"},
      # {:timex, "~> 3.2"},
    ]
  end
end
