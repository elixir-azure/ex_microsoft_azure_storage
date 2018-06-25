defmodule Microsoft.Azure.Storage.CorsRule do
  require EEx

  defstruct [:allowed_origins, :allowed_methods, :max_age_in_seconds, :exposed_headers, :allowed_headers]

  @template """
  <Cors>
    <%= for cors_rule <- @cors_rules do %>
    <CorsRule>
      <AllowedOrigins><%= cors_rule.allowed_origins |> Enum.join(",") %></AllowedOrigins>
      <AllowedMethods><%= cors_rule.allowed_methods |> Enum.join(",") %></AllowedMethods>
      <MaxAgeInSeconds><%= cors_rule.max_age_in_seconds</MaxAgeInSeconds>
      <ExposedHeaders><%= cors_rule.exposed_headers |> Enum.join(",") %></ExposedHeaders>
      <AllowedHeaders><%= cors_rule.allowed_headers |> Enum.join(",") %></AllowedHeaders>
    </CorsRule><% end %>
  </Cors>
  """

  def serialize(cors_rules) when is_list(cors_rules),
    do: @template |> EEx.eval_string(assigns: [cors_rules: cors_rules])
end
