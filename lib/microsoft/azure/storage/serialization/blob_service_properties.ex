defmodule Microsoft.Azure.Storage.Serialization.BlobServiceProperties do
  import XmlBuilder

  defp xml_cors_rules(rules) when is_list(rules) do
    element(:Cors, rules |> Enum.map(&xml_cors_rule/1))
  end

  defp xml_cors_rule(%{
         allowed_origins: allowed_origins,
         allowed_methods: allowed_methods,
         max_age_in_seconds: max_age_in_seconds,
         exposed_headers: exposed_headers,
         allowed_headers: allowed_headers
       })
       when is_integer(max_age_in_seconds) and is_list(allowed_origins) and
              is_list(allowed_methods) and is_list(exposed_headers) and is_list(allowed_headers) do
    element(:CorsRule, [
      element(:MaxAgeInSeconds, max_age_in_seconds),
      element(:AllowedOrigins, allowed_origins |> Enum.join(",")),
      element(:AllowedMethods, allowed_methods |> Enum.join(",")),
      element(:ExposedHeaders, exposed_headers |> Enum.join(",")),
      element(:AllowedHeaders, allowed_headers |> Enum.join(","))
    ])
  end

  def xml_retention_policy(name, %{enabled: false}) when is_atom(name) do
    element({name, [{:Enabled, false}]})
  end

  def xml_retention_policy(name, %{enabled: true, days: days})
      when is_atom(name) and days > 0 and days <= 365 do
    element({name, [{:Enabled, true}, {:Days, days}]})
  end

  def xml_logging(%{
        version: version,
        delete: delete,
        read: read,
        write: write,
        retention_policy: retention_policy
      }) do
    element(
      {:Logging,
       [
         element({:Version, version}),
         element({:Delete, delete}),
         element({:Read, read}),
         element({:Write, write}),
         xml_retention_policy(:RetentionPolicy, retention_policy)
       ]}
    )
  end

  def xml_metrics(
        name,
        %{
          version: version,
          enabled: true,
          include_apis: include_apis,
          retention_policy: retention_policy
        }
      ) do
    element(
      {name,
       [
         element({:Version, version}),
         element({:Enabled, true}),
         element({:IncludeAPIs, include_apis}),
         xml_retention_policy(:RetentionPolicy, retention_policy)
       ]}
    )
  end

  def xml_metrics(
        name,
        %{
          version: version,
          enabled: false,
          retention_policy: retention_policy
        }
      ) do
    element(
      {name,
       [
         element({:Version, version}),
         element({:Enabled, false}),
         xml_retention_policy(:RetentionPolicy, retention_policy)
       ]}
    )
  end

  def xml_blob_service_properties(%{
        default_service_version: default_service_version,
        logging: logging,
        hour_metrics: hour_metrics,
        minute_metrics: minute_metrics,
        cors_rules: cors_rules,
        delete_retention_policy: delete_retention_policy
      }) do
    element({:StorageServiceProperties,
     [
       # element({:DefaultServiceVersion, default_service_version}),
       xml_logging(logging),
       xml_metrics(:HourMetrics, hour_metrics),
       xml_metrics(:MinuteMetrics, minute_metrics),
       xml_cors_rules(cors_rules),
       xml_retention_policy(:DeleteRetentionPolicy, delete_retention_policy)
     ]})
  end
end
