defmodule Microsoft.Azure.Storage.BlobStorage do
  use NamedArgs

  import SweetXml
  import Microsoft.Azure.Storage.RequestBuilder

  alias Microsoft.Azure.Storage
  alias Microsoft.Azure.Storage.DateTimeUtils

  defmodule Responses do
    def to_bool("true"), do: true
    def to_bool("false"), do: false
    def to_bool(_), do: false

    def get_blob_service_stats_response(),
      do: [
        geo_replication: [
          ~x"/StorageServiceStats/GeoReplication",
          status: ~x"./Status/text()"s,
          last_sync_time: ~x"./LastSyncTime/text()"s
        ]
      ]

    def get_blob_service_properties_response(),
      do: [
        logging: [
          ~x"/StorageServiceProperties/Logging",
          version: ~x"./Version/text()"s,
          delete: ~x"./Delete/text()"s |> transform_by(&__MODULE__.to_bool/1),
          read: ~x"./Read/text()"s |> transform_by(&__MODULE__.to_bool/1),
          write: ~x"./Write/text()"s |> transform_by(&__MODULE__.to_bool/1),
          retention_policy: [
            ~x"./RetentionPolicy",
            enabled: ~x"./Enabled/text()"s |> transform_by(&__MODULE__.to_bool/1),
            days: ~x"./Days/text()"I
          ]
        ],
        hour_metrics: [
          ~x"/StorageServiceProperties/HourMetrics",
          version: ~x"./Version/text()"s,
          enabled: ~x"./Enabled/text()"s |> transform_by(&__MODULE__.to_bool/1),
          include_apis: ~x"./IncludeAPIs/text()"s |> transform_by(&__MODULE__.to_bool/1),
          retention_policy: [
            ~x"./RetentionPolicy",
            enabled: ~x"./Enabled/text()"s |> transform_by(&__MODULE__.to_bool/1),
            days: ~x"./Days/text()"I
          ]
        ],
        minute_metrics: [
          ~x"/StorageServiceProperties/MinuteMetrics",
          version: ~x"./Version/text()"s,
          enabled: ~x"./Enabled/text()"s |> transform_by(&__MODULE__.to_bool/1),
          include_apis: ~x"./IncludeAPIs/text()"s |> transform_by(&__MODULE__.to_bool/1),
          retention_policy: [
            ~x"./RetentionPolicy",
            enabled: ~x"./Enabled/text()"s |> transform_by(&__MODULE__.to_bool/1),
            days: ~x"./Days/text()"I
          ]
        ],
        cors_rules: [
          ~x"/StorageServiceProperties/Cors/CorsRule"l,
          max_age_in_seconds: ~x"./MaxAgeInSeconds/text()"I,
          allowed_origins:
            ~x"./AllowedOrigins/text()"s |> transform_by(&(&1 |> String.split(","))),
          allowed_methods:
            ~x"./AllowedMethods/text()"s |> transform_by(&(&1 |> String.split(","))),
          exposed_headers:
            ~x"./ExposedHeaders/text()"s |> transform_by(&(&1 |> String.split(","))),
          allowed_headers:
            ~x"./AllowedHeaders/text()"s |> transform_by(&(&1 |> String.split(",")))
        ],
        default_service_version: ~x"/StorageServiceProperties/DefaultServiceVersion/text()"s,
        delete_retention_policy: [
          ~x"/StorageServiceProperties/DeleteRetentionPolicy",
          enabled: ~x"./Enabled/text()"s |> transform_by(&__MODULE__.to_bool/1),
          days: ~x"./Days/text()"I
        ]
      ]
  end

  def get_blob_service_stats(context = %Storage{}) do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/get-blob-service-stats
    response =
      new_azure_storage_request()
      |> method(:get)
      |> url("/")
      |> add_param(:query, :restype, "service")
      |> add_param(:query, :comp, "stats")
      |> add_ms_context(
        context |> Storage.secondary(),
        DateTimeUtils.utc_now(),
        :storage
      )
      |> sign_and_call(:blob_service)

    case response do
      %{status: status} when 400 <= status and status < 500 ->
        response |> create_error_response()

      %{status: 200} ->
        {:ok,
         response.body
         |> xmap(__MODULE__.Responses.get_blob_service_stats_response())
         |> Map.put(:headers, response.headers)
         |> Map.put(:url, response.url)
         |> Map.put(:status, response.status)
         |> Map.put(:request_id, response.headers["x-ms-request-id"])}
    end
  end

  def get_blob_service_properties(context = %Storage{}) do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/get-blob-service-properties
    response =
      new_azure_storage_request()
      |> method(:get)
      |> url("/")
      |> add_param(:query, :restype, "service")
      |> add_param(:query, :comp, "properties")
      |> add_ms_context(context, DateTimeUtils.utc_now(), :storage)
      |> sign_and_call(:blob_service)

    case response do
      %{status: status} when 400 <= status and status < 500 ->
        response |> create_error_response()

      %{status: 200} ->
        {:ok,
         response.body
         |> xmap(__MODULE__.Responses.get_blob_service_properties_response())
         |> Map.put(:headers, response.headers)
         |> Map.put(:url, response.url)
         |> Map.put(:status, response.status)
         |> Map.put(:request_id, response.headers["x-ms-request-id"])}
    end
  end

  def set_blob_service_properties(context = %Storage{}, service_properties) do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/set-blob-service-properties
    response =
      new_azure_storage_request()
      |> method(:put)
      |> url("/")
      |> add_param(:query, :restype, "service")
      |> add_param(:query, :comp, "properties")
      |> add_header("Content-Type", "application/xml")
      |> body(
        service_properties
        |> Microsoft.Azure.Storage.Serialization.BlobServiceProperties.xml_blob_service_properties()
        |> XmlBuilder.generate(format: :none)
      )
      |> add_ms_context(context, DateTimeUtils.utc_now(), :storage)
      |> sign_and_call(:blob_service)

    response
  end
end
