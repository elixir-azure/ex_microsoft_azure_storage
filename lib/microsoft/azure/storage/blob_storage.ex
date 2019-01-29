defmodule Microsoft.Azure.Storage.BlobStorage do
  use NamedArgs

  import SweetXml
  import Microsoft.Azure.Storage.RequestBuilder

  alias Microsoft.Azure.Storage
  alias Microsoft.Azure.Storage.DateTimeUtils
  alias __MODULE__.ServiceProperties

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

  defmodule ServiceProperties do
    import SweetXml
    #import XmlBuilder

    alias __MODULE__.{Logging, RetentionPolicy, Metrics, CorsRule}

    defstruct [:logging, :hour_metrics, :minute_metrics, :cors_rules, :default_service_version, :delete_retention_policy]
    def to_struct(data) do
      struct(__MODULE__, data)
      |> Map.update!(:logging, &Logging.to_struct/1)
      |> Map.update!(:hour_metrics, &Metrics.to_struct/1)
      |> Map.update!(:minute_metrics, &Metrics.to_struct/1)
      |> Map.update!(:delete_retention_policy, &RetentionPolicy.to_struct/1)
    end
    defmodule Logging do
      defstruct [:version, :delete, :read, :write, :retention_policy]
      def to_struct(data) do
        struct(__MODULE__, data)
        |> Map.update!(:retention_policy, &RetentionPolicy.to_struct/1)
      end
    end
    defmodule RetentionPolicy do
      defstruct [:enabled, :days]
      def to_struct(data), do: struct(__MODULE__, data)
    end
    defmodule Metrics do
      defstruct [:version, :enabled, :include_apis, :retention_policy]
      def to_struct(data) do
        struct(__MODULE__, data)
        |> Map.update!(:retention_policy, &RetentionPolicy.to_struct/1)
      end
    end
    defmodule CorsRule do
      defstruct [:max_age_in_seconds, :allowed_origins, :allowed_methods, :exposed_headers, :allowed_headers]
      def to_struct(data), do: struct(__MODULE__, data)
    end

    def parse(xml), do:
      xml
      |> xmap(__MODULE__.storage_service_properties_parser())
      |> Map.get(:storage_service_properties)
      |> __MODULE__.to_struct()

    def storage_service_properties_parser(), do: [
      storage_service_properties: [
        ~x"/StorageServiceProperties",
        logging: [
          ~x"./Logging",
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
          ~x"./HourMetrics",
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
          ~x"./MinuteMetrics",
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
          ~x"./Cors/CorsRule"l,
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
          ~x"./DeleteRetentionPolicy",
          enabled: ~x"./Enabled/text()"s |> transform_by(&__MODULE__.to_bool/1),
          days: ~x"./Days/text()"I
        ]
      ]
    ]

    def to_bool("true"), do: true
    def to_bool("false"), do: false
    def to_bool(_), do: false
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
        #  |> Map.put(:headers, response.headers)
        #  |> Map.put(:url, response.url)
        #  |> Map.put(:status, response.status)
        #  |> Map.put(:request_id, response.headers["x-ms-request-id"])
        }
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
         %{ }
         |> Map.put(:service_properties, ServiceProperties.parse(response.body))
         |> Map.put(:headers, response.headers)
         |> Map.put(:url, response.url)
         |> Map.put(:status, response.status)
         |> Map.put(:request_id, response.headers["x-ms-request-id"])}
    end
  end

  def set_blob_service_properties(context = %Storage{}, service_properties = %ServiceProperties{}) do
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
