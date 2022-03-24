defmodule ExMicrosoftAzureStorage.Storage.BlobStorage do
  @moduledoc """
  BlobStorage
  """

  import SweetXml
  import ExMicrosoftAzureStorage.Storage.RequestBuilder
  import ExMicrosoftAzureStorage.Storage.Utilities, only: [to_bool: 1]

  alias __MODULE__.ServiceProperties
  alias ExMicrosoftAzureStorage.Storage

  defmodule Responses do
    @moduledoc false
    import ExMicrosoftAzureStorage.Storage.RequestBuilder

    def get_blob_service_stats_response do
      [
        geo_replication: [
          ~x"/StorageServiceStats/GeoReplication",
          status: ~x"./Status/text()"s,
          last_sync_time: ~x"./LastSyncTime/text()"s
        ]
      ]
    end

    def get_blob_service_properties_response do
      [
        logging: [
          ~x"/StorageServiceProperties/Logging",
          version: ~x"./Version/text()"s,
          delete: ~x"./Delete/text()"s |> transform_by(&to_bool/1),
          read: ~x"./Read/text()"s |> transform_by(&to_bool/1),
          write: ~x"./Write/text()"s |> transform_by(&to_bool/1),
          retention_policy: [
            ~x"./RetentionPolicy",
            enabled: ~x"./Enabled/text()"s |> transform_by(&to_bool/1),
            days: ~x"./Days/text()"I
          ]
        ],
        hour_metrics: [
          ~x"/StorageServiceProperties/HourMetrics",
          version: ~x"./Version/text()"s,
          enabled: ~x"./Enabled/text()"s |> transform_by(&to_bool/1),
          include_apis: ~x"./IncludeAPIs/text()"s |> transform_by(&to_bool/1),
          retention_policy: [
            ~x"./RetentionPolicy",
            enabled: ~x"./Enabled/text()"s |> transform_by(&to_bool/1),
            days: ~x"./Days/text()"I
          ]
        ],
        minute_metrics: [
          ~x"/StorageServiceProperties/MinuteMetrics",
          version: ~x"./Version/text()"s,
          enabled: ~x"./Enabled/text()"s |> transform_by(&to_bool/1),
          include_apis: ~x"./IncludeAPIs/text()"s |> transform_by(&to_bool/1),
          retention_policy: [
            ~x"./RetentionPolicy",
            enabled: ~x"./Enabled/text()"s |> transform_by(&to_bool/1),
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
          enabled: ~x"./Enabled/text()"s |> transform_by(&to_bool/1),
          days: ~x"./Days/text()"I
        ]
      ]
    end
  end

  defmodule ServiceProperties do
    @moduledoc false

    import SweetXml
    import XmlBuilder
    import ExMicrosoftAzureStorage.Storage.RequestBuilder

    alias __MODULE__.{Logging, RetentionPolicy, Metrics, CorsRule}

    defstruct [
      :logging,
      :hour_metrics,
      :minute_metrics,
      :cors_rules,
      :default_service_version,
      :delete_retention_policy
    ]

    def to_struct(data) do
      struct(__MODULE__, data)
      |> Map.update!(:logging, &Logging.to_struct/1)
      |> Map.update!(:hour_metrics, &Metrics.to_struct/1)
      |> Map.update!(:minute_metrics, &Metrics.to_struct/1)
      |> Map.update!(:delete_retention_policy, &RetentionPolicy.to_struct/1)
      |> Map.update!(:cors_rules, &CorsRule.to_struct/1)
    end

    defmodule Logging do
      @moduledoc false
      defstruct [:version, :delete, :read, :write, :retention_policy]

      def to_struct(data) do
        struct(__MODULE__, data)
        |> Map.update!(:retention_policy, &RetentionPolicy.to_struct/1)
      end
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

    defmodule RetentionPolicy do
      @moduledoc false
      defstruct [:enabled, :days]

      def to_struct(nil), do: %__MODULE__{enabled: false, days: 0}
      def to_struct(data), do: struct(__MODULE__, data)
    end

    def xml_retention_policy(name, %{enabled: false}) when is_atom(name) do
      element({name, [{:Enabled, false}]})
    end

    def xml_retention_policy(name, %{enabled: true, days: days})
        when is_atom(name) and days > 0 and days <= 365 do
      element({name, [{:Enabled, true}, {:Days, days}]})
    end

    defmodule Metrics do
      @moduledoc false
      defstruct [:version, :enabled, :include_apis, :retention_policy]

      def to_struct(data) do
        struct(__MODULE__, data)
        |> Map.update!(:retention_policy, &RetentionPolicy.to_struct/1)
      end
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

    defmodule CorsRule do
      @moduledoc false
      defstruct [
        :max_age_in_seconds,
        :allowed_origins,
        :allowed_methods,
        :exposed_headers,
        :allowed_headers
      ]

      def to_struct(data) when is_list(data), do: data |> Enum.map(&to_struct/1)
      def to_struct(data), do: struct(__MODULE__, data)
    end

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

    def xml_blob_service_properties(%{
          # default_service_version: default_service_version,
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

    def parse(xml) do
      xml
      |> xmap(__MODULE__.storage_service_properties_parser())
      |> Map.get(:storage_service_properties)
      |> __MODULE__.to_struct()
    end

    def storage_service_properties_parser do
      [
        storage_service_properties: [
          ~x"/StorageServiceProperties",
          logging: [
            ~x"./Logging",
            version: ~x"./Version/text()"s,
            delete: ~x"./Delete/text()"s |> transform_by(&to_bool/1),
            read: ~x"./Read/text()"s |> transform_by(&to_bool/1),
            write: ~x"./Write/text()"s |> transform_by(&to_bool/1),
            retention_policy: [
              ~x"./RetentionPolicy",
              enabled: ~x"./Enabled/text()"s |> transform_by(&to_bool/1),
              days: ~x"./Days/text()"I
            ]
          ],
          hour_metrics: [
            ~x"./HourMetrics",
            version: ~x"./Version/text()"s,
            enabled: ~x"./Enabled/text()"s |> transform_by(&to_bool/1),
            include_apis: ~x"./IncludeAPIs/text()"s |> transform_by(&to_bool/1),
            retention_policy: [
              ~x"./RetentionPolicy",
              enabled: ~x"./Enabled/text()"s |> transform_by(&to_bool/1),
              days: ~x"./Days/text()"I
            ]
          ],
          minute_metrics: [
            ~x"./MinuteMetrics",
            version: ~x"./Version/text()"s,
            enabled: ~x"./Enabled/text()"s |> transform_by(&to_bool/1),
            include_apis: ~x"./IncludeAPIs/text()"s |> transform_by(&to_bool/1),
            retention_policy: [
              ~x"./RetentionPolicy",
              enabled: ~x"./Enabled/text()"s |> transform_by(&to_bool/1),
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
          # delete_retention_policy is not present in responses from Azurite (the storage simulator)
          # so we have to make this property optional with the `o` modifier passed to `~x`.
          delete_retention_policy: [
            ~x"./DeleteRetentionPolicy"o,
            enabled: ~x"./Enabled/text()"s |> transform_by(&to_bool/1),
            days: ~x"./Days/text()"I
          ]
        ]
      ]
    end
  end

  def get_blob_service_stats(%Storage{} = context) do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/get-blob-service-stats
    response =
      context
      |> Storage.secondary()
      |> new_azure_storage_request()
      |> method(:get)
      |> url("/")
      |> add_param(:query, :restype, "service")
      |> add_param(:query, :comp, "stats")
      |> sign_and_call(:blob_service)

    case response do
      %{status: status} when 400 <= status and status < 500 ->
        {:error, response |> create_error_response()}

      %{status: 200} ->
        {
          :ok,
          response.body
          |> xmap(__MODULE__.Responses.get_blob_service_stats_response())
          #  |> Map.put(:headers, response.headers)
          #  |> Map.put(:url, response.url)
          #  |> Map.put(:status, response.status)
          #  |> Map.put(:request_id, response.headers["x-ms-request-id"])
        }
    end
  end

  def get_blob_service_properties(%Storage{} = context) do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/get-blob-service-properties
    response =
      context
      |> new_azure_storage_request()
      |> method(:get)
      |> url("/")
      |> add_param(:query, :restype, "service")
      |> add_param(:query, :comp, "properties")
      |> sign_and_call(:blob_service)

    case response do
      %{status: status} when 400 <= status and status < 500 ->
        {:error, response |> create_error_response()}

      %{status: 200} ->
        {_header, request_id} = response.headers |> List.keyfind("x-ms-request-id", 0)

        {:ok,
         %{}
         |> Map.put(:service_properties, ServiceProperties.parse(response.body))
         |> Map.put(:headers, response.headers)
         |> Map.put(:url, response.url)
         |> Map.put(:status, response.status)
         |> Map.put(:request_id, request_id)}
    end
  end

  def set_blob_service_properties(%Storage{} = context, %ServiceProperties{} = service_properties) do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/set-blob-service-properties
    response =
      context
      |> new_azure_storage_request()
      |> method(:put)
      |> url("/")
      |> add_param(:query, :restype, "service")
      |> add_param(:query, :comp, "properties")
      |> add_header("Content-Type", "application/xml")
      |> body(
        service_properties
        |> ExMicrosoftAzureStorage.Storage.BlobStorage.ServiceProperties.xml_blob_service_properties()
        |> XmlBuilder.generate(format: :none)
      )
      |> sign_and_call(:blob_service)

    response
  end
end
