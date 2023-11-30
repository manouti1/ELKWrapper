using Elasticsearch.Net;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Serialization;
using Newtonsoft.Json;
using System.Text;

namespace ElasticTools
{
    /// <summary>
    /// Custom JSON serializer for Elasticsearch operations.
    /// </summary>
    public class ElasticJsonSerializer : IElasticsearchSerializer
    {
        private readonly JsonSerializerSettings _jsonSerializerSettings;

        /// <summary>
        /// Initializes a new instance of the <see cref="ElasticJsonSerializer"/> class.
        /// </summary>
        /// <param name="customConverter">Optional custom JSON converter.</param>
        public ElasticJsonSerializer(JsonConverter customConverter = null)
        {
            _jsonSerializerSettings = new JsonSerializerSettings
            {
                NullValueHandling = NullValueHandling.Ignore,
                ReferenceLoopHandling = ReferenceLoopHandling.Error,
                DateFormatHandling = DateFormatHandling.IsoDateFormat,
                DateTimeZoneHandling = DateTimeZoneHandling.Utc,
                ContractResolver = new DefaultContractResolver
                {
                    NamingStrategy = new CamelCaseNamingStrategy()
                }
            };

            _jsonSerializerSettings.Converters.Add(new StringEnumConverter());

            if (customConverter != null)
            {
                _jsonSerializerSettings.Converters.Add(customConverter);
            }
        }

        public object Deserialize(Type type, Stream stream)
        {
            return DeserializeInternal(type, stream);
        }

        public T Deserialize<T>(Stream stream)
        {
            return (T)DeserializeInternal(typeof(T), stream);
        }

        private object DeserializeInternal(Type type, Stream stream)
        {
            if (stream == null || stream.Length == 0)
                return null;

            using (var sr = new StreamReader(stream))
            using (var jtr = new JsonTextReader(sr))
            {
                try
                {
                    var serializer = JsonSerializer.Create(_jsonSerializerSettings);
                    return serializer.Deserialize(jtr, type);
                }
                catch (JsonException ex)
                {
                    throw new InvalidOperationException($"Failed to deserialize object of type {type.FullName}", ex);
                }
            }
        }

        public async Task<object> DeserializeAsync(Type type, Stream stream, CancellationToken cancellationToken = default)
        {
            return await DeserializeInternalAsync(type, stream, cancellationToken);
        }

        public async Task<T> DeserializeAsync<T>(Stream stream, CancellationToken cancellationToken = default)
        {
            return (T)await DeserializeInternalAsync(typeof(T), stream, cancellationToken);
        }

        private async Task<object> DeserializeInternalAsync(Type type, Stream stream, CancellationToken cancellationToken)
        {
            if (stream == null || stream.Length == 0)
                return null;

            using (var sr = new StreamReader(stream))
            using (var jtr = new JsonTextReader(sr))
            {
                try
                {
                    var serializer = JsonSerializer.Create(_jsonSerializerSettings);
                    return await Task.FromResult(serializer.Deserialize(jtr, type));
                }
                catch (JsonException ex)
                {
                    throw new InvalidOperationException($"Failed to deserialize object of type {type.FullName}", ex);
                }
            }
        }

        public void Serialize<T>(T data, Stream stream, SerializationFormatting formatting = SerializationFormatting.Indented)
        {
            SerializeInternal(data, stream, formatting);
        }

        public async Task SerializeAsync<T>(T data, Stream stream, SerializationFormatting formatting = SerializationFormatting.Indented, CancellationToken cancellationToken = default)
        {
            await SerializeInternalAsync(data, stream, formatting, cancellationToken);
        }

        private void SerializeInternal<T>(T data, Stream stream, SerializationFormatting formatting)
        {
            using (var sw = new StreamWriter(stream, new UTF8Encoding(false), 1024, true))
            using (var jtw = new JsonTextWriter(sw)
            {
                Formatting = formatting == SerializationFormatting.Indented ? Formatting.Indented : Formatting.None
            })
            {
                try
                {
                    var serializer = JsonSerializer.Create(_jsonSerializerSettings);
                    serializer.Serialize(jtw, data);
                    jtw.Flush();
                }
                catch (JsonException ex)
                {
                    throw new InvalidOperationException($"Failed to serialize object of type {typeof(T).FullName}", ex);
                }
            }
        }

        private async Task SerializeInternalAsync<T>(T data, Stream stream, SerializationFormatting formatting, CancellationToken cancellationToken)
        {
            using (var sw = new StreamWriter(stream, new UTF8Encoding(false), 1024, true))
            using (var jtw = new JsonTextWriter(sw)
            {
                Formatting = formatting == SerializationFormatting.Indented ? Formatting.Indented : Formatting.None
            })
            {
                try
                {
                    var serializer = JsonSerializer.Create(_jsonSerializerSettings);
                    serializer.Serialize(jtw, data);
                    await jtw.FlushAsync(cancellationToken);
                }
                catch (JsonException ex)
                {
                    throw new InvalidOperationException($"Failed to serialize object of type {typeof(T).FullName}", ex);
                }
            }
        }
    }
}
