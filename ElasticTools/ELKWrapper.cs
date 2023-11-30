using System.Diagnostics;
using System.Text;
using Nest;
using Newtonsoft.Json;
using System.Reflection;
using Elasticsearch.Net;

namespace ElasticTools
{
    /// <summary>
    /// Provides a wrapper for interacting with Elasticsearch using NEST.
    /// </summary>
    /// <typeparam name="T">The type of documents stored in Elasticsearch.</typeparam>

    public class ELKWrapper<T> : IELKWrapper<T> where T : class
    {
        private readonly IElasticClient _client;
        private readonly string _aliasName;
        private readonly string _indexName;
        private readonly ElasticSearchSettings _settings;

        /// <summary>
        /// Initializes a new instance of the <see cref="ELKWrapper{T}"/> class.
        /// </summary>
        /// <param name="settingsAccessor">The settings accessor for Elasticsearch.</param>
        /// <param name="indexName">The name of the index.</param>
        /// <param name="aliasName">The name of the alias. If not provided, it defaults to the index name.</param>
        /// <param name="customSerializer">A function to provide a custom JSON serializer for the type.</param>

        public ELKWrapper(
            ElasticSearchSettings settingsAccessor,
             string indexName,
             string aliasName = null,
            Func<Type, JsonConverter> customSerializer = null)
        {
            try
            {
                _settings = settingsAccessor;
                _aliasName = string.IsNullOrEmpty(aliasName) ? indexName : aliasName;
                _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
                // Create the connection pool based on the alias
                IConnectionPool pool;
                if (_settings.SingleNode)
                {
                    pool = new SingleNodeConnectionPool(new Uri(_settings.Url));
                }
                else
                {
                    // Assuming the URI contains a comma-separated list of nodes for the StaticConnectionPool
                    var nodes = _settings.Url.Split(',').Select(url => new Uri(url)).ToArray();
                    pool = new StaticConnectionPool(nodes);
                }

                var settings = new ConnectionSettings(pool, (builtin, s) =>
                {
                    var serializer = customSerializer?.Invoke(typeof(T));

                    if (serializer != null && serializer is ElasticJsonSerializer)
                    {
                        return new ElasticJsonSerializer(serializer);
                    }

                    return new ElasticJsonSerializer();
                })
                .BasicAuthentication(_settings.Username, _settings.Password)
                .ServerCertificateValidationCallback((o, certificate, chain, errors) => true)
                .DefaultIndex(_aliasName) // Use the alias as the default index
                .DefaultMappingFor<T>(m => m
                    .IndexName(_indexName)
                )
                .DisableDirectStreaming()
                .PrettyJson()
                .DefaultFieldNameInferrer(p => char.ToLowerInvariant(p[0]) + p.Substring(1))
                .OnRequestCompleted(apiCallDetails =>
                {
                    Debug.WriteLine(apiCallDetails.Uri + "\n" +
                                    (apiCallDetails.RequestBodyInBytes != null
                                        ? Encoding.UTF8.GetString(apiCallDetails.RequestBodyInBytes)
                                        : ""));
                });

                _client = new ElasticClient(settings);

                EnsureMappingIsCorrect(indexName);
            }
            catch (Exception ex)
            {
                // Log or handle the exception as needed
                Console.WriteLine($"Exception during Elasticsearch initialization: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Indexes a document in Elasticsearch.
        /// </summary>
        /// <param name="document">The document to be indexed.</param>
        /// <returns>The generated ID of the indexed document.</returns>
        public string IndexDocument(T document)
        {
            try
            {
                // Note: Do not specify Id when indexing, let Elasticsearch generate it
                var indexResponse = _client.Index(document, i => i
                    .Index(_indexName)
                    .Refresh(Refresh.WaitFor)
                );

                if (!indexResponse.IsValid || indexResponse.OriginalException != null)
                {
                    throw new Exception($"Failed to index document. Debug information: {indexResponse.DebugInformation}");
                }

                // Get the generated _id
                var documentId = indexResponse.Id;

                // Return the generated _id
                return documentId;
            }
            catch (Exception ex)
            {
                // Log or handle the exception as needed
                Console.WriteLine($"Exception during document indexing: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Performs a scroll search in Elasticsearch.
        /// </summary>
        /// <param name="queryFunc">Function to define the query.</param>
        /// <param name="sortFunc">Function to define sorting.</param>
        /// <param name="aggregationFunc">Function to define aggregations.</param>
        /// <param name="pageSize">Number of documents per page.</param>
        /// <param name="pageIndex">Index of the page.</param>
        /// <param name="scrollTime">Time to keep the scroll cursor open.</param>
        /// <returns>A list of documents matching the query.</returns>
        public async Task<IList<T>> ScrollSearchAsync(
             Func<QueryContainerDescriptor<T>, QueryContainer> queryFunc,
             Func<SortDescriptor<T>, IPromise<IList<ISort>>> sortFunc,
             Func<AggregationContainerDescriptor<T>, IAggregationContainer> aggregationFunc = null,
             int pageSize = 1000,
             int pageIndex = 0,
             string scrollTime = "1m")
        {
            try
            {
                var searchDescriptor = new SearchDescriptor<T>()
                    .Index(_indexName)
                    .Sort(sortFunc)
                    .From(pageIndex * pageSize)
                    .Size(pageSize)
                    .Query(q => queryFunc(q));

                // Include all fields if none are specified
                var fieldsToInclude = GetFieldNamesFromAttributes<T>().ToArray();

                if (fieldsToInclude.Any())
                {
                    searchDescriptor.Source(sf => sf.Includes(i => i.Fields(fieldsToInclude)));
                }

                if (aggregationFunc != null)
                {
                    searchDescriptor.Aggregations(aggs => aggregationFunc(aggs));
                }

                var initialSearchResponse = await _client.SearchAsync<T>(searchDescriptor.Scroll(scrollTime));

                if (!initialSearchResponse.IsValid || initialSearchResponse.OriginalException != null)
                {
                    HandleElasticsearchException(initialSearchResponse, "Initial search");
                }

                var documents = new List<T>();
                var scrollId = initialSearchResponse.ScrollId;

                while (initialSearchResponse.IsValid && initialSearchResponse.Documents.Any())
                {
                    documents.AddRange(initialSearchResponse.Documents);

                    var scrollResponse = await _client.ScrollAsync<T>(scrollTime, scrollId);
                    if (!scrollResponse.IsValid || scrollResponse.OriginalException != null)
                    {
                        HandleElasticsearchException(scrollResponse, "Scroll");
                    }

                    scrollId = scrollResponse.ScrollId;
                    initialSearchResponse = scrollResponse;
                }

                var clearScrollResponse = await _client.ClearScrollAsync(cs => cs.ScrollId(scrollId));
                if (!clearScrollResponse.IsValid || clearScrollResponse.OriginalException != null)
                {
                    HandleElasticsearchException(clearScrollResponse, "Clear scroll");
                }

                return documents;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception during scrolling search: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Retrieves the field mappings for the specified index.
        /// </summary>
        /// <returns>The field mappings response.</returns>
        public GetMappingResponse? GetFieldMappings()
        {
            var mappingResponse = _client.Indices.GetMapping<T>(descriptor => descriptor.Index(_indexName));


            if (!mappingResponse.IsValid)
            {
                // Handle the case where retrieving the mapping failed
                throw new InvalidOperationException($"Failed to retrieve mapping. Error: {mappingResponse.DebugInformation}");
            }
            else
            {
                return mappingResponse;
            }
        }

        /// <summary>
        /// Creates an alias for the specified index.
        /// </summary>
        /// <param name="aliasName">The name of the alias.</param>
        /// <param name="indexName">The name of the index.</param>
        public void CreateAlias(string aliasName, string indexName)
        {
            try
            {
                var createAliasResponse = _client.Indices.PutAlias(indexName, aliasName);

                if (!createAliasResponse.IsValid)
                {
                    var errorMessage = createAliasResponse.ServerError?.ToString();
                    throw new Exception($"Failed to create alias: {aliasName} for index: {indexName}. Debug information: {createAliasResponse.DebugInformation}. Server Error: {errorMessage}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception during alias creation: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Deletes the specified index.
        /// </summary>
        /// <param name="indexName">The name of the index to delete.</param>
        public void DeleteIndex(string indexName)
        {
            // Assuming "_client" is your Elasticsearch client instance
            var deleteIndexResponse = _client.Indices.Delete(Indices.Index(indexName));

            if (!deleteIndexResponse.IsValid)
            {
                // Handle the case where deleting the index failed
                throw new InvalidOperationException($"Failed to delete index {indexName}. Error: {deleteIndexResponse.DebugInformation}");
            }
        }

        /// <summary>
        /// Retrieves a document by its ID from Elasticsearch.
        /// </summary>
        /// <param name="documentId">The ID of the document to retrieve.</param>
        /// <returns>The retrieved document.</returns>
        public T GetDocumentById(string documentId)
        {
            try
            {
                // Use the _client.Get method to retrieve the document by its ID
                var getResponse = _client.Get<T>(documentId, g => g.Index(_indexName));

                if (getResponse.Found)
                {
                    // Return the retrieved document
                    return getResponse.Source;
                }
                else
                {
                    // Handle the case where the document is not found
                    throw new Exception($"Document with ID {documentId} not found.");
                }
            }
            catch (Exception ex)
            {
                // Log or handle the exception as needed
                Console.WriteLine($"Exception during document retrieval: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Updates a document in Elasticsearch.
        /// </summary>
        /// <param name="documentId">The ID of the document to update.</param>
        /// <param name="updatedDocument">The updated document.</param>
        public void UpdateDocument(string documentId, T updatedDocument)
        {
            var updateResponse = _client.Update<T>(documentId, u => u
                .Doc(updatedDocument)
                .Index(_indexName)
            );

            if (!updateResponse.IsValid || updateResponse.OriginalException != null)
            {
                HandleElasticsearchException(updateResponse, "Update document");
            }
        }

        /// <summary>
        /// Performs an upsert operation for a document in Elasticsearch.
        /// </summary>
        /// <param name="documentId">The ID of the document to upsert.</param>
        /// <param name="updatedDocument">The updated document for upsert.</param>
        public void UpsertDocument(string documentId, T updatedDocument)
        {
            var updateResponse = _client.Update<T>(documentId, u => u
                .Doc(updatedDocument)
                .Upsert(updatedDocument)  // Add this line for upsert
                .Index(_indexName)        // Specify the index/alias where the document resides
            );

            if (!updateResponse.IsValid || updateResponse.OriginalException != null)
            {
                HandleElasticsearchException(updateResponse, "Upsert document");
            }
        }

        /// <summary>
        /// Deletes a document from Elasticsearch.
        /// </summary>
        /// <param name="documentId">The ID of the document to delete.</param>
        public void DeleteDocument(string documentId)
        {
            var deleteResponse = _client.Delete<T>(documentId, d => d
                .Index(_indexName) // Specify the index/alias from which to delete the document
            );

            if (!deleteResponse.IsValid || deleteResponse.OriginalException != null)
            {
                HandleElasticsearchException(deleteResponse, "Delete document");
            }
        }

        /// <summary>
        /// Ensures that the mapping for the index is correct.
        /// </summary>
        /// <param name="indexName">The name of the index.</param>
        private void EnsureMappingIsCorrect(string indexName)
        {
            var existsResponse = _client.Indices.Exists(indexName);
            if (!existsResponse.Exists)
            {
                CreateMap(indexName, _aliasName); // Pass the alias name to CreateMap
            }
        }

        /// <summary>
        /// Creates a mapping for the specified index and alias.
        /// </summary>
        /// <param name="indexName">The name of the index.</param>
        /// <param name="aliasName">The name of the alias.</param>
        private void CreateMap(string indexName, string aliasName)
        {

            var createIndexResponse = _client.Indices.Create(indexName, c => c
                .Settings(s => s
                    .NumberOfShards(_settings.NumberOfShards)
                    .NumberOfReplicas(_settings.NumberOfReplicas))
                .Map<T>(m => m.AutoMap())
            );

            if (!createIndexResponse.IsValid)
            {
                throw new Exception($"Failed to create index: {indexName}. Debug information: {createIndexResponse.DebugInformation}");
            }


            if (indexName != aliasName)
            {
                var createAliasResponse = _client.Indices.PutAlias(indexName, aliasName);

                if (!createAliasResponse.IsValid)
                {
                    var errorMessage = createAliasResponse.ServerError?.ToString();
                    throw new Exception($"Failed to create alias: {aliasName} for index: {indexName}. Debug information: {createAliasResponse.DebugInformation}. Server Error: {errorMessage}");
                }
            }
        }


        private IEnumerable<string> GetFieldNamesFromAttributes<T>()
        {
            return typeof(T).GetProperties()
                .Where(prop => prop.GetCustomAttribute<JsonIgnoreAttribute>() == null)
                .Select(prop => prop.GetCustomAttribute<KeywordAttribute>())
                .Where(attr => attr != null)
                .Select(attr => char.ToLowerInvariant(attr.Name[0]) + attr.Name.Substring(1));
        }


        private void HandleElasticsearchException(IResponse response, string operation)
        {
            var errorMessage = response.ServerError?.ToString();
            Console.WriteLine($"Failed to perform {operation}. Debug information: {response.DebugInformation}. Server Error: {errorMessage}");
            throw new Exception($"Failed to perform {operation}. Debug information: {response.DebugInformation}. Server Error: {errorMessage}");
        }
    }
}
