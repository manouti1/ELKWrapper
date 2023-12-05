using Nest;

namespace ElasticTools
{
    public interface IELKWrapper<T> where T : class
    {
        Task<IList<T>> ScrollSearchAsync(
          Func<QueryContainerDescriptor<T>, QueryContainer> queryFunc,
          Func<SortDescriptor<T>, IPromise<IList<ISort>>> sortFunc,
          Func<AggregationContainerDescriptor<T>, IAggregationContainer> aggregationFunc = null,
          int pageSize = 1000,
          int pageIndex = 0,
          string scrollTime = "1m");
        string IndexDocument(T document);
        T GetDocumentById(string documentId);
        void DeleteDocument(string documentId);
        void UpdateDocument(string documentId, T updatedDocument);
        void UpdateDocumentConcurrently(string documentId, T updatedDocument);
        void UpsertDocument(string documentId, T updatedDocument);
        void CreateAlias(string aliasName, string indexName);
        void DeleteIndex(string indexName);
        GetMappingResponse? GetFieldMappings();
    }
}
