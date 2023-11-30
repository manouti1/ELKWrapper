using ElasticTools;
using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ELKTests
{

    [TestClass]
    public class ELKWrapperTests
    {
        private const string ElasticsearchUrl = "https://localhost:9200";
        private const string IndexAlias = "test_alias";
        private const string IndexName = "test_index";

        // Test settings for Elasticsearch
        private static readonly ElasticSearchSettings TestSettings = new ElasticSearchSettings
        {
            Url = ElasticsearchUrl,
            Username = "***",
            Password = "***"
        };


        // Class initialization method that runs once before any test method
        [TestInitialize]
        public void TestInitialize()
        {
            // This method is responsible for creating some sample data in Elasticsearch
            var elkWrapper = new ELKWrapper<Person>(TestSettings, IndexName, IndexAlias);

            // Sample data for testing
            var people = new List<Person>
            {
            new Person { Name = "John Doe", Age = 25, City = "New York" },
            new Person { Name = "Jane Doe", Age = 30, City = "Los Angeles" },
            new Person { Name = "Bob Smith", Age = 35, City = "New York" },
            new Person { Name = "Alice Johnson", Age = 28, City = "Chicago" },
            new Person { Name = "Charlie Brown", Age = 22, City = "Los Angeles" }
        };

            // Index each person document
            foreach (var person in people)
            {
                elkWrapper.IndexDocument(person);
            }

            // Wait for the data to be indexed (this delay might not be necessary in a real-world scenario)
            Thread.Sleep(1000);
        }

        [TestCleanup]
        public void Cleanup()
        {
            var elkWrapper = new ELKWrapper<Person>(TestSettings, IndexName, IndexAlias);

            // Clean up any resources after each test
            elkWrapper.DeleteIndex(IndexName);
        }

        [TestMethod]
        public async Task ScrollSearchAsync_ShouldReturnResultsWithAggregation()
        {
            // Arrange
            var elkWrapper = new ELKWrapper<Person>(TestSettings, IndexName, IndexAlias);

            // Act
            var results = await elkWrapper.ScrollSearchAsync(
                q => q.MatchAll(), // Match all documents
                s => s.Descending(f => f.Age), // Sort by Age in descending order
                aggregationFunc: aggs => aggs
                    .Terms("city.keyword", t => t.Field(f => f.City.Suffix("keyword")).Size(10)), // Aggregate by City using the ".keyword" field
                pageSize: 10,
                pageIndex: 0,
                scrollTime: "1m"
            );

            // Assert
            Assert.IsNotNull(results);
            Assert.IsTrue(results.Count > 0, "Expected to get results from the scroll search.");
            Assert.IsTrue(results.All(p => !string.IsNullOrEmpty(p.City)), "Each result should have a City.");
            // Add assertions for aggregation/grouping if needed
        }


        [TestMethod]
        public async Task ScrollSearchAsync_ShouldReturnResultsWithSorting()
        {
            // Arrange
            var elkWrapper = new ELKWrapper<Person>(TestSettings, IndexName, IndexAlias);

            // Act
            var results = await elkWrapper.ScrollSearchAsync(
                q => q.MatchAll(),
                s => s.Descending(f => f.Age),
                pageSize: 10,
                pageIndex: 0,
                scrollTime: "1m"
            );

            // Assert
            Assert.IsNotNull(results);
            Assert.IsTrue(results.Count > 0, "Expected to get results from the scroll search.");
            Assert.IsTrue(results.All(p => !string.IsNullOrEmpty(p.City)), "Each result should have a City.");
            Assert.IsTrue(results.SequenceEqual(results.OrderByDescending(p => p.Age)), "Results should be sorted by Age in descending order.");
        }

        [TestMethod]
        public async Task ScrollSearchAsync_ShouldReturnResultsWithCustomQuery()
        {
            // Arrange
            var elkWrapper = new ELKWrapper<Person>(TestSettings, IndexName, IndexAlias);

            // Act
            var results = await elkWrapper.ScrollSearchAsync(
                q => q.Term(t => t.Field(f => f.City.Suffix("keyword")).Value("New York")),
                s => s.Ascending(f => f.Age),
                pageSize: 10,
                pageIndex: 0,
                scrollTime: "1m"
            );

            // Assert
            Assert.IsNotNull(results);
            Assert.IsTrue(results.Count > 0, "Expected to get results from the scroll search.");
            Assert.IsTrue(results.All(p => p.City == "New York"), "Each result should have City 'New York'.");
            Assert.IsTrue(results.SequenceEqual(results.OrderBy(p => p.Age)), "Results should be sorted by Age in ascending order.");
        }

        [TestMethod]
        public void UpdateDocument_ShouldUpdateDocumentSuccessfully()
        {
            // Arrange
            var elkWrapper = new ELKWrapper<Person>(TestSettings, IndexName, IndexAlias);

            // Act
            var personToUpdate = new Person { Name = "John Doe 2", Age = 26, City = "New York" };
            var id = elkWrapper.IndexDocument(personToUpdate); // Index the document first

            WaitForIndexing(elkWrapper, id);

            var document = elkWrapper.GetDocumentById(id);
            personToUpdate.Age = 27; // Update the Age
            elkWrapper.UpdateDocument(id, personToUpdate);
            WaitForIndexing(elkWrapper, id);
            // Assert
            var updatedPerson = elkWrapper.ScrollSearchAsync(
                q => q.MatchPhrase(t => t.Field(f => f.Name).Query("John Doe 2")),
                s => s.Descending(f => f.Age),
                pageSize: 1,
                pageIndex: 0,
                scrollTime: "1m"
            ).Result.FirstOrDefault();

            // Use more specific assertions
            Assert.IsNotNull(updatedPerson, "Updated person should not be null.");
            Assert.AreEqual(personToUpdate.Age, updatedPerson?.Age, "Age should be updated successfully.");
        }

        [TestMethod]
        public void DeleteDocument_ShouldDeleteDocumentSuccessfully()
        {
            // Arrange
            var elkWrapper = new ELKWrapper<Person>(TestSettings, IndexName, IndexAlias);

            // Act
            var personToDelete = new Person { Name = "Jane Doe", Age = 30, City = "Los Angeles" };
            var id = elkWrapper.IndexDocument(personToDelete); // Index the document first
            elkWrapper.DeleteDocument(id);

            // Assert
            var deletedPerson = elkWrapper.ScrollSearchAsync(
                q => q.Term(t => t.Field(f => f.Name).Value("Jane Doe")),
                s => s.Descending(f => f.Age),
                pageSize: 1,
                pageIndex: 0,
                scrollTime: "1m"
            ).Result.FirstOrDefault();

            Assert.IsNull(deletedPerson, "Deleted person should be null.");
        }

        [TestMethod]
        public void UpsertDocument_UpdatesExistingPerson_Successfully()
        {
            // Arrange
            var elkWrapper = new ELKWrapper<Person>(TestSettings, IndexName, IndexAlias);
            var personToUpdate = new Person { Name = "John Dow", Age = 26, City = "New York" };
            var documentId = elkWrapper.IndexDocument(personToUpdate); // Index the document first

            // Index the existing person first
            WaitForIndexing(elkWrapper, documentId);

            // Update the age
            personToUpdate.Age = 26;

            // Act
            elkWrapper.UpsertDocument(documentId, personToUpdate);

            // Assert
            // Retrieve the person from Elasticsearch and assert that the age is updated
            var updatedPerson = elkWrapper.ScrollSearchAsync(
                q => q.MatchPhrase(t => t.Field(f => f.Name).Query("John Dow")),
                s => s.Descending(f => f.Age),
                pageSize: 1,
                pageIndex: 0,
                scrollTime: "1m"
            ).Result.FirstOrDefault();

            Assert.IsNotNull(updatedPerson, "Updated person should not be null.");
            Assert.AreEqual(personToUpdate.Age, updatedPerson.Age, "Age should be updated successfully.");
        }

        [TestMethod]
        public void UpsertDocument_CreatesNewPerson_Successfully()
        {
            // Arrange
            var elkWrapper = new ELKWrapper<Person>(TestSettings, IndexName, IndexAlias);
            var newPerson = new Person { Name = "John Dow", Age = 26, City = "New York" };
            var documentId = elkWrapper.IndexDocument(newPerson); // Index the document first

            // Index the existing person first
            WaitForIndexing(elkWrapper, documentId);

            // Act
            elkWrapper.UpsertDocument(documentId, newPerson);

            // Assert
            // Retrieve the person from Elasticsearch and assert its properties
            var retrievedPerson = elkWrapper.ScrollSearchAsync(
                q => q.MatchPhrase(t => t.Field(f => f.Name).Query("John Dow")),
                s => s.Descending(f => f.Age),
                pageSize: 1,
                pageIndex: 0,
                scrollTime: "1m"
            ).Result.FirstOrDefault();

            Assert.IsNotNull(retrievedPerson, "Retrieved person should not be null.");
            Assert.AreEqual(newPerson.Name, retrievedPerson.Name, "Names should match.");
            Assert.AreEqual(newPerson.Age, retrievedPerson.Age, "Ages should match.");
            Assert.AreEqual(newPerson.City, retrievedPerson.City, "Cities should match.");
        }

        [TestMethod]
        public void IndexDocument_ShouldIndexDocumentSuccessfully()
        {
            // Arrange
            var elkWrapper = new ELKWrapper<Person>(TestSettings, IndexName, IndexAlias);

            // Act
            var newPerson = new Person { Name = "New Person", Age = 40, City = "London" };
            var id = elkWrapper.IndexDocument(newPerson);

            // Wait for the document to be indexed (adjust the delay as needed)
            Thread.Sleep(2000);

            // Assert
            var indexedPerson = elkWrapper.ScrollSearchAsync(
                q => q.MatchPhrase(m => m.Field(f => f.Name).Query("New Person")),
                s => s.Descending(f => f.Age),
                pageSize: 1,
                pageIndex: 0,
                scrollTime: "1m"
            ).Result.FirstOrDefault();

            Assert.IsNotNull(indexedPerson, "Indexed person should not be null.");
            Assert.AreEqual(newPerson.Age, indexedPerson.Age, "Age should be indexed successfully.");
        }

        [TestMethod]
        public void CreateAlias_ShouldCreateAliasSuccessfully()
        {
            // Arrange
            var elkWrapper = new ELKWrapper<Person>(TestSettings, IndexName, IndexAlias);

            // Act
            elkWrapper.CreateAlias("new_alias", IndexName);

            // Assert
            var aliasExists = elkWrapper.ScrollSearchAsync(
                q => q.MatchAll(),
                s => s.Ascending(f => f.Age),
                pageSize: 1,
                pageIndex: 0,
                scrollTime: "1m"
            ).Result.Any();

            Assert.IsTrue(aliasExists, "Alias should exist.");
        }


        private static void WaitForIndexing(ELKWrapper<Person> elkWrapper, string documentId)
        {
            int maxAttempts = 10; // Adjust the number of attempts as needed
            int currentAttempt = 0;
            bool isIndexed = false;

            while (currentAttempt < maxAttempts)
            {
                var indexedDocument = elkWrapper.GetDocumentById(documentId);

                if (indexedDocument != null)
                {
                    isIndexed = true;
                    break;
                }

                Thread.Sleep(500); // Adjust the sleep duration as needed
                currentAttempt++;
            }

            if (!isIndexed)
            {
                throw new TimeoutException("Document indexing did not complete within the specified time.");
            }
        }

    }
}
