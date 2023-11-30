namespace ElasticTools
{
    public class ElasticSearchSettings
    {
        public ElasticSearchSettings() { }

        public ElasticSearchSettings(string url, string username, string password, string indexName)
        {
            Url = url;
            Username = username;
            Password = password;
            IndexName = indexName;
        }

        public string Username { get; set; }
        public string Password { get; set; }
        public string Url { get; set; }
        public string IndexName { get; set; }
        public bool SingleNode { get; set; }
        public int NumberOfShards { get; set; } = 1;
        public int NumberOfReplicas { get; set; } = 0;
    }
}


