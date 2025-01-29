/// <summary>
/// Configuration settings for MongoDB connection and collection mappings.
/// </summary>
public class MongoDbConfiguration
{
    public string ConnectionString { get; set; }
    public string DatabaseName { get; set; }
    public Dictionary<string, string> Collections { get; set; }
}