namespace MongoMate;

/// <summary>
/// Custom exception for MongoDB connection issues.
/// </summary>
public class MongoConnectionException : Exception
{
    public MongoConnectionException(string message, Exception innerException)
        : base(message, innerException) { }
}