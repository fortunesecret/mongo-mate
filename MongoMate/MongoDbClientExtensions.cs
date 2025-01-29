using System.Linq.Expressions;

namespace MongoMate;

/// <summary>
/// Extension methods for MongoDbClient.
/// </summary>
public static class MongoDbClientExtensions
{
    public static async Task<bool> ExistsAsync<T>(
        this MongoDbClient client,
        string id,
        CancellationToken cancellationToken = default) where T : IDocument
    {
        return await client.GetAsync<T>(id, cancellationToken) != null;
    }

    public static async Task<T> FindOneAndUpdateAsync<T>(
        this MongoDbClient client,
        Expression<Func<T, bool>> filter,
        T replacement,
        CancellationToken cancellationToken = default) where T : IDocument
    {
        var collection = client.GetCollection<T>();
        return await collection.FindOneAndReplaceAsync(
            filter,
            replacement,
            cancellationToken: cancellationToken);
    }
}