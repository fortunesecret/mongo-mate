using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;

namespace MongoMate;




/// <summary>
/// Main client for interacting with MongoDB.
/// </summary>
public class MongoDbClient
{
    private readonly IMongoDatabase _database;
    private readonly ConcurrentDictionary<Type, string> _collectionMappings;
    private readonly ILogger<MongoDbClient> _logger;
    private readonly AsyncRetryPolicy _retryPolicy;

    public MongoDbClient(MongoDbConfiguration config, ILogger<MongoDbClient> logger = null)
    {
        _logger = logger;

        if (config == null)
            throw new ArgumentNullException(nameof(config));
        if (string.IsNullOrEmpty(config.ConnectionString))
            throw new ArgumentException("ConnectionString cannot be null or empty", nameof(config));
        if (string.IsNullOrEmpty(config.DatabaseName))
            throw new ArgumentException("DatabaseName cannot be null or empty", nameof(config));

        try
        {
            var client = new MongoClient(config.ConnectionString);
            _database = client.GetDatabase(config.DatabaseName);
            _collectionMappings = new ConcurrentDictionary<Type, string>();

            if (config.Collections != null)
            {
                RegisterCollectionsFromConfig(config.Collections);
            }

            // Initialize retry policy
            _retryPolicy = Policy
                .Handle<MongoException>()
                .WaitAndRetryAsync(3,
                    retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                    onRetry: (exception, timeSpan, retryCount, context) =>
                    {
                        _logger?.LogWarning(exception,
                            "Retry {RetryCount} after {TimeSpan}s delay due to: {Message}",
                            retryCount, timeSpan.TotalSeconds, exception.Message);
                    });
        }
        catch (Exception ex)
        {
            var message = $"Failed to connect to MongoDB: {ex.Message}";
            _logger?.LogError(ex, message);
            throw new MongoConnectionException(message, ex);
        }
    }

    public MongoDbClient(string connectionString, string databaseName, ILogger<MongoDbClient> logger = null)
        : this(new MongoDbConfiguration
        {
            ConnectionString = connectionString,
            DatabaseName = databaseName
        }, logger)
    {
    }

    private void RegisterCollectionsFromConfig(Dictionary<string, string> collectionMappings)
    {
        var documentTypes = AppDomain.CurrentDomain.GetAssemblies()
            .SelectMany(a =>
            {
                try
                {
                    return a.GetTypes();
                }
                catch (ReflectionTypeLoadException ex)
                {
                    _logger?.LogWarning(ex, "Failed to load some types from assembly {Assembly}", a.FullName);
                    return Array.Empty<Type>();
                }
            })
            .Where(t => !t.IsAbstract && typeof(IDocument).IsAssignableFrom(t));

        foreach (var mapping in collectionMappings)
        {
            var matchingType = documentTypes.FirstOrDefault(t =>
                t.Name.Equals(mapping.Value, StringComparison.OrdinalIgnoreCase));

            if (matchingType != null)
            {
                _collectionMappings.TryAdd(matchingType, mapping.Key);
                _logger?.LogInformation("Registered collection mapping: {Type} -> {Collection}",
                    matchingType.Name, mapping.Key);
            }
        }
    }

    public void RegisterCollection<T>(string collectionName) where T : IDocument
    {
        _collectionMappings.TryAdd(typeof(T), collectionName);
        _logger?.LogInformation("Manually registered collection: {Type} -> {Collection}",
            typeof(T).Name, collectionName);
    }

    internal IMongoCollection<T> GetCollection<T>() where T : IDocument
    {
        if (!_collectionMappings.TryGetValue(typeof(T), out string collectionName))
        {
            throw new InvalidOperationException(
                $"Collection mapping not found for type {typeof(T).Name}. Please register the collection first.");
        }
        return _database.GetCollection<T>(collectionName);
    }

    public async Task<T> GetAsync<T>(string id, CancellationToken cancellationToken = default) where T : IDocument
    {
        return await _retryPolicy.ExecuteAsync(async () =>
        {
            var collection = GetCollection<T>();
            var filter = Builders<T>.Filter.Eq(doc => doc.Id, id);
            return await collection.Find(filter).SingleOrDefaultAsync(cancellationToken);
        });
    }

    public async Task<IEnumerable<T>> GetAllAsync<T>(CancellationToken cancellationToken = default) where T : IDocument
    {
        return await _retryPolicy.ExecuteAsync(async () =>
        {
            var collection = GetCollection<T>();
            return await collection.Find(_ => true).ToListAsync(cancellationToken);
        });
    }

    public async Task<IEnumerable<T>> FindAsync<T>(
        Expression<Func<T, bool>> filterExpression,
        CancellationToken cancellationToken = default) where T : IDocument
    {
        return await _retryPolicy.ExecuteAsync(async () =>
        {
            var collection = GetCollection<T>();
            return await collection.Find(filterExpression).ToListAsync(cancellationToken);
        });
    }

    public async Task<T> FindOneAsync<T>(
        Expression<Func<T, bool>> filterExpression,
        CancellationToken cancellationToken = default) where T : IDocument
    {
        return await _retryPolicy.ExecuteAsync(async () =>
        {
            var collection = GetCollection<T>();
            return await collection.Find(filterExpression).FirstOrDefaultAsync(cancellationToken);
        });
    }

    public async Task CreateAsync<T>(T document, CancellationToken cancellationToken = default) where T : IDocument
    {
        await _retryPolicy.ExecuteAsync(async () =>
        {
            if (string.IsNullOrEmpty(document.Id))
            {
                document.Id = Guid.NewGuid().ToString();
            }
            var collection = GetCollection<T>();
            await collection.InsertOneAsync(document, null, cancellationToken);
            return document;
        });
    }

    public async Task UpdateAsync<T>(string id, T document, CancellationToken cancellationToken = default) where T : IDocument
    {
        await _retryPolicy.ExecuteAsync(async () =>
        {
            document.Id = id;
            var collection = GetCollection<T>();
            await collection.ReplaceOneAsync(doc => doc.Id == id, document,
                cancellationToken: cancellationToken);
            return document;
        });
    }

    public async Task DeleteAsync<T>(string id, CancellationToken cancellationToken = default) where T : IDocument
    {
        await _retryPolicy.ExecuteAsync(async () =>
        {
            var collection = GetCollection<T>();
            await collection.DeleteOneAsync(doc => doc.Id == id, cancellationToken);
            return true;
        });
    }

    public async Task CreateManyAsync<T>(
        IEnumerable<T> documents,
        CancellationToken cancellationToken = default) where T : IDocument
    {
        await _retryPolicy.ExecuteAsync(async () =>
        {
            foreach (var doc in documents)
            {
                if (string.IsNullOrEmpty(doc.Id))
                {
                    doc.Id = Guid.NewGuid().ToString();
                }
            }
            var collection = GetCollection<T>();
            await collection.InsertManyAsync(documents, null, cancellationToken);
            return documents;
        });
    }

    public async Task UpdateManyAsync<T>(
        IEnumerable<T> documents,
        CancellationToken cancellationToken = default) where T : IDocument
    {
        await _retryPolicy.ExecuteAsync(async () =>
        {
            var collection = GetCollection<T>();
            var writes = documents.Select(doc =>
                new ReplaceOneModel<T>(
                    Builders<T>.Filter.Eq(x => x.Id, doc.Id),
                    doc));
            await collection.BulkWriteAsync(writes, null, cancellationToken);
            return documents;
        });
    }

    public async Task DeleteManyAsync<T>(
        IEnumerable<string> ids,
        CancellationToken cancellationToken = default) where T : IDocument
    {
        await _retryPolicy.ExecuteAsync(async () =>
        {
            var collection = GetCollection<T>();
            var filter = Builders<T>.Filter.In(x => x.Id, ids);
            await collection.DeleteManyAsync(filter, cancellationToken);
            return true;
        });
    }

    public async Task<long> CountAsync<T>(
        Expression<Func<T, bool>> filterExpression = null,
        CancellationToken cancellationToken = default) where T : IDocument
    {
        return await _retryPolicy.ExecuteAsync(async () =>
        {
            var collection = GetCollection<T>();
            return filterExpression == null
                ? await collection.CountDocumentsAsync(_ => true, null, cancellationToken)
                : await collection.CountDocumentsAsync(filterExpression, null, cancellationToken);
        });
    }

    public async Task CreateIndexAsync<T>(
        Expression<Func<T, object>> field,
        bool unique = false,
        CancellationToken cancellationToken = default) where T : IDocument
    {
        await _retryPolicy.ExecuteAsync(async () =>
        {
            var collection = GetCollection<T>();
            var indexKeysDefinition = Builders<T>.IndexKeys.Ascending(field);
            await collection.Indexes.CreateOneAsync(
                new CreateIndexModel<T>(indexKeysDefinition,
                new CreateIndexOptions { Unique = unique }),
                cancellationToken: cancellationToken);
            return true;
        });
    }
}