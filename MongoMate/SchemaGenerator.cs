using System.ComponentModel.DataAnnotations;
using System.Reflection;
using MongoDB.Bson;
using MongoDB.Driver;

namespace MongoMate;

public class SchemaGenerator
{
    private readonly MongoDbClient _client;
    private readonly IMongoDatabase _database;

    public SchemaGenerator(MongoDbClient client, string connectionString, string databaseName)
    {
        _client = client;
        var mongoClient = new MongoClient(connectionString);
        _database = mongoClient.GetDatabase(databaseName);
    }

    public async Task GenerateCollectionsFromAssembly(Assembly assembly)
    {
        var documentTypes = assembly.GetTypes()
            .Where(t => !t.IsAbstract && typeof(IDocument).IsAssignableFrom(t));

        foreach (var type in documentTypes)
        {
            await GenerateCollectionForType(type);
        }
    }

    public async Task GenerateCollectionForType(Type type)
    {
        var collectionName = GetCollectionName(type);
        var validationSchema = GenerateJsonSchema(type);

        // Create collection if it doesn't exist
        var collections = await _database.ListCollectionNames().ToListAsync();
        if (!collections.Contains(collectionName))
        {
            // Create generic CreateCollectionOptions type
            var optionsType = typeof(CreateCollectionOptions<>).MakeGenericType(type);
            dynamic options = Activator.CreateInstance(optionsType);

            // Set validation options
            options.Validator = new BsonDocumentFilterDefinition<BsonDocument>(validationSchema);
            options.ValidationAction = DocumentValidationAction.Error;
            options.ValidationLevel = DocumentValidationLevel.Strict;

            await _database.CreateCollectionAsync(collectionName, options);
            Console.WriteLine($"Created collection '{collectionName}' with schema validation");
        }
        else
        {
            // Update existing collection's validation
            var command = new BsonDocument
            {
                { "collMod", collectionName },
                { "validator", validationSchema },
                { "validationAction", "error" },
                { "validationLevel", "strict" }
            };
            await _database.RunCommandAsync<BsonDocument>(command);
            Console.WriteLine($"Updated schema validation for collection '{collectionName}'");
        }

        // Register the collection with our client
        typeof(MongoDbClient)
            .GetMethod("RegisterCollection")
            ?.MakeGenericMethod(type)
            .Invoke(_client, new object[] { collectionName });
    }

    public string GetCollectionName(Type type)
    {
        // Check for custom collection name attribute first
        var collectionAttr = type.GetCustomAttribute<CollectionNameAttribute>();
        if (collectionAttr != null)
            return collectionAttr.Name;

        // Otherwise, pluralize the type name
        return Pluralize(type.Name.ToLower());
    }

    private BsonDocument GenerateJsonSchema(Type type)
    {
        var properties = new BsonDocument();
        var required = new BsonArray();

        foreach (var prop in type.GetProperties())
        {
            var propertySchema = GeneratePropertySchema(prop);
            if (propertySchema != null)
            {
                properties.Add(prop.Name, propertySchema);

                // Check if property is required
                if (prop.GetCustomAttribute<RequiredAttribute>() != null)
                {
                    required.Add(prop.Name);
                }
            }
        }

        var schema = new BsonDocument
        {
            { "bsonType", "object" },
            { "required", required },
            { "properties", properties }
        };

        return schema;
    }

    private BsonDocument GeneratePropertySchema(PropertyInfo prop)
    {
        var schema = new BsonDocument();
        var type = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;

        // Handle different types
        if (type == typeof(string))
        {
            schema.Add("bsonType", "string");

            // Add string validations if present
            var stringLength = prop.GetCustomAttribute<StringLengthAttribute>();
            if (stringLength != null)
            {
                schema.Add("minLength", stringLength.MinimumLength);
                schema.Add("maxLength", stringLength.MaximumLength);
            }

            var regex = prop.GetCustomAttribute<RegularExpressionAttribute>();
            if (regex != null)
            {
                schema.Add("pattern", regex.Pattern);
            }
        }
        else if (type == typeof(int))
        {
            schema.Add("bsonType", "int");

            var range = prop.GetCustomAttribute<RangeAttribute>();
            if (range != null)
            {
                schema.Add("minimum", Convert.ToInt32(range.Minimum));
                schema.Add("maximum", Convert.ToInt32(range.Maximum));
            }
        }
        else if (type == typeof(long))
            schema.Add("bsonType", "long");
        else if (type == typeof(double) || type == typeof(decimal))
            schema.Add("bsonType", "double");
        else if (type == typeof(bool))
            schema.Add("bsonType", "bool");
        else if (type == typeof(DateTime))
            schema.Add("bsonType", "date");
        else if (type == typeof(ObjectId))
            schema.Add("bsonType", "objectId");
        else if (type.IsEnum)
        {
            schema.Add("enum", new BsonArray(Enum.GetNames(type)));
        }
        else if (type.IsArray || (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(List<>)))
        {
            schema.Add("bsonType", "array");
            var elementType = type.IsArray ? type.GetElementType() : type.GetGenericArguments()[0];
            var itemSchema = GeneratePropertySchema(elementType.GetProperties().First());
            if (itemSchema != null)
                schema.Add("items", itemSchema);
        }

        return schema;
    }

    private string Pluralize(string word)
    {
        return Humanizer.InflectorExtensions.Pluralize(word);
    }
}