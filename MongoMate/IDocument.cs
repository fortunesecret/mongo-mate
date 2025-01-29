namespace MongoMate;


/// <summary>
/// Represents a MongoDB document with a unique identifier.
/// </summary>
public interface IDocument
{
    /// <summary>
    /// Gets or sets the unique identifier for the document.
    /// </summary>
    string Id { get; set; }
}
