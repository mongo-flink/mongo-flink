package mongoflink.internal.connection;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

/**
 * Provided for initiate and recreate {@link MongoClient}.
 **/
public interface MongoClientProvider {

    /**
     * Create one or get the current {@link MongoClient}.
     * @return Current {@link MongoClient}.
     */
    MongoClient getClient();

    /**
     * Get the default collection.
     * @return Current {@link MongoCollection}.
     */
    MongoCollection<Document> getDefaultCollection();

    /**
     * Recreate a client. Used typically when a connection is timed out or lost.
     * @return A new {@link MongoClient}.
     */
    MongoClient recreateClient();

    /**
     * Close the underlying MongoDB connection.
     */
    void close();
}
