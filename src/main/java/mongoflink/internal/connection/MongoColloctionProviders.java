package mongoflink.internal.connection;

import org.apache.flink.util.Preconditions;

/**
 * A builder class for creating {@link MongoClientProvider}.
 **/
public class MongoColloctionProviders {

    public static Builder getBuilder() {
        return new Builder();
    }

    public static class Builder {

        private String connectionString;

        private String database;

        private String collection;

        public Builder connectionString(String connectionString) {
            Preconditions.checkNotNull(connectionString, "Connection string must not be null");
            this.connectionString = connectionString;
            return this;
        }

        public Builder database(String database) {
            Preconditions.checkNotNull(database, "Database must not be null");
            this.database = database;
            return this;
        }

        public Builder collection(String collection) {
            Preconditions.checkNotNull(collection, "Collection must not be null");
            this.collection = collection;
            return this;
        }

        public MongoClientProvider build() {
            return new MongoSingleCollectionProvider(connectionString, database, collection);
        }
    }
}
