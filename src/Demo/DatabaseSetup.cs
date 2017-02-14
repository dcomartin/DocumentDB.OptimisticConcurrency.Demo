using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

namespace Demo
{
    public class DatabaseSetup
    {
        
        public DocumentClient Client { get; }

        public Database Database { get; private set; }

        public DocumentCollection Collection { get; private set; }

        public DatabaseSetup(DocumentClient client)
        {
            Client = client;
        }

        private async Task<Database> GetOrCreateDatabaseAsync(string databaseId)
        {
            var database = Client.CreateDatabaseQuery()
                .Where(db => db.Id == databaseId)
                .ToArray()
                .FirstOrDefault();

            if (database == null)
            {
                database = await Client.CreateDatabaseAsync(new Database { Id = databaseId });
            }

            return database;
        }

        private async Task<DocumentCollection> GetOrCreateCollectionAsync(string databaseId, string collectionId)
        {
            var databaseUri = UriFactory.CreateDatabaseUri(databaseId);

            DocumentCollection collection = Client.CreateDocumentCollectionQuery(databaseUri)
                .Where(c => c.Id == collectionId)
                .AsEnumerable()
                .FirstOrDefault();

            if (collection == null)
            {
                collection = await Client.CreateDocumentCollectionAsync(databaseUri, new DocumentCollection { Id = collectionId });
            }

            return collection;
        }

        public async Task Init(string databaseId, string collectionId)
        {
            Database = await GetOrCreateDatabaseAsync(databaseId);
            Collection = await GetOrCreateCollectionAsync(databaseId, collectionId);
        }

        public async Task AddCustomer(Customer customer)
        {
            await Client.CreateDocumentAsync(Collection.SelfLink, customer);
        }

    }
}
