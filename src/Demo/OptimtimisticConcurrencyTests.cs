using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Shouldly;
using Xunit;

namespace Demo
{
    public class OptimtimisticConcurrencyTests
    {
        private readonly DocumentClient _client;
        private const string EndpointUrl = "https://localhost:8081";
        private const string AuthorizationKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
        private const string DatabaseId = "ConcurrencyDemo";
        private const string CollectionId = "Customers";

        public OptimtimisticConcurrencyTests()
        {
            _client = new DocumentClient(new Uri(EndpointUrl), AuthorizationKey);
        }

        [Fact]
        public async Task Should_Throw_With_PreconditionFailed()
        {
            // Setup our Database and add a new Customer
            var dbSetup = new DatabaseSetup(_client);
            await dbSetup.Init(DatabaseId, CollectionId);
            var addCustomer = new Customer(Guid.NewGuid().ToString(), "Demo");
            await dbSetup.AddCustomer(addCustomer);

            // Fetch out the Document (Customer)
            var document = (from f in dbSetup.Client.CreateDocumentQuery(dbSetup.Collection.SelfLink)
                            where f.Id == addCustomer.Id
                            select f).AsEnumerable().FirstOrDefault();

            // Cast the Document to our Customer & make a data change
            var editCustomer = (Customer) (dynamic) document;
            editCustomer.Name = "Changed";

            // Using Access Conditions gives us the ability to use the ETag from our fetched document for optimistic concurrency.
            var ac = new AccessCondition {Condition = document.ETag, Type = AccessConditionType.IfMatch};

            // Replace our document, which will succeed with the correct ETag 
            await dbSetup.Client.ReplaceDocumentAsync(document.SelfLink, editCustomer,
                new RequestOptions {AccessCondition = ac});

            // Replace again, which will fail since our (same) ETag is now invalid
            var ex = await dbSetup.Client.ReplaceDocumentAsync(document.SelfLink, editCustomer,
                        new RequestOptions {AccessCondition = ac}).ShouldThrowAsync<DocumentClientException>();

            ex.StatusCode.ShouldBe(HttpStatusCode.PreconditionFailed);
        }

        [Fact]
        public async Task Should_insert_multiple_customers()
        {
            // Setup our Database and add a new Customer
            var dbSetup = new DatabaseSetup(_client);
            await dbSetup.Init(DatabaseId, CollectionId);

            dynamic[] customers = {
                new Customer(Guid.NewGuid().ToString(), "Test1 Customer1"),
                new Customer(Guid.NewGuid().ToString(), "Test1 Customer2")
            };

            var sproc = dbSetup.Client.CreateStoredProcedureQuery(dbSetup.Collection.SelfLink).Where(x => x.Id == "sp_bulkinsert").AsEnumerable().First();
            var imported = await dbSetup.Client.ExecuteStoredProcedureAsync<int>(sproc.SelfLink, new dynamic[] { customers });
            imported.Response.ShouldBe(2);
        }

        [Fact]
        public async Task Should_not_insert_any()
        {
            // Setup our Database and add a new Customer
            var dbSetup = new DatabaseSetup(_client);
            await dbSetup.Init(DatabaseId, CollectionId);

            string customer1Id = Guid.NewGuid().ToString();

            dynamic[] customers = {
                new Customer(customer1Id, "Test2 Customer1"),
                new Customer(Guid.NewGuid().ToString(), "")
            };

            var sproc = dbSetup.Client.CreateStoredProcedureQuery(dbSetup.Collection.SelfLink).Where(x => x.Id == "sp_bulkinsert").AsEnumerable().First();
            await dbSetup.Client.ExecuteStoredProcedureAsync<int>(sproc.SelfLink, new dynamic[] {customers}).ShouldThrowAsync<DocumentClientException>();

            var document = (from f in dbSetup.Client.CreateDocumentQuery(dbSetup.Collection.SelfLink)
                            where f.Id == customer1Id
                            select f).AsEnumerable().FirstOrDefault();

            document.ShouldBeNull();
        }

        [Fact]
        public async Task Should_continue_query()
        {
            // Setup our Database and add a new Customer
            var dbSetup = new DatabaseSetup(_client);
            await dbSetup.Init(DatabaseId, CollectionId);

            var addCustomer1 = new Customer(Guid.NewGuid().ToString(), "Demo1");
            await dbSetup.AddCustomer(addCustomer1);
            var addCustomer2 = new Customer(Guid.NewGuid().ToString(), "Demo2");
            await dbSetup.AddCustomer(addCustomer2);

            var query1 = dbSetup.Client.CreateDocumentQuery<Document>(dbSetup.Collection.SelfLink, new FeedOptions
            {
                MaxItemCount = 1
            }).Where(x => x.Id == addCustomer1.Id.ToString() || x.Id == addCustomer2.Id.ToString()).AsDocumentQuery();

           var result1 = await query1.ExecuteNextAsync<Document>();

            var query2 = dbSetup.Client.CreateDocumentQuery<Document>(dbSetup.Collection.SelfLink, new FeedOptions
            {
                MaxItemCount = 1,
                RequestContinuation = result1.ResponseContinuation
            }).Where(x => x.Id == addCustomer1.Id.ToString() || x.Id == addCustomer2.Id.ToString()).AsDocumentQuery();

            var result2 = await query2.ExecuteNextAsync<Document>();
            result2.Count.ShouldBe(1);

            var resultCustomer = (Customer) (dynamic) result2.First();
            resultCustomer.Id.ShouldBe(addCustomer2.Id);
        }
    }
}
