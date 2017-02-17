using Newtonsoft.Json;

namespace Demo
{
    public class Customer
    {
        public Customer(string id, string name)
        {
            Id = id;
            Name = name;
        }

        [JsonProperty("id")]
        public string Id { get; }

        public string Name { get; set; }
    }
}
