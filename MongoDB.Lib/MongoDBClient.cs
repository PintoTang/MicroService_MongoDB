using Microsoft.Extensions.Options;
using MongoDB.Driver;
using MongoDB.Driver.GridFS;

namespace MongoDB.Lib
{
    public class MongoDBClient
    {
        private MongoClient _client { get;}

        private readonly MongoDBHostOptions _mongodbHostOptions;
        public IMongoDatabase _db { get; private set; }
        public GridFSBucket _fs { get; private set; }

        public MongoDBClient(IOptions<MongoDBHostOptions> options) {
            _mongodbHostOptions = options.Value;
            _client = new MongoClient(_mongodbHostOptions.Connection);
            _db = _client.GetDatabase(_mongodbHostOptions.DataBase);
            _fs = new GridFSBucket(_db);
        }
    }
}
