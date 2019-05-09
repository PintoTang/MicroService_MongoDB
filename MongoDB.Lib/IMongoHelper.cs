using MongoDB.Bson;
using MongoDB.Driver;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace MongoDB.Lib
{
    public interface IMongoHelper<T> where T:class,new()
    {
        int AddOne(string col,T t);

        Task<int> AddOneAsync(string col,T t);

        int AddMany(string col, List<T> t);

        Task<int> AddManyAsync(string col, List<T> t);

        UpdateResult ModifyOne(string col, T t, string id);

        Task<UpdateResult> ModifyOneAsync(string col, T t, string id);

        UpdateResult ModifyManay(string col, Dictionary<string, string> dic, FilterDefinition<T> filter);

        Task<UpdateResult> ModifyManayAsync(string col, Dictionary<string, string> dic, FilterDefinition<T> filter);

        DeleteResult RemoveOne(string col, string id);

        Task<DeleteResult> RemoveOneAsync(string col, string id);

        DeleteResult RemoveMany(string col, FilterDefinition<T> filter);

        Task<DeleteResult> RemoveManyAsync(string col, FilterDefinition<T> filter);

        long Count(string col, FilterDefinition<T> filter);

        Task<long> CountAsync(string col, FilterDefinition<T> filter);

        T FindOne(string col, string id, string[] field = null);

        Task<T> FindOneAsync(string col, string id, string[] field = null);

        List<T> FindList(string col, FilterDefinition<T> filter, string[] field = null, SortDefinition<T> sort = null);

        Task<List<T>> FindListAsync(string col, FilterDefinition<T> filter, string[] field = null, SortDefinition<T> sort = null);

        List<T> FindListByPage(string col, FilterDefinition<T> filter, int pageIndex, int pageSize, out long count, string[] field = null, SortDefinition<T> sort = null);

        Task<List<T>> FindListByPageAsync(string col, FilterDefinition<T> filter, int pageIndex, int pageSize, string[] field = null, SortDefinition<T> sort = null);

        string UploadByByte(string filename, byte[] source);

        string UploadByStream(string filename, Stream stream);

        int DeleteFile(string id);

        byte[] DownloadToByteByObjecId(ObjectId obj);

        int DownloadToStreamByObjectId(ObjectId obj, Stream file);

        Stream OpenDownToStreamByObjectId(ObjectId obj);
    }
}
