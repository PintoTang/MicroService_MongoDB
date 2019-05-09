using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.GridFS;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace MongoDB.Lib
{
    public class MongoHelper<T>: IMongoHelper<T> where T : class, new()
    {
        public readonly MongoDBClient _client;

        public MongoHelper(MongoDBClient client) {
            _client = client;
        }

        #region 添加
        /// <summary>
        /// 批量添加
        /// </summary>
        /// <param name="col">表</param>
        /// <param name="t"></param>
        /// <returns></returns>
        public int AddMany(string col, List<T> t)
        {
            try
            {
                var db = _client._db.GetCollection<T>(col);
                db.InsertMany(t);
                return 1;
            }
            catch
            {
                return 0;
            }
        }
        /// <summary>
        /// 异步批量添加
        /// </summary>
        /// <param name="col"></param>
        /// <param name="t"></param>
        /// <returns></returns>
        public async Task<int> AddManyAsync(string col, List<T> t)
        {
            try
            {
                var db = _client._db.GetCollection<T>(col);
                await db.InsertManyAsync(t);
                return 1;
            }
            catch
            {
                return 0;
            }
        }
        /// <summary>
        /// 单个添加
        /// </summary>
        /// <param name="col"></param>
        /// <param name="t"></param>
        /// <returns></returns>
        public int AddOne(string col,T t)
        {
            try
            {
                var db = _client._db.GetCollection<T>(col);
                db.InsertOne(t);
                return 1;
            }
            catch {
                return 0;
            }
            
        }
        /// <summary>
        /// 异步单个添加
        /// </summary>
        /// <param name="col"></param>
        /// <param name="t"></param>
        /// <returns></returns>
        public async Task<int> AddOneAsync(string col, T t)
        {
            try
            {
                var db = _client._db.GetCollection<T>(col);
                await db.InsertOneAsync(t);
                return 1;
            }
            catch
            {
                return 0;
            }
        }
        #endregion

        #region 修改
        /// <summary>
        /// 单个修改
        /// </summary>
        /// <param name="col"></param>
        /// <param name="t"></param>
        /// <param name="id"></param>
        /// <returns></returns>
        public UpdateResult ModifyOne(string col, T t, string id)
        {
            try
            {
                var db = _client._db.GetCollection<T>(col);
                //修改条件
                FilterDefinition<T> filter = Builders<T>.Filter.Eq("_id", ObjectId.Parse(id));
                //要修改的字段
                var list = new List<UpdateDefinition<T>>();
                foreach (var item in t.GetType().GetProperties())
                {
                    if (item.Name.ToLower() == "id") continue;
                    list.Add(Builders<T>.Update.Set(item.Name, item.GetValue(t)));
                }
                var updatefilter = Builders<T>.Update.Combine(list);
                return db.UpdateOne(filter, updatefilter);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// 异步单个修改
        /// </summary>
        /// <param name="col"></param>
        /// <param name="t"></param>
        /// <param name="id"></param>
        /// <returns></returns>
        public async Task<UpdateResult> ModifyOneAsync(string col, T t, string id)
        {
            try
            {
                var db = _client._db.GetCollection<T>(col);
                //修改条件
                FilterDefinition<T> filter = Builders<T>.Filter.Eq("_id", ObjectId.Parse(id));
                //要修改的字段
                var list = new List<UpdateDefinition<T>>();
                foreach (var item in t.GetType().GetProperties())
                {
                    if (item.Name.ToLower() == "id") continue;
                    list.Add(Builders<T>.Update.Set(item.Name, item.GetValue(t)));
                }
                var updatefilter = Builders<T>.Update.Combine(list);
                return await db.UpdateOneAsync(filter, updatefilter);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// //1.批量修改,修改的条件
        ///var time = DateTime.Now;
        ///var list = new List<FilterDefinition<PhoneEntity>>();
        ///list.Add(Builders<PhoneEntity>.Filter.Lt("AddTime", time.AddDays(5)));
        ///list.Add(Builders<PhoneEntity>.Filter.Gt("AddTime", time));
        ///var filter = Builders<PhoneEntity>.Filter.And(list);
        /// //2.要修改的字段内容
        ///var dic = new Dictionary<string, string>();
        ///dic.Add("UseAge", "168");
        ///dic.Add("Name", "朝阳");
        /// //3.批量修改
        ///var kk = TMongodbHelper<PhoneEntity>.UpdateManay(host, dic, filter);
        /// </summary>
        /// <param name="col"></param>
        /// <param name="dic"></param>
        /// <param name="filter"></param>
        /// <returns></returns>
        public UpdateResult ModifyManay(string col, Dictionary<string, string> dic, FilterDefinition<T> filter)
        {
            try
            {
                var db = _client._db.GetCollection<T>(col);
                T t = new T();
                //要修改的字段
                var list = new List<UpdateDefinition<T>>();
                foreach (var item in t.GetType().GetProperties())
                {
                    if (!dic.ContainsKey(item.Name)) continue;
                    var value = dic[item.Name];
                    list.Add(Builders<T>.Update.Set(item.Name, value));
                }
                var updatefilter = Builders<T>.Update.Combine(list);
                return db.UpdateMany(filter, updatefilter);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// 异步批量修改
        /// </summary>
        /// <param name="col"></param>
        /// <param name="dic"></param>
        /// <param name="filter"></param>
        /// <returns></returns>
        public async Task<UpdateResult> ModifyManayAsync(string col, Dictionary<string, string> dic, FilterDefinition<T> filter)
        {
            try
            {
                var db = _client._db.GetCollection<T>(col);
                T t = new T();
                //要修改的字段
                var list = new List<UpdateDefinition<T>>();
                foreach (var item in t.GetType().GetProperties())
                {
                    if (!dic.ContainsKey(item.Name)) continue;
                    var value = dic[item.Name];
                    list.Add(Builders<T>.Update.Set(item.Name, value));
                }
                var updatefilter = Builders<T>.Update.Combine(list);
                return await db.UpdateManyAsync(filter, updatefilter);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region 删除
        /// <summary>
        /// 单个删除
        /// </summary>
        /// <param name="col"></param>
        /// <param name="id"></param>
        /// <returns></returns>
        public DeleteResult RemoveOne(string col, string id)
        {
            try
            {
                var db = _client._db.GetCollection<T>(col);
                FilterDefinition<T> filter = Builders<T>.Filter.Eq("_id", ObjectId.Parse(id));
                return db.DeleteOne(filter);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// 异步当个删除
        /// </summary>
        /// <param name="col"></param>
        /// <param name="id"></param>
        /// <returns></returns>
        public async Task<DeleteResult> RemoveOneAsync(string col, string id)
        {
            try
            {
                var db = _client._db.GetCollection<T>(col);
                FilterDefinition<T> filter = Builders<T>.Filter.Eq("_id", ObjectId.Parse(id));
                return await db.DeleteOneAsync(filter);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// 批量删除
        /// </summary>
        /// <param name="col"></param>
        /// <param name="filter"></param>
        /// <returns></returns>
        public DeleteResult RemoveMany(string col, FilterDefinition<T> filter)
        {
            try
            {
                var db = _client._db.GetCollection<T>(col);
                return db.DeleteMany(filter);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// 异步批量删除
        /// </summary>
        /// <param name="col"></param>
        /// <param name="filter"></param>
        /// <returns></returns>
        public async Task<DeleteResult> RemoveManyAsync(string col, FilterDefinition<T> filter)
        {
            try
            {
                var db = _client._db.GetCollection<T>(col);
                return await db.DeleteManyAsync(filter);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion
        
        #region 获取总数
        /// <summary>
        /// 获取记录数
        /// </summary>
        /// <param name="col"></param>
        /// <param name="filter"></param>
        /// <returns></returns>
        public long Count(string col, FilterDefinition<T> filter)
        {
            try
            {
                var db = _client._db.GetCollection<T>(col);
                //return db.Count(filter);
                return db.CountDocuments(filter);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// 异步获取记录数
        /// </summary>
        /// <param name="col"></param>
        /// <param name="filter"></param>
        /// <returns></returns>
        public async Task<long> CountAsync(string col, FilterDefinition<T> filter)
        {
            try
            {
                var db = _client._db.GetCollection<T>(col);
                //return await db.CountAsync(filter);
                return await db.CountDocumentsAsync(filter);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region 查询
        /// <summary>
        /// 单个查找
        /// </summary>
        /// <param name="col"></param>
        /// <param name="id"></param>
        /// <param name="field"></param>
        /// <returns></returns>
        public T FindOne(string col, string id, string[] field = null)
        {
            try
            {
                var db = _client._db.GetCollection<T>(col);
                FilterDefinition<T> filter = Builders<T>.Filter.Eq("_id", ObjectId.Parse(id));
                //不指定查询字段
                if (field == null || field.Length == 0)
                {
                    return db.Find(filter).FirstOrDefault<T>();
                }
                //制定查询字段
                var fieldList = new List<ProjectionDefinition<T>>();
                for (int i = 0; i < field.Length; i++)
                {
                    fieldList.Add(Builders<T>.Projection.Include(field[i].ToString()));
                }
                var projection = Builders<T>.Projection.Combine(fieldList);
                fieldList?.Clear();
                return db.Find(filter).Project<T>(projection).FirstOrDefault<T>();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// 异步单个查找
        /// </summary>
        /// <param name="col"></param>
        /// <param name="id"></param>
        /// <param name="field"></param>
        /// <returns></returns>
        public async Task<T> FindOneAsync(string col, string id, string[] field = null)
        {
            try
            {
                var db = _client._db.GetCollection<T>(col);
                FilterDefinition<T> filter = Builders<T>.Filter.Eq("_id", ObjectId.Parse(id));
                //不指定查询字段
                if (field == null || field.Length == 0)
                {
                    return await db.Find(filter).FirstOrDefaultAsync();
                }
                //制定查询字段
                var fieldList = new List<ProjectionDefinition<T>>();
                for (int i = 0; i < field.Length; i++)
                {
                    fieldList.Add(Builders<T>.Projection.Include(field[i].ToString()));
                }
                var projection = Builders<T>.Projection.Combine(fieldList);
                fieldList?.Clear();
                return await db.Find(filter).Project<T>(projection).FirstOrDefaultAsync();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// //条件查询集合
        ///var time = DateTime.Now;
        ///var list = new List<FilterDefinition<PhoneEntity>>();
        ///list.Add(Builders<PhoneEntity>.Filter.Lt("AddTime", time.AddDays(20)));
        ///list.Add(Builders<PhoneEntity>.Filter.Gt("AddTime", time));
        ///var filter = Builders<PhoneEntity>.Filter.And(list);
        /// //2.查询字段
        ///var field = new[] { "Name", "Price", "AddTime" };
        /// //3.排序字段
        ///var sort = Builders<PhoneEntity>.Sort.Descending("AddTime");
        ///var res = TMongodbHelper<PhoneEntity>.FindList(host, filter, field, sort);
        /// </summary>
        /// <param name="col"></param>
        /// <param name="filter"></param>
        /// <param name="field"></param>
        /// <param name="sort"></param>
        /// <returns></returns>
        public List<T> FindList(string col, FilterDefinition<T> filter, string[] field = null, SortDefinition<T> sort = null)
        {
            try
            {
                var db = _client._db.GetCollection<T>(col);
                //不指定查询字段
                if (field == null || field.Length == 0)
                {
                    if (sort == null) return db.Find(filter).ToList();
                    //进行排序
                    return db.Find(filter).Sort(sort).ToList();
                }
                //制定查询字段
                var fieldList = new List<ProjectionDefinition<T>>();
                for (int i = 0; i < field.Length; i++)
                {
                    fieldList.Add(Builders<T>.Projection.Include(field[i].ToString()));
                }
                var projection = Builders<T>.Projection.Combine(fieldList);
                fieldList?.Clear();
                if (sort == null) return db.Find(filter).Project<T>(projection).ToList();
                //排序查询
                return db.Find(filter).Sort(sort).Project<T>(projection).ToList();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// 异步条件查询集合
        /// </summary>
        /// <param name="col"></param>
        /// <param name="filter"></param>
        /// <param name="field"></param>
        /// <param name="sort"></param>
        /// <returns></returns>
        public async Task<List<T>> FindListAsync(string col, FilterDefinition<T> filter, string[] field = null, SortDefinition<T> sort = null)
        {
            try
            {
                var db = _client._db.GetCollection<T>(col);
                //不指定查询字段
                if (field == null || field.Length == 0)
                {
                    if (sort == null) return await db.Find(filter).ToListAsync();
                    return await db.Find(filter).Sort(sort).ToListAsync();
                }
                //制定查询字段
                var fieldList = new List<ProjectionDefinition<T>>();
                for (int i = 0; i < field.Length; i++)
                {
                    fieldList.Add(Builders<T>.Projection.Include(field[i].ToString()));
                }
                var projection = Builders<T>.Projection.Combine(fieldList);
                fieldList?.Clear();
                if (sort == null) return await db.Find(filter).Project<T>(projection).ToListAsync();
                //排序查询
                return await db.Find(filter).Sort(sort).Project<T>(projection).ToListAsync();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// 分页查询
        /// </summary>
        /// <param name="col"></param>
        /// <param name="filter"></param>
        /// <param name="pageIndex"></param>
        /// <param name="pageSize"></param>
        /// <param name="count"></param>
        /// <param name="field"></param>
        /// <param name="sort"></param>
        /// <returns></returns>
        public List<T> FindListByPage(string col, FilterDefinition<T> filter, int pageIndex, int pageSize, out long count, string[] field = null, SortDefinition<T> sort = null)
        {
            try
            {
                var db = _client._db.GetCollection<T>(col);
                count = db.Count(filter);
                //不指定查询字段
                if (field == null || field.Length == 0)
                {
                    if (sort == null) return db.Find(filter).Skip((pageIndex - 1) * pageSize).Limit(pageSize).ToList();
                    //进行排序
                    return db.Find(filter).Sort(sort).Skip((pageIndex - 1) * pageSize).Limit(pageSize).ToList();
                }

                //制定查询字段
                var fieldList = new List<ProjectionDefinition<T>>();
                for (int i = 0; i < field.Length; i++)
                {
                    fieldList.Add(Builders<T>.Projection.Include(field[i].ToString()));
                }
                var projection = Builders<T>.Projection.Combine(fieldList);
                fieldList?.Clear();

                //不排序
                if (sort == null) return db.Find(filter).Project<T>(projection).Skip((pageIndex - 1) * pageSize).Limit(pageSize).ToList();

                //排序查询
                return db.Find(filter).Sort(sort).Project<T>(projection).Skip((pageIndex - 1) * pageSize).Limit(pageSize).ToList();

            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// //异步分页查询，查询条件
        ///var time = DateTime.Now;
        ///var list = new List<FilterDefinition<PhoneEntity>>();
        ///list.Add(Builders<PhoneEntity>.Filter.Lt("AddTime", time.AddDays(400)));
        ///list.Add(Builders<PhoneEntity>.Filter.Gt("AddTime", time));
        ///var filter = Builders<PhoneEntity>.Filter.And(list);
        ///long count = 0;
        /// //排序条件
        ///var sort = Builders<PhoneEntity>.Sort.Descending("AddTime");
        ///var res = TMongodbHelper<PhoneEntity>.FindListByPage(host, filter, 2, 10, out count, null, sort);
        /// </summary>
        /// <param name="col"></param>
        /// <param name="filter">查询条件</param>
        /// <param name="pageIndex">当前页</param>
        /// <param name="pageSize">页容量</param>
        /// <param name="field">要查询的字段,不写时查询全部</param>
        /// <param name="sort">要排序的字段</param>
        /// <returns></returns>
        public async Task<List<T>> FindListByPageAsync(string col, FilterDefinition<T> filter, int pageIndex, int pageSize, string[] field = null, SortDefinition<T> sort = null)
        {
            try
            {
                var db = _client._db.GetCollection<T>(col);
                //不指定查询字段
                if (field == null || field.Length == 0)
                {
                    if (sort == null) return await db.Find(filter).Skip((pageIndex - 1) * pageSize).Limit(pageSize).ToListAsync();
                    //进行排序
                    return await db.Find(filter).Sort(sort).Skip((pageIndex - 1) * pageSize).Limit(pageSize).ToListAsync();
                }

                //制定查询字段
                var fieldList = new List<ProjectionDefinition<T>>();
                for (int i = 0; i < field.Length; i++)
                {
                    fieldList.Add(Builders<T>.Projection.Include(field[i].ToString()));
                }
                var projection = Builders<T>.Projection.Combine(fieldList);
                fieldList?.Clear();

                //不排序
                if (sort == null) return await db.Find(filter).Project<T>(projection).Skip((pageIndex - 1) * pageSize).Limit(pageSize).ToListAsync();

                //排序查询
                return await db.Find(filter).Sort(sort).Project<T>(projection).Skip((pageIndex - 1) * pageSize).Limit(pageSize).ToListAsync();

            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion

        #region  GridFS分布式文件存储系统（GridFS 使用两个集合来存储一个文件：fs.files与fs.chunks。）
        public string UploadByByte(string filename, byte[] source) {
            string oid = "";
            try
            {
                var ObjectId = _client._fs.UploadFromBytes(filename, source);

                if (ObjectId != null)
                {
                    oid = ObjectId.ToString();
                }
            }
            catch (Exception ex) {
                throw ex;
            }
            return oid;
        }
        public string UploadByStream(string filename, Stream stream)
        {
            string oid = "";
            try
            {
                var ObjectId = _client._fs.UploadFromStream(filename, stream);

                if (ObjectId != null)
                {
                    oid = ObjectId.ToString();
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return oid;
        }

        public int DeleteFile(string id) {
            int result = 0;
            try
            {
                _client._fs.Delete(ObjectId.Parse(id));
                result = 1;
            }
            catch (Exception ex) {
                throw ex;
            }
            return result;
        }

        public byte[] DownloadToByteByObjecId(ObjectId obj) {
            byte[] file;
            try {
                file = _client._fs.DownloadAsBytes(obj);
            } catch (Exception ex) {
                throw ex;
            }
            return file;
        }

        public int DownloadToStreamByObjectId(ObjectId obj, Stream file) {
            int i = 0;
            try
            {
                _client._fs.DownloadToStream(obj, file);
                i = 1;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return i;
        }

        public Stream OpenDownToStreamByObjectId(ObjectId obj) {
            GridFSDownloadStream stream = null;
            try
            {
                stream=_client._fs.OpenDownloadStream(obj);
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return stream;
        }
        #endregion
    }
}
