import hashlib
from datetime import datetime, timedelta
from typing import Dict, Any, List

from bson import Binary, BSON
from django.core.cache.backends.base import BaseCache
from pymongo import MongoClient, ASCENDING
from pymongo.errors import PyMongoError, DuplicateKeyError
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError

from mongo_factory import MongoDBConnectionFactory


class MongoDBCacheBackend(BaseCache):
    def __init__(self, server: str, params: Dict[str, Any]):
        super().__init__(params)
        self._server = server  # 外部的 LOCATION 'mongodb://localhost:27017/'
        self._params = params
        self._client = None  # TODO： django-redis 提供了多个 [None] * len(self._server) 防止redis报错，支持多路由配置
        self._collection = None

        options = params.get('options', {})
        self._database_name = options.get('DATABASE_NAME', "django_cache_db")
        self._collection_name = options.get('COLLECTION_NAME', "django_cache_collection")

        self.connection_factory = MongoDBConnectionFactory(params)

    @staticmethod
    def _split_value(value, chunk_size=16 * 1024 * 1024):
        """将值拆分为多个块"""
        if isinstance(value, bytes) and len(value) > chunk_size:
            return [value[i:i + chunk_size] for i in range(0, len(value), chunk_size)]
        return [value]  # 返回一个单块的列表

    # @staticmethod
    # def _generate_shard_key(self, key):
    #     """生成分片键，基于主键的前缀或特定逻辑"""
    #     return key[:10]  # 示例：取 key 的前 10 个字符作为分片键

    @staticmethod
    def _generate_shard_key(key):
        """生成哈希分片键"""
        return hashlib.sha256(key.encode()).hexdigest()[:10]  # 使用哈希前10个字符作为分片键

    @property
    def client(self) -> MongoClient:
        # TODO: django-redis 对应的 get_client 方法，是否需要改为 get_client方法，每次都需要调用才对？
        #       目的是为了在项目启动后，多个请求的 cache 共用连接池，将连接池的给到 每个请求
        # TODO: 如果使用 django-redis的_client多配置，此处不能设置 property
        if self._client is None:
            # self._client = MongoClient(self._server)
            self._client = self.connect()
        return self._client

    @property
    def collection(self):

        if self._collection is None:
            # TODO：考虑下是在连接池就导入，还是此处，此处好。
            self._collection = self.client[self._database_name][self._collection_name]
            self._initialize_sharding()  # 分片检查创建
            try:
                self._collection.create_index([("expires_at", ASCENDING)], expireAfterSeconds=0)
                self._collection.create_index([("shard_key", ASCENDING)])  # TODO： 和下面的保持一致，添加分片键索引
            except DuplicateKeyError:
                pass
        return self._collection

    def _initialize_sharding(self):  # TODO： 待商榷
        """检查并启用分片功能"""

        # 检查数据库是否已启用分片
        if not self.client[self._database_name].command("getCmdLineOpts").get("sharding"):
            raise RuntimeError("Sharding is not enabled on the MongoDB server.")

        # 检查集合是否已设置为分片
        shard_status = self.client[self._database_name].command("listShards")
        if not shard_status.get("sharded"):
            try:
                # 启用数据库的分片功能
                self._client.admin.command("enableSharding", self._database_name)
                # 将集合设置为分片集合，以 `_id` 为分片键
                self._client.admin.command("shardCollection",
                                           f"{self._database_name}.{self._collection_name}",
                                           key={"_id": "hashed"})
            except PyMongoError as e:
                raise RuntimeError(f"Failed to initialize sharding: {e}")

    def connect(self):
        return self.connection_factory.connect(self._server)

    def _assemble_value(self, key):
        """根据键组装所有相关的值块"""
        chunks = self.collection.find({"_id": {"$regex": f"^{key}_chunk_"}})
        return b''.join(chunk['value'] for chunk in chunks)

    def add(self, key, value, timeout=None, version=None):
        if self.get(key, version=version) is None:
            return self.set(key, value, timeout, version)
        return False

    def get(self, key, default=None, version=None):
        self._delete_expired()  # 清理过期数据
        if self.collection.find_one({"_id": key}):
            return self._assemble_value(key)
        return default

    # def set(self, key, value, timeout=None, version=None):
    #     timeout = self.get_backend_timeout(timeout)
    #     expires_at = datetime.utcnow() + timedelta(seconds=timeout) if timeout else None
    #
    #     # 拆分值并存储每个块
    #     chunks = self._split_value(value)
    #     operations = []
    #
    #     shard_key = self._generate_shard_key(key)  # 生成分片键
    #
    #     for i, chunk in enumerate(chunks):
    #         chunk_key = f"{key}_chunk_{i}"
    #         operations.append({
    #             "updateOne": {
    #                 "filter": {"_id": chunk_key},
    #                 "update": {"$set": {
    #                     "value": Binary(BSON.encode(chunk)),
    #                     "expires_at": expires_at,
    #                     "shard_key": shard_key  # 存储分片键
    #                 }},
    #                 "upsert": True
    #             }
    #         })
    #
    #     # 批量写入所有块
    #     try:
    #         self.collection.bulk_write(operations)
    #     except PyMongoError:
    #         return False
    #     return True
    #
    # def set_many(self, data: Dict[str, Any], timeout=None, version=None):
    #     timeout = self.get_backend_timeout(timeout)
    #     expires_at = datetime.utcnow() + timedelta(seconds=timeout) if timeout else None
    #
    #     operations = []
    #     for key, value in data.items():
    #         chunks = self._split_value(value)
    #         shard_key = self._generate_shard_key(key)  # 生成分片键
    #         for i, chunk in enumerate(chunks):
    #             chunk_key = f"{key}_chunk_{i}"
    #             operations.append({
    #                 "updateOne": {
    #                     "filter": {"_id": chunk_key},
    #                     "update": {"$set": {
    #                         "value": Binary(BSON.encode(chunk)),
    #                         "expires_at": expires_at,
    #                         "shard_key": shard_key  # 存储分片键
    #                     }},
    #                     "upsert": True
    #                 }
    #             })
    #
    #     if operations:
    #         try:
    #             self.collection.bulk_write(operations)
    #         except PyMongoError as e:
    #             print(f"Error during bulk write: {e}")
    #             return False
    #     return True

    def _build_operations(self, data: Dict[str, Any], timeout: int):
        expires_at = datetime.utcnow() + timedelta(seconds=timeout) if timeout else None
        operations = []

        for key, value in data.items():
            shard_key = self._generate_shard_key(key)  # 生成分片键
            chunks = self._split_value(value)

            for i, chunk in enumerate(chunks):
                chunk_key = f"{key}_chunk_{i}"
                operations.append(UpdateOne(
                    {"_id": chunk_key},
                    {"$set": {
                        "value": Binary(BSON.encode(chunk)),
                        "expires_at": expires_at,
                        "shard_key": shard_key
                    }},
                    upsert=True
                ))
                # TODO: 大问题：如果是更新操作，且第二次的 key 对应的分割少于第一次，第一次中的其他分割如何处理

        return operations

    def set(self, key, value, timeout=None, version=None):
        timeout = self.get_backend_timeout(timeout)
        operations = self._build_operations({key: value}, timeout)

        try:
            if operations:
                self.collection.bulk_write(operations)
        except BulkWriteError as e:
            print(f"Error during bulk write: {e}")
            return False
        return True

    def set_many(self, data: Dict[str, Any], timeout=None, version=None):
        timeout = self.get_backend_timeout(timeout)
        operations = self._build_operations(data, timeout)

        if operations:
            try:
                self.collection.bulk_write(operations)
            except BulkWriteError as e:
                print(f"Error during bulk write: {e}")
                return False
        return True

    def get_many(self, keys: List[str], version=None) -> Dict[str, Any]:
        self._delete_expired()  # 清理过期数据  # TODO：TTL自动清理存在延迟
        results = {}

        for key in keys:
            assembled_value = self._assemble_value(key)
            if assembled_value:
                results[key] = BSON(assembled_value).decode()  # 解码结果
            else:
                results[key] = None  # 如果未找到则返回 None

        return results

    def delete(self, key, version=None):
        # 删除所有与键相关的块
        self.collection.delete_many({"_id": {"$regex": f"^{key}"}})

    def delete_many(self, keys: List[str], version=None):
        for key in keys:
            self.delete(key, version)

    def clear(self):
        self.collection.delete_many({})

    def _delete_expired(self):
        # TODO: 外部可继承，设置额外的业务清理逻辑
        self.collection.delete_many({"expires_at": {"$lte": datetime.utcnow()}})


"""
TODO:
"""
def _delete_existing_chunks(self, key):
    # 删除旧的分割
    self.collection.delete_many({"_id": {"$regex": f"^{key}_chunk_.*"}})

def set(self, key, value, timeout=None, version=None):
    timeout = self.get_backend_timeout(timeout)
    self._delete_existing_chunks(key)  # 删除旧的分割
    operations = self._build_operations({key: value}, timeout)

    try:
        if operations:
            self.collection.bulk_write(operations)
    except BulkWriteError as e:
        print(f"Error during bulk write: {e}")
        return False
    return True

# 标记删除
def _mark_chunks_for_deletion(self, key):
    # 标记为删除
    self.collection.update_many(
        {"_id": {"$regex": f"^{key}_chunk_.*"}},
        {"$set": {"deleted": True}}
    )

def set(self, key, value, timeout=None, version=None):
    timeout = self.get_backend_timeout(timeout)
    self._mark_chunks_for_deletion(key)  # 标记旧的分割
    operations = self._build_operations({key: value}, timeout)

    try:
        if operations:
            self.collection.bulk_write(operations)
    except BulkWriteError as e:
        print(f"Error during bulk write: {e}")
        return False
    return True

def clear_marked_chunks(self):
    # 清理标记为删除的分割
    self.collection.delete_many({"deleted": True})

def get(self, key, default=None, version=None):
    self._delete_expired()  # 清理过期数据
    # 清理未使用的分割
    self.collection.delete_many({"_id": {"$regex": f"^{key}_chunk_.*"}, "deleted": True})

    results = self.collection.find({"_id": {"$regex": f"^{key}_chunk_.*"}})
    if results:
        # 合并分割值
        value = self._merge_chunks(results)
        return value

    return default





# factory.py
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from typing import Dict, Any



class MongoDBConnectionFactory:
    """
    由于 Django 会为每次请求新建cache实例，此处进程级别建立维护连接池
    """
    _pools: Dict[str, MongoClient] = {}

    def __init__(self, options: Dict[str, Any]):
        """
        client_kwargs 中存储 MongoClient 初始化的相关参数， 如

        :param options:
        """
        self.options = options
        self.client_kwargs = options.get("CLIENT_KWARGS", {})
        # 连接池参数 MongoClient 客户端默认支持线程池，无需专门的线程池工具类
        self._max_pool_size = self.client_kwargs.get('MAX_POOL_SIZE', 100)  # 最大连接数
        self._min_pool_size = self.client_kwargs.get('MIN_POOL_SIZE', 10)    # 最小连接数
        self._max_idle_time = self.client_kwargs.get('MAX_IDLE_TIME', 300)   # 最大空闲时间

    def make_connection_params(self, uri: str) -> Dict[str, Any]:
        """
        根据传入的 URI 构建连接参数字典。
        """
        params = {"uri": uri}

        username = self.options.get("USERNAME")
        password = self.options.get("PASSWORD")
        if username and password:
            params["username"] = username
            params["password"] = password

        return params

    def connect(self, uri: str) -> MongoClient:
        """
        返回一个新的 MongoDB 连接。
        """
        params = self.make_connection_params(uri)
        connection = self.get_connection(params)
        return connection

    def disconnect(self, connection: MongoClient):
        """
        断开与 MongoDB 服务器的连接。
        """
        connection.close()

    def get_connection(self, params: Dict[str, Any]) -> MongoClient:
        """
        返回一个新的 MongoDB 连接。
        """
        pool = self.get_or_create_connection_pool(params)
        return pool

    def get_or_create_connection_pool(self, params: Dict[str, Any]) -> MongoClient:
        """
        返回现有的连接池或创建一个新的连接池。
        """
        key = params["uri"]
        if key not in self._pools:
            self._pools[key] = self.create_connection_pool(params)
        return self._pools[key]

    def create_connection_pool(self, params: Dict[str, Any]) -> MongoClient:
        """
        创建一个新的 MongoDB 连接池。
        """
        uri = params["uri"]
        client = MongoClient(uri,
                             maxPoolSize=self._max_pool_size,
                             minPoolSize=self._min_pool_size,
                             maxIdleTimeMS=self._max_idle_time * 1000,  # 转换为毫秒
                             **self.client_kwargs)

        # 测试连接
        # TODO: 去除，或者异常处理，外部目前没有异常处理
        try:
            client.admin.command('ping')
        except ConnectionFailure:
            raise Exception("Failed to connect to MongoDB.")

        return client
