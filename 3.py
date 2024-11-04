from datetime import datetime, timedelta
from typing import Dict, Any

from bson import Binary, BSON
from django.core.cache.backends.base import BaseCache
from pymongo import MongoClient, ASCENDING
from pymongo.errors import PyMongoError, DuplicateKeyError


class MongoDBCacheBackend(BaseCache):
    def __init__(self, server: str, params: Dict[str, Any]):
        super().__init__(params)
        self._server = server
        self._params = params
        self._client = None
        self._collection = None

        options = params.get('options', {})
        self._database_name = options.get('DATABASE_NAME', "django_cache_db")
        self._collection_name = options.get('COLLECTION_NAME', "django_cache_collection")

    @property
    def client(self) -> MongoClient:
        if self._client is None:
            self._client = MongoClient(self._server)
        return self._client

    @property
    def collection(self):
        if self._collection is None:
            self._collection = self.client[self._database_name][self._collection_name]
            try:
                self._collection.create_index([("expires_at", ASCENDING)], expireAfterSeconds=0)
                """ttl 索引性能较低，文档的删除不是实时的，而是定期进行的，可能存在几秒的延迟"""
            except DuplicateKeyError:
                pass
        return self._collection

    def add(self, key, value, timeout=None, version=None):
        if self.get(key, version=version) is None:
            return self.set(key, value, timeout, version)
        return False

    def get(self, key, default=None, version=None):
        self._delete_expired()  # 清理过期数据
        result = self.collection.find_one({"_id": self.make_key(key, version)})
        if result and (result["expires_at"] is None or result["expires_at"] > datetime.utcnow()):
            return BSON(result["value"]).decode()  # 使用 BSON 解码
        return default

    def set(self, key, value, timeout=None, version=None):
        key = self.make_key(key, version)
        timeout = self.get_backend_timeout(timeout)
        expires_at = datetime.utcnow() + timedelta(seconds=timeout) if timeout else None
        try:
            # 使用 BSON 编码
            self.collection.update_one(
                {"_id": key},
                {"$set": {"value": Binary(BSON.encode(value)), "expires_at": expires_at}},
                upsert=True,
            )
        except PyMongoError:
            return False
        return True

    def set_many(self, data, timeout=None, version=None):
        timeout = self.get_backend_timeout(timeout)
        expires_at = datetime.utcnow() + timedelta(seconds=timeout) if timeout else None

        # 构建批量操作
        operations = []
        for key, value in data.items():
            key = self.make_key(key, version)
            operations.append(
                {
                    "updateOne": {
                        "filter": {"_id": key},
                        "update": {"$set": {"value": pickle.dumps(value), "expires_at": expires_at}},
                        "upsert": True
                    }
                }
            )

        # 批量执行
        if operations:
            try:
                self._collection.bulk_write(operations)
            except PyMongoError as e:
                print(f"Error during bulk write: {e}")
                return False
        return True

    def get_many(self, keys, version=None):
        self._delete_expired()  # 清理过期数据
        key_dict = {key: self.make_key(key, version) for key in keys}
        results = self._collection.find({"_id": {"$in": list(key_dict.values())}})  # 过滤字段添加索引

        values = {}
        for result in results:
            if result["expires_at"] is None or result["expires_at"] > datetime.utcnow():
                values[result["_id"]] = pickle.loads(result["value"])  # 使用 pickle 反序列化

        # 返回键值对
        return {key: values.get(key_dict[key]) for key in keys}

    def get_many(self, keys, version=None):
        self._delete_expired()  # 清理过期数据

        # 生成要查找的键的 ID
        key_dict = {key: self.make_key(key, version) for key in keys}

        # 使用聚合框架进行查询和处理
        pipeline = [
            {
                "$match": {
                    "_id": {"$in": list(key_dict.values())}  # 匹配所有需要的键
                }
            },
            {
                "$match": {
                    "$or": [  # 过滤过期项
                        {"expires_at": None},
                        {"expires_at": {"$gt": datetime.utcnow()}}
                    ]
                }
            },
            {
                "$project": {
                    "key": "$_id",
                    "value": {"$cond": {
                        "if": {"$gte": ["$expires_at", datetime.utcnow()]},
                        "then": "$value",
                        "else": None
                    }}
                }
            }
        ]

        results = self.collection.aggregate(pipeline)

        # 将结果组装成最终字典
        values = {result["key"]: pickle.loads(result["value"]) for result in results if result["value"] is not None}

        # 返回最终结果
        return {key: values.get(key_dict[key]) for key in keys}

    def delete(self, key, version=None):
        self.collection.delete_one({"_id": self.make_key(key, version)})

    def clear(self):
        self.collection.delete_many({})

    def _delete_expired(self):
        self.collection.delete_many({"expires_at": {"$lte": datetime.utcnow()}})



##### 分块聚合

def get_many(self, keys, version=None):
    self._delete_expired()  # 清理过期数据

    # 构建正则表达式以批量获取相关的分块
    key_patterns = [f"^{key}_chunk_" for key in keys]
    regex_pattern = "|".join(key_patterns)
    
    # 执行批量查询
    results = self.collection.find({"_id": {"$regex": regex_pattern}})

    # 将结果按键分类
    chunk_map = {}
    for result in results:
        key_prefix = result["_id"].split("_chunk_")[0]
        if key_prefix not in chunk_map:
            chunk_map[key_prefix] = []
        chunk_map[key_prefix].append(result['value'])

    # 合并分块并构建最终结果
    values = {}
    for key in keys:
        if key in chunk_map:
            values[key] = b''.join(chunk_map[key])  # 合并分块
        else:
            values[key] = None  # 如果没有找到对应的分块

    return values
