import asyncio
from datetime import datetime, timedelta
from motor.motor_asyncio import AsyncIOMotorClient
from bson.binary import Binary
from django.core.cache.backends.base import BaseCache
from django.utils.encoding import force_bytes, force_str
from pymongo.errors import PyMongoError

class MongoDBCacheBackend(BaseCache):
    def __init__(self, host="localhost", port=27017, db_name="cache_db", collection_name="cache", **kwargs):
        super().__init__(**kwargs)
        self.client = AsyncIOMotorClient(host, port)
        self.collection = self.client[db_name][collection_name]
        asyncio.create_task(self._create_index())  # 异步创建索引

    async def _create_index(self):
        # 创建 TTL 索引，用于自动删除过期的缓存项
        await self.collection.create_index("expires_at", expireAfterSeconds=0)
        await self.collection.create_index("key", unique=True)  # 用于快速查找键

    async def set(self, key, value, timeout=None, version=None):
        key = self.make_key(key, version)
        timeout = self.get_backend_timeout(timeout)
        expires_at = datetime.utcnow() + timedelta(seconds=timeout) if timeout else None

        try:
            await self.collection.update_one(
                {"key": key},
                {
                    "$set": {
                        "value": Binary(force_bytes(value)),
                        "expires_at": expires_at,
                    }
                },
                upsert=True,
            )
        except PyMongoError as e:
            print(f"Set operation failed: {e}")
            return False
        return True

    async def get(self, key, default=None, version=None):
        key = self.make_key(key, version)
        try:
            document = await self.collection.find_one({"key": key})
            if document:
                if document.get("expires_at") is None or document["expires_at"] > datetime.utcnow():
                    return force_str(document["value"])
                else:
                    await self.delete(key)  # 删除过期缓存
            return default
        except PyMongoError as e:
            print(f"Get operation failed: {e}")
            return default

    async def delete(self, key, version=None):
        key = self.make_key(key, version)
        try:
            result = await self.collection.delete_one({"key": key})
            return result.deleted_count > 0
        except PyMongoError as e:
            print(f"Delete operation failed: {e}")
            return False

    async def clear(self):
        """清空缓存"""
        try:
            await self.collection.delete_many({})
        except PyMongoError as e:
            print(f"Clear operation failed: {e}")
            return False
        return True

    async def get_many(self, keys, version=None):
        key_dict = {key: self.make_key(key, version) for key in keys}
        try:
            results = await self.collection.find({"key": {"$in": list(key_dict.values())}}).to_list(length=None)
            values = {}
            for result in results:
                if result.get("expires_at") is None or result["expires_at"] > datetime.utcnow():
                    values[result["key"]] = force_str(result["value"])
            return {key: values.get(key_dict[key]) for key in keys}
        except PyMongoError as e:
            print(f"Get many operation failed: {e}")
            return {key: None for key in keys}
