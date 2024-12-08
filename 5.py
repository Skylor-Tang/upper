import pickle
from datetime import datetime, timedelta
from pymongo.errors import PyMongoError
from concurrent.futures import ThreadPoolExecutor

class MongoCacheBackend:
    def __init__(self, collection):
        self._collection = collection

    def make_key(self, key, version):
        # 假设这是生成缓存 key 的函数
        return f"{key}_{version}"

    def get_backend_timeout(self, timeout):
        # 假设这是获取超时时间的函数
        return timeout or 60

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

        # 将批量操作分成多个批次，防止单次批量操作过大
        batch_size = 100  # 每个线程处理的批量操作大小
        batches = [operations[i:i + batch_size] for i in range(0, len(operations), batch_size)]

        # 使用线程池执行并发操作
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(self._execute_batch, batch) for batch in batches]
            
            for future in futures:
                try:
                    future.result()  # 等待每个线程的执行结果
                except Exception as e:
                    print(f"Error during batch execution: {e}")
                    return False

        return True

    def _execute_batch(self, batch):
        try:
            if batch:
                self._collection.bulk_write(batch)
        except PyMongoError as e:
            print(f"Error during bulk write: {e}")
            raise


Django 服务缓存存储迁移报告（使用）
功能的必要性
1. 业务背景：


在当前的业务场景中，我们的 Django 服务依赖 Redis 作为缓存存储来提高性能（主要用于处理分支信息，通过定时任务将处理好的分支信息进行存储，减少调用时候的查表依赖，使用的是 mysql 数据库，有性能问题，接口 3 秒不达标），缓存可以显著减少数据库负载。然而，随着业务分支不断增长，随着缓存键值的增加，Redis 集群的内存占用不断攀升，
特别是在 Redis 中存储了大量过期时间较短（2天）的缓存数据，具体包括大约 300 万个键值对。缓存键大致分为如下几种：
	• 分布式锁缓存： 由于涉及到并发操作，这类数据的过期时间较短，通常为几分钟至几小时。
	• 周期性更新的缓存： 这些缓存数据的过期时间为 2 天，每天会通过定时任务或者接口调用刷新部分缓存数据。

随着业务的不断发展，Redis 中存储的这些 300 万个缓存键，虽然过期时间较短，但由于需要频繁刷新，导致 Redis 内存占用过高，并且 Redis 集群的性能受到影响。尤其是在高并发场景下，频繁的缓存刷新和过期处理对 Redis 的性能和稳定性带来了巨大的压力。
为了应对这一挑战，决定将这些 过期时间为 2 天的缓存数据 从 Redis 迁移到 MongoDB，以减少 Redis 的内存压力，同时利用 MongoDB 更适合存储大规模数据的优势，从而优化整个缓存存储策略。



现有问题总结：
	• Redis 内存占用过高： 由于大量数据存储在 Redis 中，导致 Redis 内存占用不断增加，给 Redis 集群带来负担。
	• 性能下降： 由于频繁的缓存刷新和过期数据处理，Redis 的性能受到影响，导致缓存命中率下降和请求响应时间增加。
	• 高昂的成本： 随着数据量的增加，需要扩展 Redis 集群来应对高内存消耗，导致基础设施成本持续上升。



切换策略
如何实现迁移：
为了减轻 Redis 的负担，我们决定将这 300 万个过期时间为 2 天的缓存键从 Redis 迁移至 MongoDB。以下是详细的迁移步骤：MongoDB 具有灵活的存储模型和相对低廉的存储成本，非常适合大规模缓存数据的存储。以下是详细的迁移步骤：
	1. 分析迁移的数据：
		○ 主要迁移的是 过期时间为 2 天的缓存数据，这些缓存数据并不需要 Redis 提供的高频访问速度，而 MongoDB 更适合存储大量具有较长过期时间的数据。
	2. 实现 Django Cache 接口：
		○ 通过实现 Django 的缓存接口，定制化 MongoDB 存储后端。可以继承 Django 的 django.core.cache.backends.base.BaseCache 类，重写必要的方法（如 get、set、delete 等）来实现与 MongoDB 的交互。
	3. 设计 MongoDB 存储方案：
		○ 在 MongoDB 中为这些缓存数据设计适当的数据结构，使用 MongoDB 提供的 TTL（Time To Live） 特性来自动管理缓存的过期，确保缓存自动失效，不需要额外的手动清理机制。
	4. 数据迁移：
		○ 数据迁移步骤： 采用批量迁移的方式，逐步将 Redis 中的 300 万个缓存键迁移至 MongoDB。可以通过定时任务进行迁移，先从较低频率的缓存开始迁移，逐步迁移到高频缓存。
		○ 并行迁移： 利用多线程或异步任务并行处理大规模数据迁移，确保迁移过程中的高效性和稳定性。   --- TODO： 重写
	5. 缓存策略调整：
		○ 在迁移完成后，Redis 仍然用于存储一些频繁访问的数据和分布式锁缓存，而 MongoDB 存储的将是过期时间较长、更新频率较低的数据。这样可以确保 Redis 不再承载大量需要频繁刷新缓存的压力。
	6. 逐步替换存储后端：
		○ 在实现 MongoDB 存储后端时，首先进行小范围测试，确认无性能问题后再全面迁移，并且在 Django 项目中进行相应的配置切换，使得缓存存储可根据需求选择不同的后端。
	7. 监控与性能优化：
		○ 配置合适的缓存命中率、存储量、过期数据的删除等监控项，及时发现可能的性能瓶颈。定期进行缓存更新策略优化，确保缓存使用的高效性。


优化策略
实现过程中出现的问题及解决方案：
	1. 缓存查询性能问题：
		○ 问题： MongoDB 查询性能相较于 Redis 较差，尤其在大规模数据量的情况下，可能会影响查询的响应时间。
		○ 解决方案： 在 MongoDB 中设计合理的索引结构，确保查询性能不受到大规模数据量的影响。同时，可以对 MongoDB 使用 TTL 功能，自动过期清理缓存数据，避免 MongoDB 存储的数据量过大，影响查询效率。
	2. 存储容量管理：
		○ 问题： MongoDB 中的数据量逐渐增多，可能导致存储容量不足或性能问题。
		○ 解决方案： 利用 MongoDB 的 TTL 功能，自动清除过期数据，避免长期存储无用缓存。同时，根据数据的访问模式和业务需求动态调整缓存存储策略。
	3. 线程并发写
		


成果
迁移之后的性能提升或者好处：
	1. Redis 内存占用减少：
		○ 通过将大规模的过期时间为 2 天的缓存数据迁移至 MongoDB，Redis 内存占用显著减少，从而减轻了 Redis 集群的内存压力，降低了 Redis 的硬件扩容需求。
	2. 降低硬件成本：
		○ MongoDB 存储的成本相对较低，通过将冷数据存储在 MongoDB 中，减少了对 Redis 集群扩展的需求，降低了基础设施的硬件成本。
	3. 优化系统性能：
		○ 通过合理分配 Redis 和 MongoDB 的存储负载，系统整体性能得到优化。Redis 现在专注于存储热数据和分布式锁，缓存命中率提高，系统的响应速度和吞吐量得到了改善。MongoDB 用于存储大量的冷数据，不再对 Redis 的性能产生影响。
	4. 减少缓存刷新负担：
		○ 原本需要频繁刷新的 300 万个缓存键现在迁移到 MongoDB 后，Redis 中不再承载大量的周期性刷新任务，缓存刷新负担大幅减轻，Redis 的负载得到了有效分配。
	5. 更灵活的缓存管理：
		○ 通过 Django Cache 的多后端支持，缓存管理变得更加灵活，支持按需切换缓存存储后端，便于后续扩展和调整。
	6. 高可用性和稳定性：
		○ 数据迁移后，Redis 和 MongoDB 分担缓存负载，避免了单点故障的影响，提高了系统的高可用性和稳定性。

总结： 通过将 300 万个过期时间为 2 天的缓存数据从 Redis 迁移到 MongoDB，我们成功降低了 Redis 的内存压力，优化了缓存策略，并提升了整个系统的性能和可扩展性。这一迁移不仅降低了硬件成本，还提高了系统的稳定性，为未来的业务增长提供了更强的支持。



mysql 的优势：
使用 MongoDB 作为冷数据缓存而不是 MySQL，主要是因为 MongoDB 在某些场景下比 MySQL 更具优势，特别是针对大规模缓存存储、高并发读写以及灵活的存储结构等需求。以下是 MongoDB 相比 MySQL 的一些主要优势：
1. 性能与扩展性：
	• MongoDB：
MongoDB 是一个 NoSQL 数据库，设计时即考虑了高并发、大数据量的存储需求。它提供了 水平扩展（sharding）和 自动分片 的能力，能够将数据分布到多个服务器上，支持高并发的读写操作。因此，在需要高吞吐量、分布式缓存的场景下，MongoDB 的性能通常会优于 MySQL。
	• MySQL：
MySQL 是一个关系型数据库，虽然可以通过 主从复制 或 分区表 等方式来扩展，但在大规模数据和高并发查询时，性能不如 MongoDB，因为它的查询和写入操作通常需要更多的资源进行锁管理和事务处理。
2. 数据模型和灵活性：
	• MongoDB：
MongoDB 使用 文档存储（Document-Oriented Storage）模型，数据以 BSON 格式存储，支持灵活的模式（schema-less）。这意味着你不需要预定义数据库表结构，可以在运行时根据需要动态扩展字段。这种灵活性非常适合缓存场景，特别是当缓存的数据结构复杂且变化较快时。
在缓存场景下，存储的键值对可能有不同的字段或者某些字段会动态变化，MongoDB 能够更容易适应这些变化，不需要像 MySQL 那样重新设计表结构或调整字段。
	• MySQL：
MySQL 使用传统的 表格 和 行列模型，要求严格的表结构，并且每次修改表结构时需要执行 ALTER 操作，可能会影响性能。如果数据结构有较大变动，可能需要重构整个数据库设计。因此，MySQL 在这种高度动态、数据结构经常变动的缓存场景下会显得不那么灵活。
3. 缓存失效和过期机制：
	• MongoDB：
MongoDB 提供了 TTL（Time To Live） 索引功能，允许你为文档设置自动过期时间。当文档的过期时间到达时，MongoDB 会自动删除这些文档。这对于缓存系统来说非常重要，可以避免过期数据的积累，并且无需额外的清理任务。
优点： 在缓存系统中，数据通常会有生命周期，TTL 索引可以自动清理过期数据，避免需要手动清理缓存，这对减少数据库管理成本、提高效率非常有帮助。
	• MySQL：
MySQL 本身并不提供内建的缓存过期机制。虽然可以通过定时任务清理过期数据，但缺乏像 MongoDB TTL 这样的自动化过期机制。对于缓存数据，通常需要开发人员手动管理缓存的过期时间，并通过应用程序逻辑或外部工具进行清理。
4. 高并发处理：
	• MongoDB：
MongoDB 提供了 无锁操作 和 分布式事务 的支持，能够更高效地处理高并发请求。对于需要频繁访问和更新缓存数据的场景，MongoDB 能够提供更好的并发处理能力。
	• MySQL：
MySQL 使用的是 行级锁 和 表级锁，在高并发情况下容易出现性能瓶颈，特别是在需要频繁写入和读取的数据场景下，锁的争用会导致性能下降。而且在高并发的情况下，MySQL 的 事务处理 可能会对性能产生较大的影响，尤其是当数据量较大时。
5. 读写优化：
	• MongoDB：
MongoDB 的设计优化了 读多写少 的场景，可以进行 复制集 配置，从而分散读操作的压力。并且通过 写时合并（Write Concern）机制，优化了对缓存的高效写入。
	• MySQL：
MySQL 更适合传统的 ACID 事务管理，它在处理大量写操作时的性能表现通常不如 MongoDB。对于缓存这种常常需要快速读写的场景，MySQL 在吞吐量和延迟方面可能存在瓶颈，尤其是当缓存数据量较大时。
6. 数据一致性和事务支持：
	• MongoDB：
MongoDB 在 副本集（Replica Set） 和 分片（Sharding） 环境中，能够提供良好的高可用性和容错能力，数据的一致性保证较为灵活，支持最终一致性。虽然 MongoDB 在 4.x 版本后支持多文档事务，但一般来说，它的事务支持适合于高并发读写、写操作较多的场景，且不如 MySQL 严格。
	• MySQL：
MySQL 在事务一致性方面提供了更强的保障（遵循 ACID），非常适合于强一致性需求的应用。对于一些场景，MySQL 提供的事务支持可能更合适，但对于缓存场景来说，过于严格的事务处理反而会影响性能。
7. 成本效益：
	• MongoDB：
由于 MongoDB 的扩展性和存储效率较高，它在存储大规模冷数据时，通常具有更好的性价比。MongoDB 可以通过 分片存储 来优化存储，并且对大规模数据的读写处理更加高效。
	• MySQL：
在高并发、高存储需求的缓存场景中，MySQL 的性能可能无法与 MongoDB 相比，扩展成本也可能较高，特别是在需要进行大量写入操作时，MySQL 的硬件要求较为苛刻。
总结
使用 MongoDB 而不是 MySQL 作为冷数据缓存的主要优势是其 高性能、高并发处理、灵活的存储结构、自动过期机制（TTL） 和 易于水平扩展 等特点。MongoDB 的设计非常适合处理大量数据的存储和管理，尤其是缓存这种 大规模、生命周期较短的数据，而 MySQL 更适合用于强一致性要求的事务型应用。
因此，对于需要高性能、低延迟、并且数据结构灵活的缓存场景，MongoDB 是一个更合适的选择，特别是在数据量庞大且不断增长的情况下，MongoDB 提供的水平扩展能力和自动过期机制是非常有价值的。
