package com.redislabs.university.RU102J.dao;

import com.google.common.collect.ImmutableMap;
import com.redislabs.university.RU102J.api.MeterReading;
import redis.clients.jedis.*;

import java.nio.channels.Pipe;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FeedDaoRedisImpl implements FeedDao {

    private final JedisPool jedisPool;
    private static final long globalMaxFeedLength = 10000;
    private static final long siteMaxFeedLength = 2440;

    public FeedDaoRedisImpl(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    // Challenge #6
    @Override
    public void insert(MeterReading meterReading) {
        // START Challenge #6
        try (Jedis jedis = jedisPool.getResource()) {
            final String feedKey = RedisSchema.getFeedKey(meterReading.getSiteId());
            final String globalFeedKey = RedisSchema.getGlobalFeedKey();
            final Map<String, String> reading = meterReading.toMap();

            jedis.xadd(feedKey,StreamEntryID.NEW_ENTRY,reading,siteMaxFeedLength,true);
            jedis.xadd(globalFeedKey,StreamEntryID.NEW_ENTRY,reading,globalMaxFeedLength,true);
        }
        // END Challenge #6
    }

    @Override
    public List<MeterReading> getRecentGlobal(int limit) {
        return getRecent(RedisSchema.getGlobalFeedKey(), limit);
    }

    @Override
    public List<MeterReading> getRecentForSite(long siteId, int limit) {
        return getRecent(RedisSchema.getFeedKey(siteId), limit);
    }

    public List<MeterReading> getRecent(String key, int limit) {
        List<MeterReading> readings = new ArrayList<>(limit);
        try (Jedis jedis = jedisPool.getResource()) {
            List<StreamEntry> entries = jedis.xrevrange(key, null,
                    null, limit);
            for (StreamEntry entry : entries) {
                readings.add(new MeterReading(entry.getFields()));
            }
            return readings;
        }
    }
}
