package com.redislabs.university.RU102J.dao;

import com.redislabs.university.RU102J.core.KeyHelper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

public class RateLimiterSlidingDaoRedisImpl implements RateLimiter {

    private final JedisPool jedisPool;
    private final long windowSizeMS;
    private final long maxHits;

    public RateLimiterSlidingDaoRedisImpl(JedisPool pool, long windowSizeMS,
                                          long maxHits) {
        this.jedisPool = pool;
        this.windowSizeMS = windowSizeMS;
        this.maxHits = maxHits;
    }

    // Challenge #7
    @Override
    public void hit(String name) throws RateLimitExceededException {
        // START CHALLENGE #7
        final String rateLimiterKey = KeyHelper.getKey("sliding-window:" + windowSizeMS + ":" + name + ":" + maxHits);
        try (final Jedis jedis = jedisPool.getResource()) {
            final Transaction tx = jedis.multi();
            final long currentTimeMillis = System.currentTimeMillis();
            final String member = "" +  currentTimeMillis + "-" + Math.random();


            final Response<Long> zaddResult = tx.zadd(rateLimiterKey, (double) currentTimeMillis, member);
            final Response<Long> remRangeResult = tx.zremrangeByScore(rateLimiterKey, 0.0,currentTimeMillis - windowSizeMS);
            final Response<Long> hits = tx.zcard(rateLimiterKey);
            tx.exec();

            if (hits.get() > maxHits) {
                throw new RateLimitExceededException();
            }
        }
        // END CHALLENGE #7
    }
}
