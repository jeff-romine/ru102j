package com.redislabs.university.RU102J.dao;

import com.redislabs.university.RU102J.core.KeyHelper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.time.ZonedDateTime;

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
        try (final Jedis jedis = jedisPool.getResource()) {
            final String rateLimiterKey = KeyHelper.getKey("limiter:" + windowSizeMS + ":" + name + ":" + maxHits);

            //final long now = System.currentTimeMillis();
            final long now = ZonedDateTime.now().toInstant().toEpochMilli();

            final Transaction tx = jedis.multi();
            final String member = now + "-" + Math.random();

            tx.zadd(rateLimiterKey, (double) now, member);
            tx.zremrangeByScore(rateLimiterKey, 0,now - windowSizeMS);
            final Response<Long> hits = tx.zcard(rateLimiterKey);
            tx.exec();

            if (hits.get() > maxHits) {
                throw new RateLimitExceededException();
            }
        }
        // END CHALLENGE #7
    }
}
