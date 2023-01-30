package com.vsware.libraries.redisreactive.cache.adapters;

import com.vsware.libraries.redisreactive.cache.ports.CachePort;
import lombok.RequiredArgsConstructor;
import org.redisson.api.RBucketReactive;
import org.redisson.api.RedissonReactiveClient;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.concurrent.TimeUnit;

/**
 * @author hazem
 */
@Component
@RequiredArgsConstructor
@Profile("!in-memory-test")
public class RedisCache implements CachePort {
    private final RedissonReactiveClient redissonReactiveClient;

    @Override
    public Mono<Void> set(String key, Object value) {
        RBucketReactive<Object> bucket = redissonReactiveClient.getBucket(key);
        return bucket.set(value);
    }

    @Override
    public Mono<Void> set(String key, Object value, int ttl, TimeUnit timeUnit) {
        RBucketReactive<Object> bucket = redissonReactiveClient.getBucket(key);
        return bucket.set(value, ttl, timeUnit);
    }

    @Override
    public Mono<Void> delete(String key) {
        RBucketReactive<Object> bucket = redissonReactiveClient.getBucket(key);
        return bucket.delete()
                .then();
    }

    public Mono<Void> deleteByPattern(String pattern) {
        return redissonReactiveClient.getKeys()
                .deleteByPattern(pattern)
                .then();
    }

    @Override
    public Mono<Object> get(String key) {
        RBucketReactive<Object> bucket = redissonReactiveClient.getBucket(key);
        return bucket.get();
    }

    @Override
    public Mono<Void> flushAll() {
        return redissonReactiveClient.getKeys().flushall()
                .then();
    }
}
