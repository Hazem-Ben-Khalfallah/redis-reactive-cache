package com.vsware.libraries.redisreactive.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;
import com.vsware.libraries.redisreactive.cache.config.RedisTestContainerConfig;
import com.vsware.libraries.redisreactive.lock.erros.ResourceAlreadyLockedError;
import com.vsware.libraries.redisreactive.lock.ports.LockerPort;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonReactiveClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class LockerTest {

    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");
    @Autowired
    private RedissonReactiveClient redissonReactiveClient;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private Faker faker;
    @Autowired
    private LockerPort locker;

    @AfterAll
    static void stopTestContainer() {
        RedisTestContainerConfig.redisContainer.stop();
    }

    @BeforeEach
    void verifyRedisIsEmpty() {
        StepVerifier.create(redissonReactiveClient.getKeys().count())
                .expectNext(0L)
                .verifyComplete();
    }

    @AfterEach
    void cleanRedis() {
        redissonReactiveClient.getKeys().flushall().subscribe();
    }

    @Test
    void test_lock() {
        // given
        final String resourceKey = String.format("test-key-%s", Faker.instance().funnyName().name());
        final String fencingKey = UUID.randomUUID().toString();

        // when
        lock(resourceKey, fencingKey).block();

        // then
        Assertions.assertThat(locker.isLocked(resourceKey, fencingKey).block()).isTrue();
    }

    @Test
    void test_lock_already_locked() {
        // given
        final String resourceKey = String.format("test-key-%s", Faker.instance().funnyName().name());
        final String fencingKey = UUID.randomUUID().toString();
        lock(resourceKey, fencingKey).block();

        final String otherFencingKey = UUID.randomUUID().toString();
        // when
        StepVerifier.create(lock(resourceKey, otherFencingKey))
                .expectError(ResourceAlreadyLockedError.class)
                .verify();

        Assertions.assertThat(locker.isLocked(resourceKey, fencingKey).block()).isTrue();
        Assertions.assertThat(locker.isLocked(resourceKey, otherFencingKey).block()).isFalse();
    }

    @Test
    void test_unlock() {
        // given
        final String resourceKey = String.format("test-key-%s", Faker.instance().funnyName().name());
        final String fencingKey = UUID.randomUUID().toString();

        lock(resourceKey, fencingKey).block();

        // when
        locker.unlock(resourceKey, fencingKey).block();

        // then
        Assertions.assertThat(locker.isLocked(resourceKey, fencingKey).block()).isFalse();
    }

    private Mono<Void> lock(String resourceKey, String fencingKey) {
        return locker.lock(resourceKey,
                fencingKey,
                10,
                TimeUnit.SECONDS);
    }

}
