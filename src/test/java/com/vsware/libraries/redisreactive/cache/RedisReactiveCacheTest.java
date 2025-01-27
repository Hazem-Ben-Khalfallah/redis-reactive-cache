package com.vsware.libraries.redisreactive.cache;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;
import com.vsware.libraries.redisreactive.cache.config.RedisTestContainerConfig;
import com.vsware.libraries.redisreactive.cache.model.TestTable;
import com.vsware.libraries.redisreactive.cache.service.TestService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RBucketReactive;
import org.redisson.api.RedissonReactiveClient;
import org.redisson.codec.TypedJsonJacksonCodec;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.test.StepVerifier;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@SpringBootTest
class RedisReactiveCacheTest {

    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");
    @Autowired
    private TestService testService;
    @Autowired
    private RedissonReactiveClient redissonReactiveClient;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private Faker faker;

    @AfterAll
    static void stopTestContainer() {
        RedisTestContainerConfig.redisContainer.stop();
    }

    private String calculateCacheKey(String key, Object anyArg) {
        return key + "_" + Arrays.hashCode(new Object[]{anyArg});
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
        testService.methodCall.set(0);
    }

    @Test
    void test_storeInDb() throws InterruptedException {
        String name = faker.name().firstName();
        TestTable testTable = testService.storeInDb(name).block();

        //Verify now Redis contains cache
        checkThatRedisContainsACachedObject(name, testTable);
        checkOriginalMethodIsExecutedNTimes(1);
    }

    @Test
    void test_storeMultipleInDb() throws InterruptedException {

        List<String> names = IntStream.range(0, 10).mapToObj(index -> faker.name().firstName()).collect(Collectors.toList());

        List<TestTable> testTables = testService.storeMultipleInDb(names).collectList().block();
        String cacheKey = calculateCacheKey("names", names);

        checkThatRedisContainsACachedList(cacheKey, testTables);
        checkOriginalMethodIsExecutedNTimes(1);
    }

    @Test
    void test_getFromDb_whenCacheDoesntExists() throws InterruptedException {
        String name = faker.name().firstName();
        TestTable testTable = testService.getFromDb(name).block();

        //Verify now Redis contains cache
        checkThatRedisContainsACachedObject(name, testTable);
        checkOriginalMethodIsExecutedNTimes(1);
    }

    @Test
    void test_getFromDb_whenCacheExists() throws InterruptedException {
        String name = faker.name().firstName();
        TestTable valForCache = new TestTable(1, name, LocalDateTime.now());
        //Create cache to exist
        RBucketReactive<TestTable> bucket = redissonReactiveClient.getBucket(name);
        bucket.set(valForCache).block();
        //call method
        TestTable testTable = testService.getFromDb(name).block();

        //Verify now Redis contains cache
        checkThatRedisContainsACachedObject(name, testTable);
        //make sure actual method with potential DB call was not executed
        checkOriginalMethodIsExecutedNTimes(0);
    }

    @Test
    void test_getFromDb_whenConditionTrue() throws InterruptedException {
        String name = faker.name().firstName();
        TestTable valForCache = new TestTable(1, name, LocalDateTime.now());
        //Create cache to exist
        RBucketReactive<TestTable> bucket = redissonReactiveClient.getBucket(name);
        bucket.set(valForCache).block();
        //call method
        TestTable testTable = testService.getFromDb(name).block();

        //Verify now Redis contains cache
        checkThatRedisContainsACachedObject(name, testTable);
        //make sure actual method with potential DB call was not executed
        checkOriginalMethodIsExecutedNTimes(0);
    }

    @Test
    void test_getMultipleFromDb_whenCacheExists() throws InterruptedException {

        List<String> names = IntStream.range(0, 10).mapToObj(index -> faker.name().firstName()).collect(Collectors.toList());
        String cacheKey = calculateCacheKey("names", names);
        List<TestTable> valForCache = IntStream.range(0, names.size())
                .mapToObj(index -> new TestTable(index, names.get(index), LocalDateTime.now()))
                .collect(Collectors.toList());
        //Create cache to exist
        RBucketReactive<Object> bucket = redissonReactiveClient.getBucket(cacheKey);
        bucket.set(valForCache).block();

        List<TestTable> testTables = testService.getMultipleFromDb(names).collectList().block();

        checkThatRedisContainsACachedList(cacheKey, testTables);
        checkOriginalMethodIsExecutedNTimes(0);
    }

    @Test
    void test_getMultipleFromDb_whenCacheDoesntExists() throws InterruptedException {

        List<String> names = IntStream.range(0, 10).mapToObj(index -> faker.name().firstName()).collect(Collectors.toList());

        List<TestTable> testTables = testService.getMultipleFromDb(names).collectList().block();
        String cacheKey = calculateCacheKey("names", names);

        checkThatRedisContainsACachedList(cacheKey, testTables);
        checkOriginalMethodIsExecutedNTimes(1);
    }

    @Test
    void test_updateDbRecord_whenCacheExists() throws InterruptedException {
        TestTable oldCache = new TestTable(1, faker.name().firstName(), LocalDateTime.now());
        //Create cache to exist
        RBucketReactive<Object> bucket = redissonReactiveClient.getBucket("1");
        bucket.set(oldCache).block();

        //call method
        TestTable testTable = testService.updateDbRecord(new TestTable(1, faker.name().firstName(), LocalDateTime.now())).block();

        //Verify now Redis contains updated cache
        checkThatRedisContainsACachedObject("1", testTable);
        //make sure actual method with potential DB call was not executed
        checkOriginalMethodIsExecutedNTimes(1);
    }

    @Test
    void test_updateDbRecord_whenCacheDoesntExists() throws InterruptedException {
        //call method
        TestTable testTable = testService.updateDbRecord(new TestTable(1, faker.name().firstName(), LocalDateTime.now())).block();

        //Verify now Redis contains updated cache
        checkThatRedisContainsACachedObject("1", testTable);
        //make sure actual method with potential DB call was not executed
        checkOriginalMethodIsExecutedNTimes(1);
    }

    @Test
    void test_updateMultipleDbRecords_whenCacheExists() throws InterruptedException {

        List<String> names = IntStream.range(0, 10).mapToObj(index -> faker.name().firstName()).collect(Collectors.toList());
        String cacheKey = "multiple";
        List<TestTable> valForCache = IntStream.range(0, names.size())
                .mapToObj(index -> new TestTable(index, names.get(index), LocalDateTime.now()))
                .collect(Collectors.toList());
        //Create cache to exist
        RBucketReactive<Object> bucket = redissonReactiveClient.getBucket(cacheKey);
        bucket.set(valForCache).block();

        //modify values
        List<TestTable> recsToUpdate = valForCache.stream().peek(item -> item.setName(faker.name().firstName())).collect(Collectors.toList());
        List<TestTable> testTables = testService.updateMultipleDbRecords(recsToUpdate).collectList().block();

        checkThatRedisContainsACachedList(cacheKey, testTables);
        checkOriginalMethodIsExecutedNTimes(1);
    }

    @Test
    void test_updateMultipleDbRecords_whenCacheDoesntExists() throws InterruptedException {
        String cacheKey = "multiple";

        List<TestTable> testTables = testService.updateMultipleDbRecords(
                        IntStream.range(0, 10)
                                .mapToObj(index -> new TestTable(index, faker.name().firstName(), LocalDateTime.now()))
                                .collect(Collectors.toList()))
                .collectList().block();

        checkThatRedisContainsACachedList(cacheKey, testTables);
        checkOriginalMethodIsExecutedNTimes(1);
    }

    @Test
    void test_deleteDbRec_whenCacheExists() throws InterruptedException {

        String name = faker.name().firstName();
        TestTable valForCache = new TestTable(1, name, LocalDateTime.now());
        //Create cache to exist
        RBucketReactive<Object> bucket = redissonReactiveClient.getBucket(name);
        bucket.set(valForCache).block();

        //Deleting
        testService.deleteDbRec(valForCache).block();

        checkThatKeyHasBeenRemovedFromRedis(name);
        checkOriginalMethodIsExecutedNTimes(1);
    }

    @Test
    void test_deleteDbRec_whenCacheDoesntExists() throws InterruptedException {
        String name = faker.name().firstName();
        //Deleting
        testService.deleteDbRec(new TestTable(1, name, LocalDateTime.now())).block();

        checkThatKeyHasBeenRemovedFromRedis(name);
        checkOriginalMethodIsExecutedNTimes(1);
    }

    @Test
    void test_deleteMultipleDbRecs_whenCacheExists() throws InterruptedException {

        List<TestTable> testTables = IntStream.range(0, 10)
                .mapToObj(index -> new TestTable(index, faker.name().firstName(), LocalDateTime.now()))
                .collect(Collectors.toList());
        String cacheKey = calculateCacheKey("names", testTables);

        //Create cache to exist
        RBucketReactive<Object> bucket = redissonReactiveClient.getBucket(cacheKey);
        bucket.set(testTables).block();

        //Deleting
        testService.deleteMultipleDbRecs(testTables).block();

        checkThatKeyHasBeenRemovedFromRedis(cacheKey);
        checkOriginalMethodIsExecutedNTimes(1);
    }

    @Test
    void test_deleteMultipleDbRecs_whenCacheDoesntExists() throws InterruptedException {

        List<TestTable> testTables = IntStream.range(0, 10)
                .mapToObj(index -> new TestTable(index, faker.name().firstName(), LocalDateTime.now()))
                .collect(Collectors.toList());
        String cacheKey = calculateCacheKey("names", testTables);

        //Deleting
        testService.deleteMultipleDbRecs(testTables).block();

        checkThatKeyHasBeenRemovedFromRedis(cacheKey);
        checkOriginalMethodIsExecutedNTimes(1);
    }

    @Test
    void test_deleteMultipleDbRecsByPattern_whenCacheExists() throws InterruptedException {

        List<TestTable> testTables = IntStream.range(0, 10)
                .mapToObj(index -> new TestTable(index, faker.name().firstName(), LocalDateTime.now()))
                .collect(Collectors.toList());
        String cacheKey = calculateCacheKey("names", testTables);

        //Create cache to exist
        RBucketReactive<Object> bucket = redissonReactiveClient.getBucket(cacheKey);
        bucket.set(testTables).block();

        //Deleting
        testService.deleteMultipleDbRecsByPattern(testTables).block();

        checkThatKeyHasBeenRemovedFromRedis(cacheKey);
        checkOriginalMethodIsExecutedNTimes(1);
    }

    @Test
    void test_deleteMultipleDbRecsByPattern_whenCacheDoesntExists() throws InterruptedException {

        List<TestTable> testTables = IntStream.range(0, 10)
                .mapToObj(index -> new TestTable(index, faker.name().firstName(), LocalDateTime.now()))
                .collect(Collectors.toList());
        String cacheKey = calculateCacheKey("names", testTables);

        //Deleting
        testService.deleteMultipleDbRecsByPattern(testTables).block();

        checkThatKeyHasBeenRemovedFromRedis(cacheKey);
        checkOriginalMethodIsExecutedNTimes(1);
    }

    @Test
    void test_flushAllDbRecs_whenCacheExists() throws InterruptedException {

        List<TestTable> testTables = IntStream.range(0, 10)
                .mapToObj(index -> new TestTable(index, faker.name().firstName(), LocalDateTime.now()))
                .collect(Collectors.toList());
        String cacheKey = calculateCacheKey("names", testTables);

        //Create cache to exist
        RBucketReactive<Object> bucket = redissonReactiveClient.getBucket(cacheKey);
        bucket.set(testTables).block();

        //Deleting
        testService.flushAllDbRecs().block();

        checkThatKeyHasBeenRemovedFromRedis(cacheKey);
        checkOriginalMethodIsExecutedNTimes(1);
    }

    private void checkThatRedisContainsACachedObject(String key, TestTable testTable) {
        RBucketReactive<TestTable> bucket = redissonReactiveClient.getBucket(key, new TypedJsonJacksonCodec(TestTable.class, objectMapper));
        StepVerifier.create(bucket.get().log())
                .expectNextMatches(cacheResponse -> {
                    try {
                        return Objects.equals(cacheResponse.getId(), testTable.getId()) &&
                                cacheResponse.getName().equals(testTable.getName()) &&
                                cacheResponse.getInsertDate().format(formatter).equals(testTable.getInsertDate().format(formatter));
                    } catch (Exception e) {
                        return false;
                    }
                })
                .verifyComplete();
    }

    private void checkThatRedisContainsACachedList(String key, List<TestTable> testTables) {
        RBucketReactive<List<TestTable>> bucket = redissonReactiveClient.getBucket(key, new TypedJsonJacksonCodec(new TypeReference<List<TestTable>>() {
        }, objectMapper));

        StepVerifier.create(bucket.get())
                .expectNextMatches(cacheResponse -> {
                    try {
                        return cacheResponse.size() == testTables.size() &&
                                cacheResponse.get(5).getName().equals(testTables.get(5).getName());
                    } catch (Exception e) {
                        return false;
                    }
                })
                .verifyComplete();
    }

    private void checkThatKeyHasBeenRemovedFromRedis(String name) {
        RBucketReactive<Object> bucket = redissonReactiveClient.getBucket(name);
        StepVerifier.create(bucket.get().log())
                .expectNextCount(0)
                .verifyComplete();
    }

    private void checkOriginalMethodIsExecutedNTimes(int count) {
        assert testService.methodCall.get() == count;
    }

}
