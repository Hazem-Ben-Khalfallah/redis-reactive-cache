package com.vsware.libraries.redisreactive.cache.aspect;

import com.vsware.libraries.redisreactive.cache.ports.CachePort;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author hazem
 */
@Slf4j
abstract class AbstractReactiveCacheAspect {

    abstract CachePort getCache();

    protected Mono<?> putMethodMonoResponseToCache(ProceedingJoinPoint joinPoint, String key) {
        return proceedAsMono(joinPoint)
                .map(methodResponse -> updateCache(key, methodResponse));
    }

    protected Flux<?> putMethodFluxResponseToCache(ProceedingJoinPoint joinPoint, String key) {
        return proceedAsFlux(joinPoint)
                .collectList()
                .map(methodResponseList -> updateCache(key, methodResponseList))
                .flatMapMany(Flux::fromIterable);
    }

    private <T> T updateCache(String key, T valueToCache) {
        log.info("Put key [{}] in cache", key);
        getCache().set(key, valueToCache).subscribe();
        return valueToCache;
    }

    protected Mono<?> proceedAsMono(ProceedingJoinPoint joinPoint) {
        try {
            return ((Mono<?>) proceedMethod(joinPoint));
        } catch (Throwable e) {
            return Mono.error(e);
        }
    }

    protected Flux<?> proceedAsFlux(ProceedingJoinPoint joinPoint) {
        try {
            return ((Flux<?>) proceedMethod(joinPoint));
        } catch (Throwable e) {
            return Flux.error(e);
        }
    }

    protected Object proceedMethod(ProceedingJoinPoint joinPoint) throws Throwable {
        return joinPoint.proceed(joinPoint.getArgs());
    }
}
