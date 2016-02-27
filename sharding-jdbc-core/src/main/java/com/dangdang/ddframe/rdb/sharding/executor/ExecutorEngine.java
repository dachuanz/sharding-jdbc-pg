/**
 * Copyright 1999-2015 dangdang.com.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package com.dangdang.ddframe.rdb.sharding.executor;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import com.dangdang.ddframe.rdb.sharding.exception.ShardingJdbcException;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 多线程执行框架.
 * 
 * @author gaohongtao
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public final class ExecutorEngine {
    
    /**
     * 多线程执行任务.
     * 
     * @param inputs 输入参数
     * @param executeUnit 执行单元
     * @param <I> 入参类型
     * @param <O> 出参类型
     * @return 执行结果
     */
    public static <I, O> List<O> execute(final Collection<I> inputs, final ExecuteUnit<I, O> executeUnit) {
        ListenableFuture<List<O>> futures = submitFutures(inputs, executeUnit);
        addCallback(futures);
        return getFutureResults(futures);
    }
    
    /**
     * 多线程执行任务并归并结果.
     * 
     * @param inputs 执行入参
     * @param executeUnit 执行单元
     * @param mergeUnit 合并结果单元
     * @param <I> 入参类型
     * @param <M> 中间结果类型
     * @param <O> 最终结果类型
     * @return 执行结果
     */
    public static <I, M, O> O execute(final Collection<I> inputs, final ExecuteUnit<I, M> executeUnit, final MergeUnit<M, O> mergeUnit) {
        return mergeUnit.merge(execute(inputs, executeUnit));
    }
    
    private static <I, O> ListenableFuture<List<O>> submitFutures(final Collection<I> inputs, final ExecuteUnit<I, O> executeUnit) {
        Set<ListenableFuture<O>> result = new HashSet<>(inputs.size());
        ListeningExecutorService service = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(inputs.size()));
        for (final I each : inputs) {
            result.add(service.submit(new Callable<O>() {
                
                @Override
                public O call() throws Exception {
                    return executeUnit.execute(each);
                }
            }));
        }
        service.shutdown();
        return Futures.allAsList(result);
    }
    
    private static <T> void addCallback(final ListenableFuture<T> allFutures) {
        Futures.addCallback(allFutures, new FutureCallback<T>() {// 使用 google common 的多线程执行框架
            
            @Override
            public void onSuccess(final T result) {
                log.trace("Concurrent execute result success {}", result);
            }
            
            @Override
            public void onFailure(final Throwable thrown) {
                log.error("Concurrent execute result error {}", thrown);
            }
        });
    }
    
    private static <O> O getFutureResults(final ListenableFuture<O> futures) {
        try {
            return futures.get();
        } catch (final InterruptedException | ExecutionException ex) {
            throw new ShardingJdbcException(ex);
        }
    }
}
