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

package com.dangdang.ddframe.rdb.sharding.api.strategy.common;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.junit.Test;

import com.dangdang.ddframe.rdb.sharding.api.ShardingValue;
import com.dangdang.ddframe.rdb.sharding.api.strategy.fixture.TestMultipleKeysShardingAlgorithm;
import com.dangdang.ddframe.rdb.sharding.api.strategy.fixture.TestSingleKeyShardingAlgorithm;
import com.google.common.collect.Range;

public final class ShardingStrategyTest {
    
    private final Collection<String> targets = Arrays.asList("1", "2", "3");
    
    @Test
    public void assertDoShardingWithoutShardingColumns() {
        ShardingStrategy strategy = new ShardingStrategy(Arrays.asList("column"), null);
        assertThat(strategy.doSharding(targets, Collections.<ShardingValue<?>>emptyList()), is(targets));
    }
    
    @Test
    public void assertDoShardingForEqualSingleKey() {
        ShardingStrategy strategy = new ShardingStrategy("column", new TestSingleKeyShardingAlgorithm());
        assertThat(strategy.doSharding(targets, createShardingValues(new ShardingValue<String>("column", "1"))), is((Collection<String>) Arrays.asList("1")));
    }
    
    @Test
    public void assertDoShardingForInSingleKey() {
        ShardingStrategy strategy = new ShardingStrategy("column", new TestSingleKeyShardingAlgorithm());
        assertThat(strategy.doSharding(targets, createShardingValues(new ShardingValue<String>("column", Arrays.asList("1", "3")))), is((Collection<String>) Arrays.asList("1", "3")));
    }
    
    @Test
    public void assertDoShardingForBetweenSingleKey() {
        ShardingStrategy strategy = new ShardingStrategy("column", new TestSingleKeyShardingAlgorithm());
        assertThat(strategy.doSharding(targets, createShardingValues(new ShardingValue<String>("column", Range.open("1", "3")))), is((Collection<String>) Arrays.asList("1", "2", "3")));
    }
    
    @Test
    public void assertDoShardingForMultipleKeys() {
        ShardingStrategy strategy = new ShardingStrategy("column", new TestMultipleKeysShardingAlgorithm());
        assertThat(strategy.doSharding(targets, createShardingValues(new ShardingValue<String>("column", "1"))), is((Collection<String>) Arrays.asList("1", "2", "3")));
    }
    
    private Collection<ShardingValue<?>> createShardingValues(final ShardingValue<String> shardingValue) {
        Collection<ShardingValue<?>> result = new ArrayList<>(1);
        result.add(shardingValue);
        return result;
    }
}
