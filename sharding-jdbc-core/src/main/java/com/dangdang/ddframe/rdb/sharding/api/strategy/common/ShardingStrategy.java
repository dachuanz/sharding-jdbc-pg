

package com.dangdang.ddframe.rdb.sharding.api.strategy.common;

import java.util.Arrays;
import java.util.Collection;

import com.dangdang.ddframe.rdb.sharding.api.ShardingValue;
import com.dangdang.ddframe.rdb.sharding.exception.ShardingJdbcException;
import com.dangdang.ddframe.rdb.sharding.parser.result.router.SQLStatementType;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * 分片策略.
 * 
 * @author zhangliang
 */
@RequiredArgsConstructor
public class ShardingStrategy {
    
    @Getter
    private final Collection<String> shardingColumns;
    
    private final ShardingAlgorithm shardingAlgorithm;
    
    public ShardingStrategy(final String shardingColumn, final ShardingAlgorithm shardingAlgorithm) {
        this(Arrays.asList(shardingColumn), shardingAlgorithm);
    }
    
    /**
     * 根据分片值计算数据源名称集合.
     *
     *
     * @param sqlStatementType SQL语句的类型
     * @param availableTargetNames 所有的可用数据源名称集合
     * @param shardingValues 分库片值集合
     * @return 分库后指向的数据源名称集合
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Collection<String> doSharding(final SQLStatementType sqlStatementType, final Collection<String> availableTargetNames, 
                                         final Collection<ShardingValue<? extends Comparable<?>>> shardingValues) {
        if (shardingValues.isEmpty()) {
            if (SQLStatementType.INSERT.equals(sqlStatementType) && availableTargetNames.size() > 1) {
                throw new ShardingJdbcException("INSERT statement must contains sharding value");
            } else {
                return availableTargetNames;
            }
        }
        if (shardingAlgorithm instanceof SingleKeyShardingAlgorithm) {
            SingleKeyShardingAlgorithm<?> singleKeyShardingAlgorithm = (SingleKeyShardingAlgorithm<?>) shardingAlgorithm;
            ShardingValue shardingValue = shardingValues.iterator().next();
            switch (shardingValue.getType()) {
                case SINGLE: 
                    return Arrays.asList(singleKeyShardingAlgorithm.doEqualSharding(availableTargetNames, shardingValue));
                case LIST: 
                    return singleKeyShardingAlgorithm.doInSharding(availableTargetNames, shardingValue);
                case RANGE: 
                    return singleKeyShardingAlgorithm.doBetweenSharding(availableTargetNames, shardingValue);
                default: 
                    throw new UnsupportedOperationException(shardingValue.getType().getClass().getName());
            }
        }
        if (shardingAlgorithm instanceof MultipleKeysShardingAlgorithm) {
            return ((MultipleKeysShardingAlgorithm) shardingAlgorithm).doSharding(availableTargetNames, shardingValues);
        }
        throw new UnsupportedOperationException(shardingAlgorithm.getClass().getName());
    }
}
