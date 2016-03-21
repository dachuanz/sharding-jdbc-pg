package com.dangdang.ddframe.rdb.sharding.router.single;

import java.util.List;

import com.dangdang.ddframe.rdb.sharding.api.ShardingValue;
import com.dangdang.ddframe.rdb.sharding.parser.result.router.Condition;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;

/**
 * 路由工具类.
 * 
 * 
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class SingleRouterUtil {
    
    /**
     * 将条件对象转换为分片值对象.
     * 
     * @param condition 条件对象
     * @return 分片值对象
     */
    public static ShardingValue<?> convertConditionToShardingValue(final Condition condition) {
        List<Comparable<?>> conditionValues = condition.getValues();
        switch (condition.getOperator()) {
            case EQUAL:
            case IN:
                if (1 == conditionValues.size()) {
                    return new ShardingValue<Comparable<?>>(condition.getColumn().getColumnName(), conditionValues.get(0));
                }
                return new ShardingValue<>(condition.getColumn().getColumnName(), conditionValues);
            case BETWEEN:
                return new ShardingValue<>(condition.getColumn().getColumnName(), Range.range(conditionValues.get(0), BoundType.CLOSED, conditionValues.get(1), BoundType.CLOSED));
            default:
                throw new UnsupportedOperationException(condition.getOperator().getExpression());
        }
    }
    
}

    Status API Training Shop Blog About 

    © 2016 GitHub, Inc. Terms Privacy Security Contact Help 

