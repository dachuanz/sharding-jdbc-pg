package com.dangdang.ddframe.rdb.sharding.jdbc;

import com.dangdang.ddframe.rdb.sharding.api.rule.ShardingRule;
import com.dangdang.ddframe.rdb.sharding.executor.ExecutorEngine;
import com.dangdang.ddframe.rdb.sharding.router.SQLRouteEngine;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * ShardingDataSource运行期上下文.
 * 
 * @author gaohongtao
 */
@RequiredArgsConstructor
@Getter
public final class ShardingContext {
    
    private final ShardingRule shardingRule;
    
    private final SQLRouteEngine sqlRouteEngine;
    
    private final ExecutorEngine executorEngine;
}
