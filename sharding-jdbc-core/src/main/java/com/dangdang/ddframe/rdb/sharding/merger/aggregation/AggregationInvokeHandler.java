
package com.dangdang.ddframe.rdb.sharding.merger.aggregation;

import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.dangdang.ddframe.rdb.sharding.merger.common.AbstractMergerInvokeHandler;
import com.dangdang.ddframe.rdb.sharding.merger.common.ResultSetQueryIndex;
import com.dangdang.ddframe.rdb.sharding.parser.result.merger.AggregationColumn;
import com.google.common.base.Optional;

/**
 * 聚合函数动态代理.
 * 
 * @author gaohongtao, zhangliang
 */
public final class AggregationInvokeHandler extends AbstractMergerInvokeHandler<AggregationResultSet> {
    
    public AggregationInvokeHandler(final AggregationResultSet aggregationResultSet) {
        super(aggregationResultSet);
    }
    
    @SuppressWarnings("unchecked")
    protected Object doMerge(final AggregationResultSet aggregationResultSet, final Method method, final ResultSetQueryIndex resultSetQueryIndex) throws ReflectiveOperationException, SQLException {
        Optional<AggregationColumn> aggregationColumn = findAggregationColumn(aggregationResultSet, resultSetQueryIndex);
        if (!aggregationColumn.isPresent()) {
            return invokeOriginal(method, resultSetQueryIndex);
        }
        return aggregate(aggregationResultSet, (Class<Comparable<?>>) method.getReturnType(), resultSetQueryIndex, aggregationColumn.get());
    }
    
    private Optional<AggregationColumn> findAggregationColumn(final AggregationResultSet aggregationResultSet, final ResultSetQueryIndex resultSetQueryIndex) {
        for (AggregationColumn each : aggregationResultSet.getAggregationColumns()) {
            if (resultSetQueryIndex.isQueryBySequence() && each.getIndex() == resultSetQueryIndex.getQueryIndex()) {
                return Optional.of(each);
            } else if (each.getAlias().isPresent() && each.getAlias().get().equals(resultSetQueryIndex.getQueryName())) {
                return Optional.of(each);
            } else if (each.getExpression().equalsIgnoreCase(resultSetQueryIndex.getQueryName())) {
                return Optional.of(each);
            }
        }
        return Optional.absent();
    }
    
    private Object aggregate(final AggregationResultSet aggregationResultSet, final Class<Comparable<?>> returnType, 
            final ResultSetQueryIndex resultSetQueryIndex, final AggregationColumn aggregationColumn) 
            throws SQLException {
        AggregationUnit unit = AggregationUnitFactory.create(aggregationColumn.getAggregationType(), returnType);
        for (ResultSet each : aggregationResultSet.getEffectivedResultSets()) {
            unit.merge(aggregationColumn, new ResultSetAggregationValue(each), resultSetQueryIndex);
        }
        return unit.getResult();
    }
}
