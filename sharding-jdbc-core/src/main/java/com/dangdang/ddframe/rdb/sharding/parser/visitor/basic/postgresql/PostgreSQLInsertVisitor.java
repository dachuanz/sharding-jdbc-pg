package com.dangdang.ddframe.rdb.sharding.parser.visitor.basic.postgresql;

import com.alibaba.druid.sql.dialect.postgresql.ast.stmt.PGInsertStatement;
import com.dangdang.ddframe.rdb.sharding.parser.result.router.Condition.BinaryOperator;
import com.google.common.base.Optional;

/**
 * 
 * @author zhangdachuan 
 *插入数据 处理
 */

public class PostgreSQLInsertVisitor extends AbstractPostgreSQLVisitor {

	@Override
	public boolean visit(final PGInsertStatement x) {
		getParseContext().setCurrentTable(x.getTableName().toString(), Optional.fromNullable(x.getAlias()));
		for (int i = 0; i < x.getColumns().size(); i++) {
			getParseContext().addCondition(x.getColumns().get(i).toString(), x.getTableName().toString(),
					BinaryOperator.EQUAL, x.getValues().getValues().get(i), getDatabaseType(), getParameters());
		}
		return super.visit(x);
	}
}
