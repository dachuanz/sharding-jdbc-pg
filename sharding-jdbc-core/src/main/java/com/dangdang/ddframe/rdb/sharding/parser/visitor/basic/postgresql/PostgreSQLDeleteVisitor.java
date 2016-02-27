package com.dangdang.ddframe.rdb.sharding.parser.visitor.basic.postgresql;

import com.alibaba.druid.sql.dialect.postgresql.ast.stmt.PGDeleteStatement;
import com.google.common.base.Optional;

/**
 * 
 * @author zhangdachuan(dachuanz@gmail.com)
 *
 */
public class PostgreSQLDeleteVisitor extends AbstractPostgreSQLVisitor {

	@Override
	public boolean visit(final PGDeleteStatement x) {
		getParseContext().setCurrentTable(x.getTableName().toString(), Optional.fromNullable(x.getAlias()));
		return super.visit(x);
	}
}
