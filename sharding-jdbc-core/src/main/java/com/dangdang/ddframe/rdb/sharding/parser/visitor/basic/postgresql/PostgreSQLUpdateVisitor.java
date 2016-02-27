package com.dangdang.ddframe.rdb.sharding.parser.visitor.basic.postgresql;

import com.alibaba.druid.sql.dialect.postgresql.ast.stmt.PGUpdateStatement;
import com.google.common.base.Optional;

/**
 * 
 * 
 * @author zhangdachuan(dachuanz@gmail.com)
 *
 */

public class PostgreSQLUpdateVisitor extends AbstractPostgreSQLVisitor {
	public boolean visit(final PGUpdateStatement x) {
        getParseContext().setCurrentTable(x.getTableName().toString(), Optional.<String>absent());
        return super.visit(x);
    }
}
