package com.dangdang.ddframe.rdb.sharding.parser.visitor.basic.postgresql;

import java.util.Arrays;

import com.alibaba.druid.sql.ast.SQLHint;
import com.alibaba.druid.sql.ast.expr.SQLBetweenExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.dialect.postgresql.visitor.PGOutputVisitor;
import com.dangdang.ddframe.rdb.sharding.api.DatabaseType;
import com.dangdang.ddframe.rdb.sharding.parser.result.router.SQLBuilder;
import com.dangdang.ddframe.rdb.sharding.parser.result.router.Table;
import com.dangdang.ddframe.rdb.sharding.parser.result.router.Condition.BinaryOperator;
import com.dangdang.ddframe.rdb.sharding.parser.visitor.ParseContext;
import com.dangdang.ddframe.rdb.sharding.parser.visitor.SQLVisitor;
/**
 * 
 * @author 张大川
 *
 */
public class AbstractPostgreSQLVisitor extends PGOutputVisitor implements SQLVisitor {

	public AbstractPostgreSQLVisitor() {
		super(new SQLBuilder());
		setPrettyFormat(false);
	}

	private final ParseContext parseContext = new ParseContext();

	@Override
	public final DatabaseType getDatabaseType() {
		return DatabaseType.PostgreSQL;
	}

	@Override
	public final ParseContext getParseContext() {
		return parseContext;
	}

	@Override
	public final SQLBuilder getSQLBuilder() {
		return (SQLBuilder) appender;
	}

	@Override
	public final void printToken(final String token) {
		getSQLBuilder().appendToken(parseContext.getExactlyValue(token));
	}

	
	public final boolean visit(final SQLVariantRefExpr x) {
		print(x.getName());
		return false;
	}

	@Override
	public final boolean visit(final SQLExprTableSource x) {
		return visit(x, parseContext.addTable(x));
	}

	private boolean visit(final SQLExprTableSource x, final Table table) {
		printToken(table.getName());
		if (table.getAlias().isPresent()) {
			print(' ');
			print(table.getAlias().get());
		}
		for (SQLHint each : x.getHints()) {
			print(' ');
			each.accept(this);
		}
		return false;
	}

	
	@Override

	public final boolean visit(final SQLPropertyExpr x) {
		if (!(x.getParent() instanceof SQLBinaryOpExpr) && !(x.getParent() instanceof SQLSelectItem)) {
			return super.visit(x);
		}
		if (!(x.getOwner() instanceof SQLIdentifierExpr)) {
			return super.visit(x);
		}
		String tableOrAliasName = ((SQLIdentifierExpr) x.getOwner()).getLowerName();
		if (parseContext.isBinaryOperateWithAlias(x, tableOrAliasName)) {
			return super.visit(x);
		}
		printToken(tableOrAliasName);
		print(".");
		print(x.getName());
		return false;
	}

	
	public boolean visit(final SQLBinaryOpExpr x) {
		switch (x.getOperator()) {
		case BooleanOr:
			parseContext.setHasOrCondition(true);
			break;
		case Equality:
			parseContext.addCondition(x.getLeft(), BinaryOperator.EQUAL, Arrays.asList(x.getRight()), getDatabaseType(),
					getParameters());
			parseContext.addCondition(x.getRight(), BinaryOperator.EQUAL, Arrays.asList(x.getLeft()), getDatabaseType(),
					getParameters());
			break;
		default:
			break;
		}
		return super.visit(x);
	}

	
	public boolean visit(final SQLInListExpr x) {
		parseContext.addCondition(x.getExpr(), x.isNot() ? BinaryOperator.NOT_IN : BinaryOperator.IN, x.getTargetList(),
				getDatabaseType(), getParameters());
		return super.visit(x);
	}

	public boolean visit(final SQLBetweenExpr x) {
		parseContext.addCondition(x.getTestExpr(), BinaryOperator.BETWEEN,
				Arrays.asList(x.getBeginExpr(), x.getEndExpr()), getDatabaseType(), getParameters());
		return super.visit(x);
	}

}
