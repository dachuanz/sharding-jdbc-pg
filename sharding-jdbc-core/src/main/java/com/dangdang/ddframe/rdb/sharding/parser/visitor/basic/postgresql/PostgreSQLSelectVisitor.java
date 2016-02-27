package com.dangdang.ddframe.rdb.sharding.parser.visitor.basic.postgresql;

import java.util.List;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.postgresql.ast.stmt.PGSelectQueryBlock;
import com.alibaba.druid.sql.dialect.postgresql.visitor.PGOutputVisitor;
import com.dangdang.ddframe.rdb.sharding.parser.result.merger.AggregationColumn;
import com.dangdang.ddframe.rdb.sharding.parser.result.merger.AggregationColumn.AggregationType;
import com.dangdang.ddframe.rdb.sharding.parser.result.merger.GroupByColumn;
import com.dangdang.ddframe.rdb.sharding.parser.result.merger.OrderByColumn.OrderByType;
import com.google.common.base.Optional;

/**
 * postgresql的SELECT语句访问器.
 * 
 * @author
 *
 */
public class PostgreSQLSelectVisitor extends AbstractPostgreSQLVisitor {

	private static final String AUTO_GEN_TOKE_KEY = "sharding_auto_gen";

	private int itemIndex;

	@Override
	protected void printSelectList(final List<SQLSelectItem> selectList) {
		super.printSelectList(selectList);

		getSQLBuilder().appendToken(AUTO_GEN_TOKE_KEY, false);
	}

	@Override
	public boolean visit(final PGSelectQueryBlock x) {
		if (x.getFrom() instanceof SQLExprTableSource) {
			SQLExprTableSource tableExpr = (SQLExprTableSource) x.getFrom();
			getParseContext().setCurrentTable(tableExpr.getExpr().toString(),
					Optional.fromNullable(tableExpr.getAlias()));
		}
		return super.visit(x);
	}

	public boolean visit(final SQLSelectItem x) {
		itemIndex++;
		return super.visit(x);
	}

	@Override
	public boolean visit(final SQLAggregateExpr x) {
		if (!(x.getParent() instanceof SQLSelectItem)) {
			return super.visit(x);
		}
		AggregationType aggregationType;
		try {
			aggregationType = AggregationType.valueOf(x.getMethodName().toUpperCase());
		} catch (final IllegalArgumentException ex) {
			return super.visit(x);
		}
		StringBuilder expression = new StringBuilder();
		x.accept(new PGOutputVisitor(expression));

		AggregationColumn column = new AggregationColumn(expression.toString(), aggregationType,
				Optional.fromNullable(((SQLSelectItem) x.getParent()).getAlias()),
				null == x.getOption() ? Optional.<String> absent() : Optional.of(x.getOption().toString()), itemIndex);
		getParseContext().getParsedResult().getMergeContext().getAggregationColumns().add(column);
		if (AggregationType.AVG.equals(aggregationType)) {
			getParseContext().addDerivedColumnsForAvgColumn(column);

		}
		return super.visit(x);
	}

	public boolean visit(final SQLOrderBy x) {
		for (SQLSelectOrderByItem each : x.getItems()) {
			SQLExpr expr = each.getExpr();
			OrderByType orderByType = null == each.getType() ? OrderByType.ASC : OrderByType.valueOf(each.getType());
			if (expr instanceof SQLIntegerExpr) {
				getParseContext().addOrderByColumn(((SQLIntegerExpr) expr).getNumber().intValue(), orderByType);
			} else if (expr instanceof SQLIdentifierExpr) {
				getParseContext().addOrderByColumn(((SQLIdentifierExpr) expr).getName(), orderByType);
			} else if (expr instanceof SQLPropertyExpr) {
				getParseContext().addOrderByColumn(((SQLPropertyExpr) expr).getName(), orderByType);
			}
		}
		return super.visit(x);
	}

	@Override
	public void endVisit(final SQLSelectStatement x) {
		StringBuilder derivedSelectItems = new StringBuilder();
		for (AggregationColumn aggregationColumn : getParseContext().getParsedResult().getMergeContext()
				.getAggregationColumns()) {
			for (AggregationColumn derivedColumn : aggregationColumn.getDerivedColumns()) {
				derivedSelectItems.append(", ").append(derivedColumn.getExpression()).append(" AS ")
						.append(derivedColumn.getAlias().get());
			}
		}
		for (GroupByColumn each : getParseContext().getParsedResult().getMergeContext().getGroupByColumns()) {
			derivedSelectItems.append(", ").append(each.getName()).append(" AS ").append(each.getAlias());
		}
		if (0 != derivedSelectItems.length()) {
			getSQLBuilder().buildSQL(AUTO_GEN_TOKE_KEY, derivedSelectItems.toString());
		}
		super.endVisit(x);
	}

}
