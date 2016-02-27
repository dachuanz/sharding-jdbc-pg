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

package com.dangdang.ddframe.rdb.sharding.parser;

import java.util.Collection;
import java.util.List;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.visitor.SQLASTOutputVisitor;
import com.dangdang.ddframe.rdb.sharding.parser.result.SQLParsedResult;
import com.dangdang.ddframe.rdb.sharding.parser.visitor.SQLVisitor;
import com.dangdang.ddframe.rdb.sharding.parser.visitor.or.OrParser;
import com.google.common.base.Preconditions;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 不包含OR语句的SQL构建器解析.
 * 
 * @author gaohongtao, zhangliang
 */
@RequiredArgsConstructor
@Slf4j
public final class SQLParseEngine {
    
    private final SQLStatement sqlStatement;
    
    private final List<Object> parameters;
    
    private final SQLASTOutputVisitor visitor;
    
    private final Collection<String> shardingColumns;
    
    /**
     *  解析SQL.
     * 
     * @return SQL解析结果
     */
    public SQLParsedResult parse() {
        Preconditions.checkArgument(visitor instanceof SQLVisitor);
        SQLVisitor sqlVisitor = (SQLVisitor) visitor;
        visitor.setParameters(parameters);
        sqlVisitor.getParseContext().setShardingColumns(shardingColumns);
        sqlStatement.accept(visitor);
        SQLParsedResult result;
        if (sqlVisitor.getParseContext().isHasOrCondition()) {
            result = new OrParser(sqlStatement, visitor).parse();
        } else {
            sqlVisitor.getParseContext().mergeCurrentConditionContext();
            result = sqlVisitor.getParseContext().getParsedResult();
        }
        log.debug("Parsed SQL result: {}", result);
        log.debug("Parsed SQL: {}", sqlVisitor.getSQLBuilder());
        result.getRouteContext().setSqlBuilder(sqlVisitor.getSQLBuilder());
        return result;
    }
}
