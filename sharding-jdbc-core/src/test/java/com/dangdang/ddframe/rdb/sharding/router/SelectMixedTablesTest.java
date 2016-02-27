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

package com.dangdang.ddframe.rdb.sharding.router;

import java.util.Arrays;

import org.junit.Test;

import com.dangdang.ddframe.rdb.sharding.exception.SQLParserException;
import com.dangdang.ddframe.rdb.sharding.exception.ShardingJdbcException;

public final class SelectMixedTablesTest extends AbstractBaseRouteSqlTest {
    
    @Test
    public void assertBindingTableWithUnBoundTable() throws SQLParserException {
        assertSingleTarget("select * from order o join order_item i join order_attr a using(order_id) where o.order_id = 1", "ds_1", 
                "SELECT * FROM order_1 o JOIN order_item_1 i JOIN order_attr_b a USING (order_id) WHERE o.order_id = 1");
    }
    
    @Test
    public void assertConditionFromRelationship() throws SQLParserException {
        assertSingleTarget("select * from order o join order_attr a using(order_id) where o.order_id = 1", "ds_1", 
                "SELECT * FROM order_1 o JOIN order_attr_b a USING (order_id) WHERE o.order_id = 1");
    }
    
    @Test
    public void assertSelectWithCartesianProductAllPartitions() throws SQLParserException {
        assertMultipleTargets("select * from order o, order_attr a", 4, Arrays.asList("ds_0", "ds_1"), 
                Arrays.asList("SELECT * FROM order_0 o, order_attr_a a", "SELECT * FROM order_1 o, order_attr_a a", 
                        "SELECT * FROM order_0 o, order_attr_b a", "SELECT * FROM order_1 o, order_attr_b a"));
    }
    
    @Test
    public void assertSelectWithoutTableRule() throws SQLParserException {
        assertSingleTarget("select * from order o join product p using(prod_id) where o.order_id = 1", "ds_1", 
                "SELECT * FROM order_1 o JOIN product p USING (prod_id) WHERE o.order_id = 1");
    }
    
    @Test(expected = ShardingJdbcException.class)
    public void assertSelectTableWithoutRules() throws SQLParserException {
        assertSingleTarget("select * from aaa, bbb, ccc", null, null);
    }
}
