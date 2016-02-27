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

package com.dangdang.ddframe.rdb;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.dangdang.ddframe.rdb.integrate.AllIntegrateTests;
import com.dangdang.ddframe.rdb.sharding.api.AllApiTest;
import com.dangdang.ddframe.rdb.sharding.jdbc.AllJDBCTest;
import com.dangdang.ddframe.rdb.sharding.merger.AllMergerTest;
import com.dangdang.ddframe.rdb.sharding.metrics.AllMetricsTest;
import com.dangdang.ddframe.rdb.sharding.parser.AllParserTest;
import com.dangdang.ddframe.rdb.sharding.router.AllRouterTest;

@RunWith(Suite.class)
@SuiteClasses({
    AllApiTest.class, 
    AllParserTest.class, 
    AllRouterTest.class, 
    AllMergerTest.class, 
    AllJDBCTest.class, 
    AllMetricsTest.class, 
    AllIntegrateTests.class
    })
public class AllTests {
}
