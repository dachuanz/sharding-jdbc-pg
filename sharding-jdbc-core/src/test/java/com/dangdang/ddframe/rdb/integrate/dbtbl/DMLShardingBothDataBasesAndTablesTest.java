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

package com.dangdang.ddframe.rdb.integrate.dbtbl;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.dbunit.DatabaseUnitException;
import org.junit.Before;
import org.junit.Test;

import com.dangdang.ddframe.rdb.sharding.api.ShardingDataSource;

public final class DMLShardingBothDataBasesAndTablesTest extends AbstractShardingBothDataBasesAndTablesDBUnitTest {
    
    private ShardingDataSource shardingDataSource;
    
    @Before
    public void init() throws SQLException {
        shardingDataSource = getShardingDataSource();
    }
    
    @Test
    public void assertInsertWithAllPlacehloders() throws SQLException, DatabaseUnitException {
        String sql = "INSERT INTO `t_order` (`order_id`, `user_id`, `status`) VALUES (?, ?, ?)";
        for (int i = 1; i <= 10; i++) {
            for (int j = 1; j <= 10; j++) {
                try (Connection connection = shardingDataSource.getConnection()) {
                    PreparedStatement pstmt = connection.prepareStatement(sql);
                    pstmt.setInt(1, i);
                    pstmt.setInt(2, j);
                    pstmt.setString(3, "insert");
                    pstmt.executeUpdate();
                }
            }
        }
        assertDataset("insert", "insert");
    }
    
    @Test
    public void assertInsertWithoutPlacehloder() throws SQLException, DatabaseUnitException {
        String sql = "INSERT INTO `t_order` (`order_id`, `user_id`, `status`) VALUES (%s, %s, 'insert')";
        for (int i = 1; i <= 10; i++) {
            for (int j = 1; j <= 10; j++) {
                try (Connection connection = shardingDataSource.getConnection()) {
                    PreparedStatement pstmt = connection.prepareStatement(String.format(sql, i, j));
                    pstmt.executeUpdate();
                }
            }
        }
        assertDataset("insert", "insert");
    }
    
    @Test
    public void assertInsertWithPlacehlodersForShardingKeys() throws SQLException, DatabaseUnitException {
        String sql = "INSERT INTO `t_order` (`order_id`, `user_id`, `status`) VALUES (%s, %s, ?)";
        for (int i = 1; i <= 10; i++) {
            for (int j = 1; j <= 10; j++) {
                try (Connection connection = shardingDataSource.getConnection()) {
                    PreparedStatement pstmt = connection.prepareStatement(String.format(sql, i, j));
                    pstmt.setString(1, "insert");
                    pstmt.executeUpdate();
                }
            }
        }
        assertDataset("insert", "insert");
    }
    
    @Test
    public void assertInsertWithPlacehlodersForNotShardingKeys() throws SQLException, DatabaseUnitException {
        String sql = "INSERT INTO `t_order` (`order_id`, `user_id`, `status`) VALUES (%s, %s, ?)";
        for (int i = 1; i <= 10; i++) {
            for (int j = 1; j <= 10; j++) {
                try (Connection connection = shardingDataSource.getConnection()) {
                    PreparedStatement pstmt = connection.prepareStatement(String.format(sql, i, j));
                    pstmt.setString(1, "insert");
                    pstmt.executeUpdate();
                }
            }
        }
        assertDataset("insert", "insert");
    }
    
    @Test
    public void assertUpdateWithoutAlias() throws SQLException, DatabaseUnitException {
        String sql = "UPDATE `t_order` SET `status` = ? WHERE `order_id` = ? AND `user_id` = ?";
        for (int i = 10; i < 20; i++) {
            for (int j = 0; j < 10; j++) {
                try (Connection connection = shardingDataSource.getConnection()) {
                    PreparedStatement pstmt = connection.prepareStatement(sql);
                    pstmt.setString(1, "updated");
                    pstmt.setInt(2, i * 100 + j);
                    pstmt.setInt(3, i);
                    assertThat(pstmt.executeUpdate(), is(1));
                }
            }
        }
        assertDataset("update", "updated");
    }
    
    @Test
    public void assertUpdateWithAlias() throws SQLException, DatabaseUnitException {
        String sql = "UPDATE `t_order` AS o SET o.`status` = ? WHERE o.`order_id` = ? AND o.`user_id` = ?";
        for (int i = 10; i < 20; i++) {
            for (int j = 0; j < 10; j++) {
                try (Connection connection = shardingDataSource.getConnection()) {
                    PreparedStatement pstmt = connection.prepareStatement(sql);
                    pstmt.setString(1, "updated");
                    pstmt.setInt(2, i * 100 + j);
                    pstmt.setInt(3, i);
                    assertThat(pstmt.executeUpdate(), is(1));
                }
            }
        }
        assertDataset("update", "updated");
    }
    
    @Test
    public void assertUpdateWithoutShardingValue() throws SQLException, DatabaseUnitException {
        String sql = "UPDATE `t_order` SET `status` = ? WHERE `status` = ?";
        try (Connection connection = shardingDataSource.getConnection()) {
            PreparedStatement pstmt = connection.prepareStatement(sql);
            pstmt.setString(1, "updated");
            pstmt.setString(2, "init");
            assertThat(pstmt.executeUpdate(), is(100));
        }
        assertDataset("update", "updated");
    }
    
    @Test
    public void assertDeleteWithoutAlias() throws SQLException, DatabaseUnitException {
        String sql = "DELETE `t_order` WHERE `order_id` = ? AND `user_id` = ?";
        for (int i = 10; i < 20; i++) {
            for (int j = 0; j < 10; j++) {
                try (Connection connection = shardingDataSource.getConnection()) {
                    PreparedStatement pstmt = connection.prepareStatement(sql);
                    pstmt.setInt(1, i * 100 + j);
                    pstmt.setInt(2, i);
                    assertThat(pstmt.executeUpdate(), is(1));
                }
            }
        }
        assertDataset("delete", "init");
    }
    
    @Test
    public void assertDeleteWithoutShardingValue() throws SQLException, DatabaseUnitException {
        String sql = "DELETE `t_order` WHERE `status` = ?";
        try (Connection connection = shardingDataSource.getConnection()) {
            PreparedStatement pstmt = connection.prepareStatement(sql);
            pstmt.setString(1, "init");
            assertThat(pstmt.executeUpdate(), is(100));
        }
        assertDataset("delete", "init");
    }
    
    private void assertDataset(final String expectedDataSetPattern, final String status) throws SQLException, DatabaseUnitException {
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 10; j++) {
                assertDataset(String.format("integrate/dataset/dbtbl/expect/%s/dbtbl_%s.xml", expectedDataSetPattern, i),
                        shardingDataSource.getConnection().getConnection(String.format("dataSource_dbtbl_%s", i)), 
                        String.format("t_order_%s", j), String.format("SELECT * FROM `t_order_%s` WHERE `status`=?", j), status);
            }
        }
    }
}
