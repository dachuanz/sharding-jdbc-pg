package zdctest.zdc1;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang3.RandomStringUtils;

import com.dangdang.ddframe.rdb.sharding.api.ShardingDataSource;
import com.dangdang.ddframe.rdb.sharding.api.rule.DataSourceRule;
import com.dangdang.ddframe.rdb.sharding.api.rule.ShardingRule;
import com.dangdang.ddframe.rdb.sharding.api.rule.TableRule;
import com.dangdang.ddframe.rdb.sharding.api.strategy.database.DatabaseShardingStrategy;

import zdctest.zdc1.algorithm.ModuloDatabaseShardingAlgorithm;

public final class Main {

	public static void main(final String[] args) throws SQLException {
		DataSource dataSource = getShardingDataSource();
		printSimpleSelect(dataSource);
		System.out.println("--------------");
		// printGroupBy(dataSource);
	}

	private static void printSimpleSelect(final DataSource dataSource) throws SQLException {
		String sql = "SELECT user_id,order_id FROM t_order where  USER_ID=822222222222";
		String sql1 = "delete FROM t_order where  USER_ID=23";
		String string = "INSERT INTO t_order( order_id, user_id) VALUES (10, ?)";
		try (Connection conn = dataSource.getConnection();) {
			// pstmt.setInt(1, 23);
			// pstmt.setInt(2, 1001);

			PreparedStatement pstmt = conn.prepareStatement(string);
			String s = "8" + RandomStringUtils.randomNumeric(10);
			pstmt.setLong(1, Long.parseLong(s));
			pstmt.executeUpdate();
			// try (ResultSet rs = pstmt.executeQuery()) {
			// while (rs.next()) {
			// System.out.println(rs.getInt(1));
			// System.out.println(rs.getInt(2));
			// // System.out.println(rs.getInt(2));
			// // System.out.println(rs.getInt(3));
			// }
		}
		// }
	}

	private static ShardingDataSource getShardingDataSource() throws SQLException {
		DataSourceRule dataSourceRule = new DataSourceRule(createDataSourceMap());
/**
 * 使用的表规则
 */
		TableRule orderTableRule = new TableRule("t_order", Arrays.asList("t_order"), dataSourceRule);
/**
 * 根据user_id 进行 分库
 */
		ShardingRule shardingRule = new ShardingRule(dataSourceRule, Arrays.asList(orderTableRule),new DatabaseShardingStrategy("user_id", new ModuloDatabaseShardingAlgorithm()));

		return new ShardingDataSource(shardingRule);
	}

	/**
	 * 
	 * 
	 * 
	 * @throws SQLException
	 * 
	 * 
	 */
	private static Map<String, DataSource> createDataSourceMap() throws SQLException {
		Map<String, DataSource> result = new HashMap<>(2);
		result.put("ds_0", createDataSource("ds_0"));
		result.put("ds_1", createDataSource2("ds_1"));
		return result;
	}

	// 创建数据源
	private static DataSource createDataSource(final String dataSourceName) throws SQLException {
		BasicDataSource result = new BasicDataSource();
		result.setDriverClassName("org.postgresql.Driver");
		result.setUrl("jdbc:postgresql://192.168.79.132:5432/postgres");
		result.setUsername("postgres");
		result.setPassword("password");
		System.out.println(result.getConnection().getMetaData().getDatabaseProductName());
		return result;
	}

	// 创建数据源
	private static DataSource createDataSource2(final String dataSourceName) {
		BasicDataSource result = new BasicDataSource();
		result.setDriverClassName(org.postgresql.Driver.class.getName());
		result.setUrl("jdbc:postgresql://192.168.79.133:5432/postgres");
		result.setUsername("postgres");
		result.setPassword("password");
		return result;
	}
}
