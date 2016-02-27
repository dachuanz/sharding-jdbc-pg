package zdctest.zdc1;

import org.postgresql.*;
import java.sql.*;
public class PostgresqlTest {

	/**
	 * @param args
         * 测试
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try{
			Class.forName("org.postgresql.Driver").newInstance();
			String connectUrl ="jdbc:postgresql://hostname:5432/postgres";
			Connection conn = DriverManager.getConnection(connectUrl, "postgres", "password");
			conn.getMetaData().getDatabaseProductName();
			Statement st = conn.createStatement();
			String sql = " SELECT 1;";
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				System.out.println(rs.getInt(1));
			}
			rs.close();
			st.close();
			conn.close();
		}catch(Exception e){
			e.printStackTrace();
		}

	}

}