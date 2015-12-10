package xml.parsing;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class DBOperations {

	private Connection connect = null;
	private ResultSet rs = null;
	private String dbname = null;
	private String hostname = null;
	private String user = null;
	private String password = null;
	private String url = null ;
	static final Logger log = LogManager.getLogger(DBOperations.class);

	
	public void setHostName (String hostname) {
		this.hostname= hostname;
	}
	
	public void setUser (String username) {
		this.user= username;
	}
	
	public void setPassword (String password) {
		this.password= password;
	}
	
	public void setDBName (String dbname) {
		this.dbname= dbname;
	}
	
	
	public void DBConnection() throws InstantiationException {
		try {
			
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			url = "jdbc:mysql://" + hostname + "/";
			connect = DriverManager.getConnection(url, user, password);
			
		} catch (ClassNotFoundException e) {
			log.error("Problem related with JDBC driver");
			log.debug(e,e);
			System.exit(1);

		} catch (SQLException e) {
			log.error("Can not connect database!");
			log.debug(e,e);
			System.exit(1);

		} catch (IllegalAccessException e) {
			log.error("Problem related with JDBC driver new instance");
			log.debug(e,e);
			System.exit(1);
		}
	}

	public void Executions(String Sql) {

		try {
			PreparedStatement Psmt = connect.prepareStatement(Sql);
			Psmt.executeUpdate();

		} catch (SQLException e) {
			log.error("Can not made database execution! SQL sentence!");
			log.debug(e,e);
			System.exit(1);
		}

	}
	
	public void CreateDatabase () throws Exception  {
		rs = connect.getMetaData().getCatalogs();
		while (rs.next()) {
			String catalogs = rs.getString(1);
			if (dbname.equals(catalogs)) {
				String dropdb = "DROP DATABASE " + dbname;
				Executions(dropdb);
			    }
		}
		String createdb = "CREATE DATABASE " + dbname ;
		Executions(createdb);
		url = "jdbc:mysql://" + hostname + "/" + dbname + "?useUnicode=true&characterEncoding=utf8";
		connect = DriverManager.getConnection(url, user, password);
	}
}
