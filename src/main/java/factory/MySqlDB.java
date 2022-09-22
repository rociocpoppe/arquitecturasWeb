package factory;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import clasesDAO.ClienteDao;

public class MySqlDB extends DAOFactory{


	public static final String DRIVER = "com.mysql.cj.jdbc.Driver";
	
	public static final String URI = "jdbc:mysql://localhost:3306/mysqlEntregable";
	
	Connection conn = null;
	
	public MySqlDB() {
		this.registrarDriver();
	}
	
	
	private static void registrarDriver() {

		try {
			Class.forName(DRIVER).getDeclaredConstructor().newInstance();
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
				| NoSuchMethodException | SecurityException | ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}

	}	

	
	  
	public  static Connection crearConeccion() throws SQLException{
		try {
			Connection conn = DriverManager.getConnection(URI, "root", "password");
			conn.setAutoCommit(false);
			return conn;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}


    @Override
	public ClienteDao getClienteDAO(String db) throws SQLException {
		return new ClienteDao(db);
	}

}
