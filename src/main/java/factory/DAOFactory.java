package factory;

import java.sql.SQLException;

import clasesDAO.ClienteDao;
import clasesDAO.FacturaDao;
import clasesDAO.Factura_ProductoDao;
import clasesDAO.ProductoDao;

public abstract class DAOFactory {

    public static final String MYSQL_DB = "mysql";
	public static final String DERBY_DB = "derby";
	
	public abstract ClienteDao getClienteDAO(String db) throws SQLException;
	public abstract FacturaDao getFacturaDAO(String db) throws SQLException;
	public abstract ProductoDao getProductoDAO(String db) throws SQLException;
	public abstract Factura_ProductoDao getFactura_ProductoDAO(String db) throws SQLException;
	
	//Patron Singleton mysql
	private static DAOFactory db_mysql =null;	
	
	public static DAOFactory getInstanceMysql() {
		if(db_mysql== null) {
			db_mysql = new MySqlDB();
			return db_mysql;
		}
		else return db_mysql;
	}
	
	//Patron Singleton derby
	private static DAOFactory db_derby = null;

	public static DAOFactory getInstanceDerby() {
		if(db_derby== null) {
			db_derby = new DerbyDB();
			return  db_derby;
		}
		else return db_derby;
	}
	
	
	public static DAOFactory getDAOFactory(String db) {
		switch (db) {
		case MYSQL_DB:
			return getInstanceMysql();
		case DERBY_DB:
			return getInstanceDerby();
		default:
			return null;
		}
	}
	
}
    

