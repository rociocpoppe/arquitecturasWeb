package factory;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import clasesDAO.ClienteDao;
import clasesDAO.FacturaDao;
import clasesDAO.Factura_ProductoDao;
import clasesDAO.ProductoDao;

public class DerbyDB extends DAOFactory {

    public static final String DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
	public static final String URI = "jdbc:derby:MyDerbyDb;create=true";

	public DerbyDB() {
		this.registrarDriver();
	}

	private static void registrarDriver() {
		try {
			Class.forName(DRIVER).getDeclaredConstructor().newInstance();
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
				| NoSuchMethodException | SecurityException | ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}

	}

	public static  Connection crearConeccion() throws SQLException {
		try {
			Connection conn = DriverManager.getConnection(URI);
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

	@Override
	public FacturaDao getFacturaDAO(String db) throws SQLException {
		return new FacturaDao(db);
	}

	@Override
	public ProductoDao getProductoDAO(String db) throws SQLException {
		return new ProductoDao(db);
	}

	@Override
	public Factura_ProductoDao getFactura_ProductoDAO(String db) throws SQLException {
		return new Factura_ProductoDao(db);
	}

}
