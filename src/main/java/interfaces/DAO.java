package interfaces;

import java.sql.SQLException;

public interface DAO <T> {
    
	public void crearTabla(String db) throws SQLException;

	public void insertarDatos( T datos, String db) throws SQLException;
}
