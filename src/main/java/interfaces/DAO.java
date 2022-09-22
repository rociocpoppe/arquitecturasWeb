package interfaces;

import java.sql.SQLException;

import org.apache.commons.csv.CSVParser;

public interface DAO <T> {
    
	public void crearTabla(String db) throws SQLException;

	public void parserDatos( CSVParser datos, String db) throws SQLException;
}
