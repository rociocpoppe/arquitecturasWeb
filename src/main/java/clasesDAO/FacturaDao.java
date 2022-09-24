package clasesDAO;

import factory.DerbyDB;
import factory.MySqlDB;
import interfaces.DAO;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import clases.Factura;

public class FacturaDao implements DAO <FacturaDao>{
    
    private static final String MYSQL_DB = "mysql";
	private static final String DERBY_DB = "derby";
    
	private Connection conn;

	public FacturaDao(String db) throws SQLException {
		this.crearTabla(db);
	}

	@Override
	public void crearTabla(String db) throws SQLException {
		switch (db) {
		case MYSQL_DB:
			this.conn = MySqlDB.crearConeccion();
			String eliminarConstraint="ALTER TABLE factura_producto DROP FOREIGN KEY Factura_Producto_Factura";
			conn.prepareStatement(eliminarConstraint).execute();
			conn.commit();
			String eliminarTablaMySql= "DROP  TABLE  IF EXISTS factura";
			conn.prepareStatement(eliminarTablaMySql).execute();
			conn.commit();
			String clienteMYSQL = "CREATE TABLE IF NOT EXISTS factura(" 
								+ "idFactura INT," 
								+ "idCliente INT,"
								+ "CONSTRAINT PK_Factura PRIMARY KEY (idFactura),"
								+ "CONSTRAINT Factura_ClienteFK FOREIGN KEY (idCliente) REFERENCES cliente (idCliente) ON DELETE CASCADE)";
			this.conn.prepareStatement(clienteMYSQL).execute();
			this.conn.commit();
			this.conn.close();
			break;
		case DERBY_DB:
			this.conn = DerbyDB.crearConeccion();
			String eliminarTablaDerby= "DROP  TABLE factura";
			conn.prepareStatement(eliminarTablaDerby).execute();
			conn.commit();
			String clienteDerby = "CREATE TABLE factura(" 
								+ "idFactura INT," 
								+ "idCliente INT,"
								+ "CONSTRAINT PK_Factura PRIMARY KEY (idFactura),"
								+ "CONSTRAINT Factura_Cliente_FK"
								+ " FOREIGN KEY (idCliente) REFERENCES cliente (idCliente))";
			conn.prepareStatement(clienteDerby).execute();
			conn.commit();
			break;
		}
	}

	@Override
	public void parserDatos(CSVParser facturas, String db) throws SQLException {
		switch (db) {
			case MYSQL_DB:
				this.conn = MySqlDB.crearConeccion();
				break;
	
			case DERBY_DB:
				this.conn = DerbyDB.crearConeccion();
				break;
			}
	
		for(CSVRecord row: facturas) {
			int idFactura=0;
            int idCliente=0;
			idFactura=Integer.parseInt(row.get("idFactura"));
			idCliente = Integer.parseInt(row.get("idCliente"));
			insertarFactura(idFactura, idCliente);
		}
		conn.commit();
		this.conn.close();
	}


	private void insertarFactura(int idFactura, int idCliente) throws SQLException{
		String insert = "INSERT INTO factura (idFactura, idCliente) VALUES(?,?)";
		PreparedStatement ps = conn.prepareStatement(insert);
		ps.setInt(1, idFactura);
		ps.setInt(2, idCliente);
		ps.executeUpdate();
		ps.close();
	}

	public ArrayList<Factura> listaDeFacturas(String db) throws SQLException {
		ArrayList<Factura> facturas = new ArrayList<Factura>();
		switch (db) {
		case MYSQL_DB:
			this.conn = MySqlDB.crearConeccion();
			break;
		case DERBY_DB:
			this.conn = DerbyDB.crearConeccion();
			break;
		}
		String select = "SELECT * FROM factura";
		PreparedStatement ps = conn.prepareStatement(select);
		ResultSet rs = ps.executeQuery();
		while (rs.next()) {
			Factura factura = new Factura(rs.getInt(1),rs.getInt(2));
			facturas.add(factura);
		}
		this.conn.commit();
		ps.close();

        //que pasa con derby???
		this.conn.close();

		return facturas;
	}
}
