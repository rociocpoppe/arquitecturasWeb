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
		if(!existeTabla(db)){
			this.crearTabla(db);
		}
	}


	private boolean existeTabla(String db) throws SQLException {
		boolean cumple=false;
		switch (db) {
			case MYSQL_DB:
				this.conn = MySqlDB.crearConeccion();
				String existe="SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'mysqlEntregable' AND TABLE_NAME = 'factura'";
				PreparedStatement ps = conn.prepareStatement(existe);
				ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				String aux = rs.getString(3);
				if(aux=="factura"){
					cumple= true;
				}
			}
				conn.commit();
				this.conn.close();
			break;
			case DERBY_DB:
				this.conn = DerbyDB.crearConeccion();
				java.sql.DatabaseMetaData dbmd = this.conn.getMetaData();
				ResultSet rs1 = dbmd.getTables(null, null, "producto",null);
				if(rs1.next())
				{
					cumple=true;
				}
				
				conn.commit();
				this.conn.close();
			break;
		}
		return cumple;
	}


	@Override
	public void crearTabla(String db) throws SQLException {
		switch (db) {
		case MYSQL_DB:
			this.conn = MySqlDB.crearConeccion();
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
			String clienteDerby = "CREATE TABLE factura(" 
								+ "idFactura INT," 
								+ "idCliente INT,"
								+ "CONSTRAINT PK_Factura PRIMARY KEY (idFactura),"
								+ "CONSTRAINT Factura_ClienteFK FOREIGN KEY (idCliente) REFERENCES cliente (idCliente) ON DELETE CASCADE)";
			conn.prepareStatement(clienteDerby).execute();
			conn.commit();
			System.out.println("success tabla factura");
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
		this.conn.close();

		return facturas;
	}
}
