package clasesDAO;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

import factory.DerbyDB;
import factory.MySqlDB;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import clases.Factura_Producto;
import interfaces.DAO;

public class Factura_ProductoDao implements DAO<Factura_ProductoDao> {
    
    private static final String MYSQL_DB = "mysql";
	private static final String DERBY_DB = "derby";
    
	private Connection conn;

	public Factura_ProductoDao(String db) throws SQLException {
		if(!existeTabla(db)){
			this.crearTabla(db);
		}
	}


	private boolean existeTabla(String db) throws SQLException {
		boolean cumple=false;
		switch (db) {
			case MYSQL_DB:
				this.conn = MySqlDB.crearConeccion();
				String existe="SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'mysqlEntregable' AND TABLE_NAME = 'factura_producto'";
				PreparedStatement ps = conn.prepareStatement(existe);
				ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				String aux = rs.getString(3);
				if(aux=="factura_producto"){
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
			String clienteMYSQL = "CREATE TABLE IF NOT EXISTS factura_producto(" + "idFactura INT," + "idProducto INT,"
					+ "cantidad INT," + "CONSTRAINT PK_Factura_Producto PRIMARY KEY (idFactura,idProducto),"
					+"CONSTRAINT Factura_Producto_Producto FOREIGN KEY (idProducto) REFERENCES producto (idProducto) ON DELETE CASCADE,"
					+"CONSTRAINT Factura_Producto_factura FOREIGN KEY (idFactura) REFERENCES factura (idFactura) ON DELETE CASCADE)";
			this.conn.prepareStatement(clienteMYSQL).execute();
			this.conn.commit();
			this.conn.close();
			break;
		case DERBY_DB:
			this.conn = DerbyDB.crearConeccion();
			String clienteDerby = "CREATE TABLE factura_producto(" + "idFactura INT," + "idProducto INT,"
			+ "cantidad INT," + "CONSTRAINT PK_Factura_Producto PRIMARY KEY (idFactura,idProducto),"
			+"CONSTRAINT Factura_Producto_Producto FOREIGN KEY (idProducto) REFERENCES producto (idProducto) ON DELETE CASCADE,"
			+"CONSTRAINT Factura_Producto_factura FOREIGN KEY (idFactura) REFERENCES factura (idFactura) ON DELETE CASCADE)";
			conn.prepareStatement(clienteDerby).execute();
			conn.commit();
			System.out.println("success tabla factura_producto");
			break;
		}
	}

	@Override
	public void parserDatos(CSVParser facturas_productos, String db) throws SQLException {
		switch (db) {
			case MYSQL_DB:
				this.conn = MySqlDB.crearConeccion();
				break;
	
			case DERBY_DB:
				this.conn = DerbyDB.crearConeccion();
				break;
		}
	
		for(CSVRecord row: facturas_productos) {
			int idFactura=0;
            int idProducto=0;
            int cantidad=0;
            idFactura=Integer.parseInt(row.get("idFactura"));
			idProducto=Integer.parseInt(row.get("idProducto"));
			cantidad=Integer.parseInt(row.get("cantidad"));
			insertarFactura_Producto(idFactura, idProducto, cantidad);
		}
		conn.commit();
		this.conn.close();
	}


	private void insertarFactura_Producto(int idFactura, int idProducto, int cantidad) throws SQLException{
		String insert = "INSERT INTO factura_producto (idFactura, idProducto, cantidad) VALUES(?,?,?)";
		PreparedStatement ps = conn.prepareStatement(insert);
		ps.setInt(1, idFactura);
		ps.setInt(2, idProducto);
		ps.setInt(3, cantidad);
		ps.executeUpdate();
		ps.close();
	}

	public ArrayList<Factura_Producto> listaDeFacturas_Prod(String db) throws SQLException {
		ArrayList<Factura_Producto> facturas_producto = new ArrayList<Factura_Producto>();
		switch (db) {
		case MYSQL_DB:
			this.conn = MySqlDB.crearConeccion();
			break;
		case DERBY_DB:
			this.conn = DerbyDB.crearConeccion();
			break;
		}
		String select = "SELECT * FROM factura_producto";
		PreparedStatement ps = conn.prepareStatement(select);
		ResultSet rs = ps.executeQuery();
		while (rs.next()) {
			Factura_Producto factura_prod = new Factura_Producto(rs.getInt(1), rs.getInt(2), rs.getInt(3));
			facturas_producto.add(factura_prod);
		}
		this.conn.commit();
		ps.close();

    
		this.conn.close();

		return facturas_producto;
	}
	
}
