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

import clases.Producto;

public class ProductoDao implements DAO <ProductoDao>{
    private static final String MYSQL_DB = "mysql";
	private static final String DERBY_DB = "derby";
    
	private Connection conn;

	public ProductoDao(String db) throws SQLException {
		this.crearTabla(db);
	}

	@Override
	public void crearTabla(String db) throws SQLException {
		switch (db) {
		case MYSQL_DB:
			this.conn = MySqlDB.crearConeccion();
			String eliminarConstraint="ALTER TABLE factura_producto DROP FOREIGN KEY Factura_Producto_Producto";
			conn.prepareStatement(eliminarConstraint).execute();
			conn.commit();
			String eliminarTablaMySql= "DROP  TABLE IF EXISTS producto";
			conn.prepareStatement(eliminarTablaMySql).execute();
			conn.commit();
			String clienteMYSQL = "CREATE TABLE IF NOT EXISTS producto(" 
								+ "idProducto INT," 
								+ "nombre VARCHAR(45),"
								+ "valor FLOAT," 
								+ "CONSTRAINT PK_Producto PRIMARY KEY (idProducto))";
			this.conn.prepareStatement(clienteMYSQL).execute();
			this.conn.commit();
			this.conn.close();
			break;
		case DERBY_DB:
			this.conn = DerbyDB.crearConeccion();
			String eliminarConstraintd="ALTER TABLE factura_producto DROP FOREIGN KEY Factura_Producto_Producto";
			conn.prepareStatement(eliminarConstraintd).execute();
			conn.commit();
			String eliminarTablaDerby= "DROP  TABLE producto";
			conn.prepareStatement(eliminarTablaDerby).execute();
			conn.commit();
			String clienteDerby =  "CREATE TABLE producto(" 
								+ "idProducto INT," 
								+ "nombre VARCHAR(45),"
            					+ "valor FLOAT," 
								+ "CONSTRAINT PK_Producto PRIMARY KEY (idProducto))";
			conn.prepareStatement(clienteDerby).execute();
			conn.commit();
			System.out.println("success tabla producto");
			break;
		}
	}

	@Override
	public void parserDatos(CSVParser productos, String db) throws SQLException {
		switch (db) {
			case MYSQL_DB:
				this.conn = MySqlDB.crearConeccion();
				break;
	
			case DERBY_DB:
				this.conn = DerbyDB.crearConeccion();
				break;
			}
	
		for(CSVRecord row: productos) {
			int idProducto=0;
			String name="";
			float valor=0;
			idProducto = Integer.parseInt(row.get("idProducto"));
			name = row.get("nombre");
			valor =Float.parseFloat(row.get("valor"));
			insertarProducto(idProducto, name, valor);
		}
		conn.commit();
		this.conn.close();
	}


	private void insertarProducto(int idProducto, String nombre, float valor) throws SQLException{
		String insert = "INSERT INTO producto (idProducto, nombre, valor) VALUES(?,?,?)";
		PreparedStatement ps = conn.prepareStatement(insert);
		ps.setInt(1, idProducto);
		ps.setString(2, nombre);
		ps.setFloat(3, valor);
		ps.executeUpdate();
		ps.close();
	}

	public Producto obtenerProductoConMasRecaudacion(String db) throws SQLException {
		Producto producto= new Producto();
		switch (db) {
		case MYSQL_DB:
			this.conn = MySqlDB.crearConeccion();
			String select = "SELECT p.*, SUM(p.valor * fp.cantidad) as sumaTotal "
						+ "FROM producto p JOIN factura_producto fp ON (p.idProducto = fp.idProducto)" 
						+ "WHERE p.idProducto = fp.idProducto "
						+ "GROUP BY idProducto "
						+ "ORDER BY `sumaTotal` DESC LIMIT 1 ";
		PreparedStatement ps = conn.prepareStatement(select);
		ResultSet rs = ps.executeQuery();
		while (rs.next()) {
			producto = new Producto(rs.getInt(1), rs.getString(2), rs.getInt(3));
			
		}
		this.conn.commit();
		ps.close();

			break;
		case DERBY_DB:
			this.conn = DerbyDB.crearConeccion();
			String selectDby = "SELECT p.*, SUM(p.valor * fp.cantidad) as sumaTotal"
			+ " FROM producto p JOIN factura_producto fp ON (p.idProducto = fp.idProducto)"
			+ " WHERE p.idProducto = fp.idProducto"
			  + " GROUP BY p.IDPRODUCTO, p.NOMBRE, p.valor"
			  +" ORDER BY sumaTotal ASC";

			PreparedStatement psDby = conn.prepareStatement(selectDby);
			ResultSet rsDby = psDby.executeQuery();
			while (rsDby.next()) {
			producto = new Producto(rsDby.getInt(1), rsDby.getString(2), rsDby.getInt(3));
			
			}
			this.conn.commit();
			psDby.close();
			break;
		}
		
        //que pasa con derby???
		this.conn.close();

		return producto;
	}
}
