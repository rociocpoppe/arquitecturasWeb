package clasesDAO;

import clases.Cliente;
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


public class ClienteDao implements DAO<Cliente> {

    private static final String MYSQL_DB = "mysql";
	private static final String DERBY_DB = "derby";
    
	private Connection conn;

	public ClienteDao(String db) throws SQLException {
		if(!existeTabla(db)){
			this.crearTabla(db);
		}	
	}

	private boolean existeTabla(String db) throws SQLException {
		boolean cumple=false;
		switch (db) {
			case MYSQL_DB:
				this.conn = MySqlDB.crearConeccion();
				String existe="SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'mysqlEntregable' AND TABLE_NAME = 'cliente'";
				PreparedStatement ps = conn.prepareStatement(existe);
				ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				String aux = rs.getString(3);
				if(aux=="cliente"){
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
			String clienteMYSQL = 
								"CREATE TABLE IF NOT EXISTS cliente(" 
								+ "idCliente INT NOT NULL," 
								+ "nombre VARCHAR(500),"
								+ "email VARCHAR(150)," 
								+ "CONSTRAINT PK_Cliente PRIMARY KEY (idCliente))";
			this.conn.prepareStatement(clienteMYSQL).execute();
			this.conn.commit();
			this.conn.close();
			break;
		case DERBY_DB:
			this.conn = DerbyDB.crearConeccion();
			String clienteDerby = "CREATE TABLE cliente(" 
								+ "idCliente INT," 
								+ "nombre VARCHAR(500),"
            					+ "email VARCHAR(150)," 
								+ "CONSTRAINT PK_Cliente PRIMARY KEY (idCliente))";
			conn.prepareStatement(clienteDerby).execute();
			conn.commit();
			System.out.println("success tabla cliente");
			break;
		}
	}

	@Override
	public void parserDatos(CSVParser clientes, String db) throws SQLException {
		switch (db) {
			case MYSQL_DB:
				this.conn = MySqlDB.crearConeccion();
				break;
	
			case DERBY_DB:
				this.conn = DerbyDB.crearConeccion();
				break;
			}
	
		for(CSVRecord row: clientes) {
			int idCliente=0;
			String nombre="";
			String email="";
			idCliente = Integer.parseInt(row.get(idCliente));
			nombre = row.get("nombre");
			email = row.get("email");
			insertarCliente(idCliente, nombre, email);
		}
		conn.commit();
		this.conn.close();
	}


	private void insertarCliente(int idCliente, String nombre, String email) throws SQLException{
		String insert = "INSERT INTO cliente (idCliente, nombre, email) VALUES(?,?,?)";
		PreparedStatement ps = conn.prepareStatement(insert);
		ps.setInt(1, idCliente);
		ps.setString(2, nombre);
		ps.setString(3, email);
		ps.executeUpdate();
		ps.close();
	}

	public ArrayList<Cliente> listaDeClientesOrdenada(String db) throws SQLException {
		ArrayList<Cliente> clientes = new ArrayList<Cliente>();
		switch (db) {
		case MYSQL_DB:
			this.conn = MySqlDB.crearConeccion();
			String select = "SELECT c.*, SUM( p.valor * fp.cantidad) as sumaTotal" 
						+" FROM cliente c JOIN factura f ON (c.idCliente = f.idCliente)"
						+" JOIN factura_producto fp ON (f.idFactura=fp.idFactura)"
						+" JOIN producto p ON (fp.idProducto=p.idProducto)"
						+" WHERE c.idCliente= f.idCliente AND f.idFactura=fp.idFactura "
						+" GROUP BY idCliente"
						+" ORDER BY `sumaTotal` DESC";
			PreparedStatement ps = conn.prepareStatement(select);
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				Cliente cliente = new Cliente(rs.getInt(1), rs.getString(2), rs.getString(3));
				clientes.add(cliente);
			}
			this.conn.commit();
			ps.close();
				break;
		case DERBY_DB:
			this.conn = DerbyDB.crearConeccion();
			String selectDerby = " SELECT c.*, SUM( p.valor * fp.cantidad) as sumaTotal"
								+ " FROM cliente c JOIN factura f ON (c.idCliente = f.idCliente)"
								+ " JOIN factura_producto fp ON (f.idFactura=fp.idFactura)"
			    				+ " JOIN producto p ON (fp.idProducto=p.idProducto)"
								+ " WHERE c.idCliente= f.idCliente AND f.idFactura=fp.idFactura"
								+ " GROUP BY c.idCliente, c.NOMBRE, c.email"
								+ " ORDER BY sumaTotal DESC";
			
			PreparedStatement psDby = conn.prepareStatement(selectDerby);
			ResultSet rsDby = psDby.executeQuery();
			while (rsDby.next()) {
				Cliente cliente = new Cliente(rsDby.getInt(1), rsDby.getString(2), rsDby.getString(3));
				clientes.add(cliente);
			}
			this.conn.commit();
			psDby.close();
			break;
		}
		
	
		this.conn.close();

		return clientes;
	}
}