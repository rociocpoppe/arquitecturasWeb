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
		this.crearTabla(db);
	}

	@Override
	public void crearTabla(String db) throws SQLException {
		switch (db) {
		case MYSQL_DB:
			 this.conn = MySqlDB.crearConeccion();
			String eliminarConstraint="ALTER TABLE factura DROP FOREIGN KEY Factura_ClienteFK";
			conn.prepareStatement(eliminarConstraint).execute();
			conn.commit();
			// conn.prepareStatement(eliminarConstraint).execute();
			// conn.commit();
			String eliminarTablaMySql= "DROP TABLE IF EXISTS cliente";
			conn.prepareStatement(eliminarTablaMySql).execute();
			conn.commit();
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
			String eliminarConstraintD="ALTER TABLE factura DROP FOREIGN KEY Factura_ClienteFK";
			conn.prepareStatement(eliminarConstraintD).execute();
			conn.commit();
			String eliminarTabla= "DROP TABLE cliente";
			conn.prepareStatement(eliminarTabla).execute();
			conn.commit();
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

	public ArrayList<Cliente> listaDeClientes(String db) throws SQLException {
		ArrayList<Cliente> clientes = new ArrayList<Cliente>();
		switch (db) {
		case MYSQL_DB:
			this.conn = MySqlDB.crearConeccion();
			break;
		case DERBY_DB:
			this.conn = DerbyDB.crearConeccion();
			break;
		}
		String select = "SELECT * FROM cliente";
		PreparedStatement ps = conn.prepareStatement(select);
		ResultSet rs = ps.executeQuery();
		while (rs.next()) {
			Cliente cliente = new Cliente(rs.getInt(1), rs.getString(2), rs.getString(3));
			clientes.add(cliente);
		}
		this.conn.commit();
		ps.close();

        //que pasa con derby???
		this.conn.close();

		return clientes;
	}
}