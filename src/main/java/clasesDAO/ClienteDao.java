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
			String clienteMYSQL = "CREATE TABLE IF NOT EXISTS cliente(" + "idCliente INT," + "nombre VARCHAR(500),"
					+ "email VARCHAR(150)," + "PRIMARY KEY(idCliente))";
			this.conn.prepareStatement(clienteMYSQL).execute();
			this.conn.commit();
			this.conn.close();
			break;
		case DERBY_DB:
			this.conn = DerbyDB.crearConeccion();
			// String dropTableInDerby= "DROP  TABLE mascota";
			// conn.prepareStatement(dropTableInDerby).execute();
			//   conn.commit();
			String clienteDerby = "CREATE TABLE IF NOT EXISTS cliente(" + "idCliente INT," + "nombre VARCHAR(500),"
            + "email VARCHAR(150)," + "PRIMARY KEY(idCliente))";
			conn.prepareStatement(clienteDerby).execute();
			conn.commit();
			break;
		}
	}

	@Override
	public void insertarDatos(Cliente cliente, String db) throws SQLException {
		int idCliente = cliente.getIdCliente();
		String nombre = cliente.getNombre();
		String email = cliente.getEmail();

		switch (db) {
		case MYSQL_DB:
			this.conn = MySqlDB.crearConeccion();
			break;

		case DERBY_DB:
			this.conn = DerbyDB.crearConeccion();
			break;
		}
		String insert = "INSERT INTO cliente (idCliente, nombre, email) VALUES(?,?,?)";
		PreparedStatement ps = conn.prepareStatement(insert);
		ps.setInt(1, idCliente);
		ps.setString(2, nombre);
		ps.setString(3, email);
		ps.executeUpdate();
		ps.close();
		conn.commit();
		this.conn.close();
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