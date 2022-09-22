import java.io.FileReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.io.FileNotFoundException;

import java.io.IOException;


import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;

import clases.Cliente;
import clasesDAO.ClienteDao;
import factory.DAOFactory;
import interfaces.DAO;


public class Main {

    private static ClienteDao clienteDb1;
	private static ClienteDao clienteDb2;
    public static void main(String[] args) throws SQLException, FileNotFoundException, IOException{

		
            String db1 = "mysql";
            String db2 = "derby";
            CSVParser parserClientes = CSVFormat.DEFAULT.withHeader().parse(new FileReader("src/main/java/csv/clientes.csv"));
            
            // DAOFactory dbElegida1 = DAOFactory.getDAOFactory(db1);
            // clienteDb1=dbElegida1.getClienteDAO(db1);
            // clienteDb1.parserDatos(parserClientes, db1);
            // System.out.println("Clientes db " + db1 + " " + clienteDb1.listaDeClientes(db1));

            DAOFactory dbElegida2=DAOFactory.getDAOFactory(db2);
            clienteDb2=dbElegida2.getClienteDAO(db2);
            clienteDb2.parserDatos(parserClientes, db2);
            System.out.println("Clientes db " + db2 + " " + clienteDb2.listaDeClientes(db2));
        
            

 
        }
    
    
}