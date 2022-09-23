import java.io.FileReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.io.FileNotFoundException;

import java.io.IOException;


import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;

import clases.Cliente;
import clases.Factura;
import clasesDAO.ClienteDao;
import clasesDAO.ProductoDao;
import clasesDAO.FacturaDao;
import clasesDAO.Factura_ProductoDao;
import factory.DAOFactory;
import interfaces.DAO;


public class Main {

    private static ClienteDao clienteDb1;
	private static ClienteDao clienteDb2;
    private static ProductoDao ProductoDb1;
    private static ProductoDao ProductoDb2;
    private static FacturaDao FacturaDb1;
    private static FacturaDao FacturaDb2;
    private static Factura_ProductoDao Factura_ProductoDb1;
    private static Factura_ProductoDao Factura_ProductoDb2;

    public static void main(String[] args) throws SQLException, FileNotFoundException, IOException{

		
            String db1 = "mysql";
            String db2 = "derby";
            CSVParser parserClientes = CSVFormat.DEFAULT.withHeader().parse(new FileReader("src/main/java/csv/clientes.csv"));
            CSVParser parserFacturas = CSVFormat.DEFAULT.withHeader().parse(new FileReader("src/main/java/csv/facturas.csv"));
            CSVParser parserFactura_producto = CSVFormat.DEFAULT.withHeader().parse(new FileReader("src/main/java/csv/facturas-productos.csv"));
            CSVParser parserProducto = CSVFormat.DEFAULT.withHeader().parse(new FileReader("src/main/java/csv/productos.csv"));
            
            DAOFactory dbElegida1 = DAOFactory.getDAOFactory(db1);
            // clienteDb1=dbElegida1.getClienteDAO(db1);
            // clienteDb1.parserDatos(parserClientes, db1);
            // System.out.println("Clientes db " + db1 + " " + clienteDb1.listaDeClientes(db1));
            // ProductoDb1=dbElegida1.getProductoDAO(db1);
            // ProductoDb1.parserDatos(parserProducto, db1);
            // System.out.println(" Productos db "+ db1 + ProductoDb1.listaDeProductos(db1));
            DAOFactory dbElegida2=DAOFactory.getDAOFactory(db2);
            // clienteDb2=dbElegida2.getClienteDAO(db2);
            // clienteDb2.parserDatos(parserClientes, db2);
            // System.out.println("Clientes db " + db2 + " " + clienteDb2.listaDeClientes(db2));
            ProductoDb2=dbElegida2.getProductoDAO(db2);
            ProductoDb2.parserDatos(parserProducto, db2);
            System.out.println(" Productos db "+ db2 + ProductoDb2.listaDeProductos(db2));

 
        }
    
    
}