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

    private static ClienteDao ClienteDb1;
	private static ClienteDao ClienteDb2;
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
            

            //se crean las tablas

            DAOFactory dbElegida1 = DAOFactory.getDAOFactory(db1);
            ClienteDb1=dbElegida1.getClienteDAO(db1);
            FacturaDb1=dbElegida1.getFacturaDAO(db1);
            ProductoDb1=dbElegida1.getProductoDAO(db1);
            Factura_ProductoDb1=dbElegida1.getFactura_ProductoDAO(db1);

            //se cargan los datos  !SE DEBEN CARGAR LOS DATOS SOLO LA PRIMERA VEZ

            //ClienteDb1.parserDatos(parserClientes, db1);
            //FacturaDb1.parserDatos(parserFacturas, db1); 
            //ProductoDb1.parserDatos(parserProducto, db1);
            //Factura_ProductoDb1.parserDatos(parserFactura_producto, db1);
            

            System.out.println("*********************CLIENTES**********************************");
            System.out.println("Clientes db " + db1 + " " + ClienteDb1.listaDeClientesOrdenada(db1));
            
            System.out.println("*********************PRODUCTOS**********************************");
            System.out.println(" Productos db "+ db1 + ProductoDb1.obtenerProductoConMasRecaudacion(db1));
           
            DAOFactory dbElegida2 = DAOFactory.getDAOFactory(db2);

            //se crean las tablas
            ClienteDb2=dbElegida2.getClienteDAO(db2);
            FacturaDb2=dbElegida2.getFacturaDAO(db2); 
            ProductoDb2=dbElegida2.getProductoDAO(db2);
            Factura_ProductoDb2=dbElegida2.getFactura_ProductoDAO(db2);

            //se cargan los datos  !SE DEBEN CARGAR LOS DATOS SOLO LA PRIMERA VEZ
            //ClienteDb2.parserDatos(parserClientes, db2);
            //FacturaDb2.parserDatos(parserFacturas, db2);
            // ProductoDb2.parserDatos(parserProducto, db2);
            //Factura_ProductoDb2.parserDatos(parserFactura_producto, db2);
            

            System.out.println("*********************CLIENTES**********************************");
            System.out.println("Clientes db " + db2 + " " + ClienteDb2.listaDeClientesOrdenada(db2));
            System.out.println("*********************PRODUCTOS**********************************");
            System.out.println(" Productos db "+ db2 + ProductoDb2.obtenerProductoConMasRecaudacion(db2));
           
        }
    
    
}