import java.sql.SQLException;

import clasesDAO.ClienteDao;
import factory.DAOFactory;

public class Main {

    private static ClienteDao clienteDb1;
	private static ClienteDao clienteDb2;
    public static void main(String[] args) throws SQLException{

		
            String db1 = "mysql";
            String db2 = "derby";
            
            DAOFactory dbElegida1 = DAOFactory.getDAOFactory(db1);
            clienteDb1=dbElegida1.getClienteDAO(db1);

            // mascotaDAO1 = dbElegida1.getMascotaDAO(db1);
            // Mascota mascota1 = new Mascota(1, "Roky", "perro");
            // Mascota mascota2 = new Mascota(2, "Gato Blanco", "gato");
            // mascotaDAO1.crearTabla(db1);
            // mascotaDAO1.insertarDatos(mascota1,db1);
            // mascotaDAO1.insertarDatos(mascota2,db1);
            // System.out.println(mascotaDAO1.listaDeMascotas(db1));
            
            // CreadorDAOs dbElegida2 = CreadorDAOs.getDAOFactory(db2);
            // mascotaDAO2 = dbElegida2.getMascotaDAO(db2);
            // Mascota mascota3 = new Mascota(3, "Darwin", "perro");
            // Mascota mascota4 = new Mascota(4, "Ceni", "gato");
            // mascotaDAO2.crearTabla(db2);
            // mascotaDAO2.insertarDatos(mascota3,db2);
            // mascotaDAO2.insertarDatos(mascota4,db2);
            // System.out.println(mascotaDAO2.listaDeMascotas(db2));
        }
    
    
}