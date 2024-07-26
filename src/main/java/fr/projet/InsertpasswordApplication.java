package fr.projet;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Scanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.SpringBootApplication; 

@SpringBootApplication
public class InsertpasswordApplication {

	public static void main(String[] args) {

	  final Logger logger = LoggerFactory.getLogger(InsertpasswordApplication.class);
      
	  final  File repertoirein = new File("C:\\DATAPASSWORD\\IN\\"); 
	  final  File repertoireout = new File("C:\\DATAPASSWORD\\TRAITE\\"); 
         
    	if (repertoirein.isDirectory()) {
			System.out.println("répertoire");
			int batchIndex = 0;
            
			File[] listOfFiles = repertoirein.listFiles();
			if(listOfFiles.length > 0) {

				try (Connection connection = 
      			DriverManager.getConnection("jdbc:clickhouse://127.0.0.1:8123/passworddb", 
				"default", "")) {
					connection.setAutoCommit(false);
			
					for (int i = 0; i < listOfFiles.length; i++){
						batchIndex = 0;
						logger.info("Traitement du fichier"+listOfFiles[i].getName());
						
						String fileName = repertoirein + "\\" + listOfFiles[i].getName();
						
						String buffer ;
						String[] bufferTab;
					
						File file = new File(fileName);
						fileName = repertoireout + "\\" + listOfFiles[i].getName()+".ok";
						
						File fileout = new File(fileName);
						
						Scanner scanner = new Scanner(file);
						
						try (PreparedStatement statement = 
								connection.prepareStatement("INSERT INTO passwordtbl  VALUES (?)")) {
							while (scanner.hasNextLine()){
								buffer = scanner.nextLine();
								batchIndex++;
								
								bufferTab = buffer.split(":");
								if (bufferTab.length > 0){
										statement.setString(1, bufferTab[0]);	
										
										statement.addBatch();

										if (batchIndex == 100_000) {
											statement.executeBatch();
											connection.commit();
											batchIndex = -1;
										}									
								}					 
							} 
							statement.executeBatch();
							connection.commit();
							scanner.close();	
							file.renameTo(fileout);
							
						}	
            		}
				  	  
				} catch (Exception ex) {
					ex.printStackTrace();
					logger.error("Impossible de se connecter ...");
				}
			} else {
				logger.info("Répertoire vide ...");
			}
		} 
		
}

}
