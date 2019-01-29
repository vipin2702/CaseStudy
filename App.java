package aisehi2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;

public class App {
	
	public static void main(String[] args) throws Exception  {

		FileReader fr = new FileReader("myfile2.txt");
		BufferedReader br = new BufferedReader(fr);
		
		String str = null;
		int count = 0;
		
		long start = System.currentTimeMillis();
		
		Path path = Paths.get("myfile2.txt");
		long lineCount = Files.lines(path).count();
		System.out.println("Data enties available in file:" + (lineCount-1));
		
		if(lineCount==1000001) 
		{
			String query = "insert into data2 values(?,?,?,?,?)";
			String url = "jdbc:oracle:thin:@localhost:1521:xe";
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection con = DriverManager.getConnection(url, "system", "system");
			PreparedStatement ps = con.prepareStatement(query);
			Statement st = con.createStatement();
			st.executeUpdate("truncate table data2");

			while (!(str = br.readLine()).equalsIgnoreCase("Trailer1000001")) {
				String[] str1 = str.split("\\|");
				
				ps.setString(1, str1[0]);
				ps.setString(2, str1[1]);
				ps.setString(3, str1[2]);
				ps.setString(4, str1[3]);
				ps.setString(5, str1[4]);

				count = count + ps.executeUpdate();
			}
			
			long end = System.currentTimeMillis();
			System.out.println("Time elapsed = "+(end-start)+"ms");
			
			System.out.println(count+"row/s updated");
			
		}
		
		else {
			try {
				throw new Exception() ;	
			}
			catch(Exception e) {
				System.out.println("Exception: The entries in file are either less or more than 1 million records.");
			}
		}
		
	}
}
