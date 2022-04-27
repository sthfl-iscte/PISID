package cloudToLocal_Mongo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.DuplicateKeyException;
import com.mongodb.MongoClient;

import mongoToMySQL.Sensor;
import mongoToMySQL.Zona;


public class ConnectMongoCloud {
	
	private Sensor S_H1;
	private Sensor S_H2;
	private Sensor S_T1;
	private Sensor S_T2;
	private Sensor S_L1;
	private Sensor S_L2;
	
	public Connection connectionMysqlCloud() throws ClassNotFoundException, SQLException {
		Connection conn = DriverManager.getConnection("jdbc:mysql://194.210.86.10/sid2022?" + "user=aluno&password=aluno");
		System.out.println("Connected to cloud MySQL!");
		return conn;
	}
	
	public void runOnCloud(Connection connection) throws SQLException { // para ir buscar dados das tabelas zona e sensor ao MySQL Cloud
//		String query = "SELECT * FROM zona";
//		Statement stmt = connection.createStatement();
//		ResultSet rs = stmt.executeQuery(query);
//		while (rs.next()) {
//			int idzona = rs.getInt("idzona");
//			double temperatura = rs.getDouble("temperatura");
//			double humidade = rs.getDouble("humidade");
//			double luz = rs.getDouble("luz");
//			Zona z = new Zona(idzona, temperatura, humidade, luz);
//			System.out.println(z.toString());
//			zonas.add(z);
//		}

		String query2 = "SELECT * FROM sensor";
		Statement stmt2 = connection.createStatement();
		ResultSet rs2 = stmt2.executeQuery(query2);
		while (rs2.next()) {
			int idsensor = rs2.getInt("idsensor");
			String tipo = rs2.getString("tipo");
			double limiteinferior = rs2.getDouble("limiteinferior");
			double limitesuperior = rs2.getDouble("limitesuperior");
			int idzona = rs2.getInt("idzona");
			
			String idzonaS = rs2.getString("idzona");
			
			String TZ = tipo + idzonaS;
			Sensor s = new Sensor(idsensor, tipo, limiteinferior, limitesuperior, idzona);
			switch(TZ) {
			case "H1": 
				this.S_H1 = s;
				break;
			case "H2":
				this.S_H2 = s;
				break;
			case "T1":
				this.S_T1 = s;
				break;
			case "T2":
				this.S_T2 = s;
				break;
			case "L1":
				this.S_L1 = s;
				break;
			case "L2":
				this.S_L2 = s;
				break;
			}
		}

	}

	public MongoClient connection() {
		MongoClient mongoClient = new MongoClient("194.210.86.10", 27017);
		//DB database = mongoClient.getDB("db");
		//mongoClient.getDatabaseNames().forEach(System.out::println);
		return mongoClient;
	}
	
	public MongoClient connectionLocal() {
		MongoClient mongoClient = new MongoClient("localhost", 27017);
		//DB database = mongoClient.getDB("db");
		//mongoClient.getDatabaseNames().forEach(System.out::println);
		
		return mongoClient;
	}
	
	public DB databaseEntry(MongoClient mongoClient, String name) {
		DB database = mongoClient.getDB(name);
		System.out.println("Connect to " + name + " successfully");

		return database;
	}
	
	public DBCollection collectionOpen(DB database, String db) {
		DBCollection collection = database.getCollection(db);
		System.out.println("Connected to Collection: " + db);

		return collection;
	}
	
	public void insertMongoLocal(DBCollection mongoCloud,DB dblocal) {		
		DBCursor cursor = mongoCloud.find(); //mongo Stores
		
		int count = 0;
		int duplicate = 0;
		
		while(cursor.hasNext()) {
			count++;
			DBObject object = cursor.next();
			BasicDBObject newEntry = new BasicDBObject();
			newEntry.put("_id", object.get("_id"));
			newEntry.put("Zona", object.get("Zona"));
			newEntry.put("Sensor", object.get("Sensor"));
			newEntry.put("Data", object.get("Data"));
			newEntry.put("Medicao", object.get("Medicao"));
			
			double medicao = Double.parseDouble(object.get("Medicao").toString());
			
			if(object.get("Zona").equals("Z1")) {
				if(object.get("Sensor").equals("H1")) {
					if(medicao > this.S_H1.getLimiteinferior() && medicao < this.S_H1.getLimitesuperior())
						duplicate = insertInCollection(dblocal, newEntry, "sensorhum1", duplicate);
					else
						duplicate = insertInCollection(dblocal, newEntry, "lixo", duplicate);
				}
				if(object.get("Sensor").equals("T1")) {
					if(medicao > this.S_T1.getLimiteinferior() && medicao < this.S_T1.getLimitesuperior())
						duplicate = insertInCollection(dblocal, newEntry, "sensortemp1", duplicate);
					else
						duplicate = insertInCollection(dblocal, newEntry, "lixo", duplicate);
				}
				if(object.get("Sensor").equals("L1")) {
					if(medicao > this.S_L1.getLimiteinferior() && medicao < this.S_L1.getLimitesuperior())
						duplicate = insertInCollection(dblocal, newEntry, "sensorluz1", duplicate);
					else
						duplicate = insertInCollection(dblocal, newEntry, "lixo", duplicate);
				}
			} else 	if(object.get("Zona").equals("Z2")) {
						if(object.get("Sensor").equals("H2")) {
							if(medicao > this.S_H2.getLimiteinferior() && medicao < this.S_H2.getLimitesuperior())
								duplicate = insertInCollection(dblocal, newEntry, "sensorhum2", duplicate);
							else
								duplicate = insertInCollection(dblocal, newEntry, "lixo", duplicate);
						}
						if(object.get("Sensor").equals("T2")) {
							if(medicao > this.S_T2.getLimiteinferior() && medicao < this.S_T2.getLimitesuperior())
								duplicate = insertInCollection(dblocal, newEntry, "sensortemp2", duplicate);
							else
								duplicate = insertInCollection(dblocal, newEntry, "lixo", duplicate);
						}
						if(object.get("Sensor").equals("L2")) {
							if(medicao > this.S_L2.getLimiteinferior() && medicao < this.S_L2.getLimitesuperior())
								duplicate = insertInCollection(dblocal, newEntry, "sensorluz2", duplicate);
							else
								duplicate = insertInCollection(dblocal, newEntry, "lixo", duplicate);
						}
				} else {
					duplicate = insertInCollection(dblocal, newEntry, "lixo", duplicate);
				}
		}
		
		System.out.println("Insertion Successfull | entradas: " + count + " duplicados: " + duplicate);
	}
	
	public int insertInCollection(DB dblocal, BasicDBObject newEntry, String collection_name, int duplicate) {
		try {
			DBCollection collec = dblocal.getCollection(collection_name);
			collec.insert(newEntry);
		} catch (DuplicateKeyException e) {
			// TODO: handle exception
			duplicate++;
		}
		return duplicate;
		
	}
	
	public static void main(String[] args) {
		try {
			ConnectMongoCloud mongo = new ConnectMongoCloud();
			
			Connection connectionMySQLCloud = mongo.connectionMysqlCloud();
			
			mongo.runOnCloud(connectionMySQLCloud);
			
			MongoClient mongoClient = mongo.connection();						// para ligar ao mongo dos stores
			MongoClient mongoClient2 = mongo.connectionLocal();
			
			DB dbCloud = mongo.databaseEntry(mongoClient, "sid2022");
			DB dbLocal = mongo.databaseEntry(mongoClient2, "medicoes_valores");
			
			DBCollection medicoes = mongo.collectionOpen(dbCloud, "medicoes");
			
			mongo.insertMongoLocal(medicoes, dbLocal);
			
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}	
}
