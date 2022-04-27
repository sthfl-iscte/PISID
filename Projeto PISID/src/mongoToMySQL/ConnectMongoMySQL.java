package mongoToMySQL;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

import mongoToMySQL.Sensor;
import mongoToMySQL.Zona;

public class ConnectMongoMySQL {

	private ArrayList<Zona> zonas = new ArrayList<Zona>();
	private ArrayList<Sensor> sensores = new ArrayList<Sensor>();
	
	public Connection connectionMysql() throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {			// Ligar � nossa base de dados MySQL
//		Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/aluno_g15_final?" + "user=root&password=");
		Connection conn = DriverManager.getConnection("jdbc:mysql://192.168.1.177/aluno_g15?" + "user=root&password=");
		System.out.println("Connected to local MySQL!");
		return conn;
	}
	
	public Connection connectionMysqlCloud() throws ClassNotFoundException, SQLException {
		Connection conn = DriverManager.getConnection("jdbc:mysql://194.210.86.10/sid2022?" + "user=aluno&password=aluno");
		System.out.println("Connected to cloud MySQL!");
		return conn;
	}

	
	public void runOnCloud(Connection connection) throws SQLException { // para ir buscar dados ao MySQL Cloud
		String query = "SELECT * FROM zona";
		Statement stmt = connection.createStatement();
		ResultSet rs = stmt.executeQuery(query);
		while (rs.next()) {
			int idzona = rs.getInt("idzona");
			double temperatura = rs.getDouble("temperatura");
			double humidade = rs.getDouble("humidade");
			double luz = rs.getDouble("luz");
			Zona z = new Zona(idzona, temperatura, humidade, luz);
			System.out.println(z.toString());
			zonas.add(z);
		}

		String query2 = "SELECT * FROM sensor";
		Statement stmt2 = connection.createStatement();
		ResultSet rs2 = stmt2.executeQuery(query2);
		while (rs2.next()) {
			int idsensor = rs2.getInt("idsensor");
			String tipo = rs2.getString("tipo");
			double limiteinferior = rs2.getDouble("limiteinferior");
			double limitesuperior = rs2.getDouble("limitesuperior");
			int idzona = rs2.getInt("idzona");
			Sensor s = new Sensor(idsensor, tipo, limiteinferior, limitesuperior, idzona);
			System.out.println(s.toString());
			sensores.add(s);
		}

	}
	
	public void runInto(DBCollection collection, String sensor, Connection conn) throws SQLException, ParseException {
		DBCursor cursor = collection.find().skip((int) collection.count() - x); // ir buscar as x ultimas da cole��o
		Timestamp timesystem = new Timestamp(System.currentTimeMillis());
		timesystem.setSeconds(timesystem.getSeconds() - x);
		ArrayList<BasicDBObject> medicoesMedia = new ArrayList<BasicDBObject>();
		BasicDBObject ultimaMedicao = null;

		while (cursor.hasNext()) { // colocar na tabela medi��o, as medi��es mais recentes
			BasicDBObject object = (BasicDBObject) cursor.next();
//			System.out.println(object.get("Data").toString());
//			String data = object.get("Data").toString();
//			String hora = object.get("Hora").toString();
//			String stringData = data + " " + hora;
//
//			Timestamp timestampMedicao = Timestamp.valueOf(stringData);
			if (object.get("Sent").toString().equals("false")) {
				ultimaMedicao = object;
				medicoesMedia.add(object);

				String query = "insert into medicao(Zona, Sensor, Hora, Leitura) values(?, ?, ?, ?)";

				PreparedStatement preparedStmt = conn.prepareStatement(query);

				preparedStmt.setString(1, object.get("Zona").toString());
				
				preparedStmt.setString(2, object.get("Sensor").toString());
				
				String dataHora = object.get("Data").toString().concat(" ").concat(object.get("Hora").toString());
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
				Date parsedDate = dateFormat.parse(dataHora);
				Timestamp timestamp = new java.sql.Timestamp(parsedDate.getTime());
				preparedStmt.setTimestamp(3, timestamp);

				double medicao = Double.parseDouble(object.get("Medicao").toString());
				preparedStmt.setDouble(4, medicao);

				// Atualizar o Sync no ReplicaSet
				BasicDBObject novo = new BasicDBObject();
				novo.put("$set", new BasicDBObject().append("Sync", true));
				collection.update(object, novo);

				preparedStmt.execute();
				System.out.println("Medicao adicionada ao MySQL");

			}
		}
		if(ultimaMedicao!=null) {
			String data = ultimaMedicao.get("Data").toString();
			String hora = ultimaMedicao.get("Hora").toString();
			String stringData = data + " " + hora;

			Timestamp timestampMedicao = Timestamp.valueOf(stringData);

			ArrayList<ParametroCultura> listaParametros = new ArrayList<ParametroCultura>();

			String[] split = ultimaMedicao.get("Zona").toString().split("Z");
			String query = "SELECT * FROM parametrocultura WHERE IDCultura=ANY(SELECT IDCultura FROM cultura WHERE IDZona=" + split[1] + ")"; //quero os parametros cultura de todas as culturas da zona afetada pela ultima medi��o
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while (rs.next()) {
				int idCultura = rs.getInt("IDCultura");
				int maxH = rs.getInt("MaxH");
				int maxL  = rs.getInt("MaxL");
				int maxT  = rs.getInt("MaxT");
				int minH  = rs.getInt("MinH");
				int minL  = rs.getInt("MinL");
				int minT  = rs.getInt("MinT");
				ParametroCultura p = new ParametroCultura(idCultura, maxH, maxL, maxT, minH, minL, minT);
				String query2 = "SELECT * FROM percentagensalertas WHERE IDUtilizador=ANY(SELECT IDUtilizador FROM cultura WHERE IDCultura=" + p.getIdCultura() + ")"; //ir buscar as percentagens para alertas, associadas ao utilizador que � dono das culturas cujos parametros foram colecionados acima
				Statement stmt2 = conn.createStatement();
				ResultSet rs2 = stmt2.executeQuery(query2);
				if(rs2.next()) {
					p.setIdUtilizador(rs2.getInt("IDUtilizador"));
					p.setPercentagemAtencao(rs2.getInt("PercentagemAtencao"));
					p.setPercentagemPerigo(rs2.getInt("PercentagemPerigo"));
					p.setPercentagemCritico(rs2.getInt("PercentagemCritico"));
					p.setTempoAlerta(rs2.getInt("TempoAlerta"));
				}
				listaParametros.add(p);
			}
			if(listaParametros.size()!=0) {
				for(ParametroCultura pc : listaParametros) {

					if(pc.getPercentagemAtencao()==0 && pc.getPercentagemCritico()==0 && pc.getPercentagemPerigo()==0 && pc.getTempoAlerta()==0) {
						String query3 = "SELECT * FROM cultura WHERE IDCultura=" + pc.getIdCultura();
						Statement stmt3 = conn.createStatement();
						ResultSet rs3 = stmt3.executeQuery(query3);
						rs3.next();
						pc.setIdUtilizador(rs3.getInt("IDUtilizador"));
						pc.setPercentagemAtencao(percentagemAtencao);
						pc.setPercentagemPerigo(percentagemPerigo);
						pc.setPercentagemCritico(percentagemCritico);
						pc.setTempoAlerta(tempoAlerta);
					}

					String tipoSensor = String.valueOf(sensor.charAt(0));
					String zonaSensor = String.valueOf(sensor.charAt(1));
					int zonaS = Integer.parseInt(zonaSensor);
					double valorOtimo=0;
					int valorMin=0;
					int valorMax=0;
					for(Zona z : zonas) {
						if(z.getIdzona()==zonaS) {
							Zona zona = z;
							if(tipoSensor.equals("T")) {
								valorOtimo = zona.getTemperatura();
								valorMin=pc.getMinT();
								valorMax=pc.getMaxT();
							}
							if(tipoSensor.equals("H")) {
								valorOtimo = zona.getHumidade();
								valorMin=pc.getMinH();
								valorMax=pc.getMaxH();
							}
							if(tipoSensor.equals("L")) {
								valorOtimo = zona.getLuz();
								valorMin=pc.getMinL();
								valorMax=pc.getMaxL();
							}
						}
					}

					double valorUltimaMedicao = Double.parseDouble(ultimaMedicao.get("Medicao").toString());
					int resultado=0;
					if(valorUltimaMedicao<valorOtimo) {
						resultado = (int)(((valorOtimo-valorUltimaMedicao)/(valorOtimo-valorMin))*100);
					}
					else {
						resultado = (int)((1-((valorMax-valorUltimaMedicao)/(valorMax-valorOtimo)))*100);
					}

					if(resultado>=pc.getPercentagemAtencao() && resultado<pc.getPercentagemPerigo()) {
						int tipoAlerta = 1;
						verificarNecessidadeAlerta(conn, medicoesMedia, valorUltimaMedicao, pc, tipoAlerta, sensor, timestampMedicao);
					}
					else if(resultado>=pc.getPercentagemPerigo() && resultado<pc.getPercentagemCritico()) {
						int tipoAlerta = 2;
						verificarNecessidadeAlerta(conn, medicoesMedia, valorUltimaMedicao, pc, tipoAlerta, sensor, timestampMedicao);
					}
					else if(resultado>pc.getPercentagemCritico()) {
						int tipoAlerta = 3;
						verificarNecessidadeAlerta(conn, medicoesMedia, valorUltimaMedicao, pc, tipoAlerta, sensor, timestampMedicao);
					}
				}
			}
		}
	}

	
}
