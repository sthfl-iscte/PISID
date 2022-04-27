package cloudToLocal_Mongo;

public class Sensor {

	private int idsensor;
	private String tipo;
	private double limiteinferior;
	private double limitesuperior;
	private int idzona;
	
	public Sensor(int idsensor, String tipo, double limiteinferior, double limitesuperior, int idzona) {
		this.idsensor = idsensor;
		this.tipo = tipo;
		this.limiteinferior = limiteinferior;
		this.limitesuperior = limitesuperior;
		this.idzona = idzona;
	}

	public int getIdsensor() {
		return idsensor;
	}

	public String getTipo() {
		return tipo;
	}

	public double getLimiteinferior() {
		return limiteinferior;
	}

	public double getLimitesuperior() {
		return limitesuperior;
	}

	public int getIdzona() {
		return idzona;
	}

	@Override
	public String toString() {
		return "Sensor [idsensor=" + idsensor + ", tipo=" + tipo + ", limiteinferior=" + limiteinferior
				+ ", limitesuperior=" + limitesuperior + ", idzona=" + idzona + "]";
	}
	
	
}
