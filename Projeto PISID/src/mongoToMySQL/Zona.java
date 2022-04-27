package mongoToMySQL;

public class Zona {

	private int idzona;
	private double temperatura;
	private double humidade;
	private double luz;
	
	public Zona(int idzona, double temperatura, double humidade, double luz) {
		this.idzona = idzona;
		this.temperatura = temperatura;
		this.humidade = humidade;
		this.luz = luz;
	}

	public int getIdzona() {
		return idzona;
	}

	public double getTemperatura() {
		return temperatura;
	}

	public double getHumidade() {
		return humidade;
	}

	public double getLuz() {
		return luz;
	}

	@Override
	public String toString() {
		return "Zona [idzona=" + idzona + ", temperatura=" + temperatura + ", humidade=" + humidade + ", luz=" + luz
				+ "]";
	}
	
	
}
