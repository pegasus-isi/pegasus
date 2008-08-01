package edu.isi.pegasus.planner.catalog.site.classes;

public class VORSVOInfo {
	private String ID;
	private String Name;
	private String Gatekeeper;
	private String Type;
	private String Grid;
	private String Status;
	private String Last_Test_Date;
	public void print(){
		
		System.out.println("ID " + ID);		
		System.out.println("Name " + Name);
		System.out.println("Gatekeeper " + Gatekeeper);
		System.out.println("Type " + Type);
		System.out.println("Grid " + Grid);
		System.out.println("Status " + Status);
		System.out.println("Last_Test_Date " + Last_Test_Date);		
		
	}
	public String getID() {
		return ID;
	}
	public void setID(String id) {
		ID = id;
	}
	public String getName() {
		return Name;
	}
	public void setName(String name) {
		Name = name;
	}
	public String getGatekeeper() {
		return Gatekeeper;
	}
	public void setGatekeeper(String gatekeeper) {
		Gatekeeper = gatekeeper;
	}
	public String getType() {
		return Type;
	}
	public void setType(String type) {
		Type = type;
	}
	public String getGrid() {
		return Grid;
	}
	public void setGrid(String grid) {
		Grid = grid;
	}
	public String getStatus() {
		return Status;
	}
	public void setStatus(String status) {
		Status = status;
	}
	public String getLast_Test_Date() {
		return Last_Test_Date;
	}
	public void setLast_Test_Date(String last_Test_Date) {
		Last_Test_Date = last_Test_Date;
	}
}
