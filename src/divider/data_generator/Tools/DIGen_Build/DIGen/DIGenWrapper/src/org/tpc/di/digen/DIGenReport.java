package org.tpc.di.digen;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

public class DIGenReport {
	private String pdgf_version;
	private String digen_version;
	private String scaleFactor;
	private Date startTS;
	private String command;
	private ArrayList<String> row_count;
	private String filename;

	public Date getStartTS() {
		return this.startTS;
	}

	public void setStartTS() {
		this.startTS = Calendar.getInstance().getTime();
	}

	public String getScaleFactor() {
		return this.scaleFactor;
	}

	public void setScaleFactor(String scaleFactor) {
		this.scaleFactor = scaleFactor;
	}

	public String getDigen_version() {
		return this.digen_version;
	}

	public void setDigen_version(String digenVersion) {
		this.digen_version = digenVersion;
	}

	public DIGenReport(String filename) {
		this.filename = filename;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}
	
	public String getCommand() {
		return this.command;
	}

	public String getFilename() {
		return this.filename;
	}

	public String getPdgf_version() {
		return this.pdgf_version;
	}

	public ArrayList<String> getRow_count() {
		return this.row_count;
	}

	public void setCommand(String command) {
		this.command = command;
	}

	public void setPdgf_version(String pdgfVersion) {
		this.pdgf_version = pdgfVersion;
	}

	public void addRow_count(String str) {
		if (this.row_count == null) {
			this.row_count = new ArrayList<String>();
		}
		this.row_count.add(str);
	}

	public void writeReport() {
		try {
			PrintStream report = new PrintStream(this.filename);

			report.println("TPC-DI Data Generation Report");
			report.println("=============================");
			report.println();

			SimpleDateFormat dateformat = new SimpleDateFormat(
					"yyyy-MM-dd'T'HH:mm:ssZ");
			report.println("Start Time: " + dateformat.format(this.startTS));
			report.println("End Time: "
					+ dateformat.format(Calendar.getInstance().getTime()));

			report.println("DIGen Version: " + this.digen_version);
			report.println("Scale Factor: " + this.scaleFactor);

			for (String s : this.row_count) {
				report.println(s);
			}
			report.println();

			if (this.command.isEmpty())
				report.println("Command options used: <default>");
			else
				report.println("Command options used: " + this.command);
			report.println("PDGF Version: " + this.pdgf_version);
			report.println("Java version: " + System.getProperty("java.vendor")
					+ " " + System.getProperty("java.version"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
}