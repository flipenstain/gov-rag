/*
 * Main class for DIGen wrapper
 * 
 * To-do: extract strings into resource bundle
 */


package org.tpc.di.digen;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class DIGen {
	static final String VERSION 			= "1.1.0";
	static final String PDGF_SCHEMA_FILE 	= "tpc-di-schema.xml";
	static final String PDGF_GENERATION_FILE = "tpc-di-generation.xml";
	static final String PDGF_CONFIG_DIR 	= "config";
	static final String PDGF_HOME 			= Configuration.getString("PDGF_HOME");
	static final String PDGF_JAR 			= "pdgf.jar";
	static final String PDGF_OUTDIR 		= "output";
	static final String REPORT_FILE 		= "digen_report.txt";
	static final String PDGF_JVMOPTION 		= "-Xmx1g";
	static final int DEFAULT_SF 			= 5;
	static final int SF_UNIT 				= 1000;
	private boolean DEBUG 					= false;
	private List<String> pdgfArgs;
	private DIGenReport report;

	public DIGen() {
		String reportfile = PDGF_HOME + File.separator + PDGF_OUTDIR
				+ File.separator + REPORT_FILE;
		this.report = new DIGenReport(reportfile);
		this.report.setDigen_version(VERSION);
		this.report.setStartTS();
	}

	public void parseCommandLineOptions(String[] args) {
		StringBuffer command = new StringBuffer();
		for (String s : args) {
			command.append(s).append(" ");
		}
		this.report.setCommand(command.toString());

		CommandLineParser parser = new BasicParser();

		Options options = new Options();
		options.addOption("h", false, "print this message");
		options.addOption("v", false, "print DIGen version");
		OptionBuilder.withArgName("sf");
		OptionBuilder.withDescription("Scale factor.  Default value is 5. (range: 3 -  2147483647");
		OptionBuilder.hasArgs();
		options.addOption(OptionBuilder.create("sf"));
		
		OptionBuilder.withArgName("directory");
		OptionBuilder
				.withDescription("Specify output directory.  Default is output.");
		OptionBuilder.hasArg();
		options.addOption(OptionBuilder.create("o"));
		
		OptionBuilder.withArgName("JVM options");
		OptionBuilder
				.withDescription("JVM options. E.g. -jvm \"-Xms1g -Xmx2g\"");
		OptionBuilder.hasArg();
		options.addOption(OptionBuilder.create("jvm"));
		
		try {
			CommandLine cmdline = parser.parse(options, args);

			if (cmdline.hasOption("h")) {
				System.out.println("DIGen Version: " + VERSION);
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("java -jar DIGen.jar [options]", options);
				System.exit(255);
			} else if (cmdline.hasOption("v")) {
				System.out.println("DIGen Version: " + VERSION);
				System.exit(255);
			}
			if (cmdline.hasOption("d")) {
				DEBUG = true;
			}

			String javabin = System.getProperty("java.home") + File.separator
					+ "bin" + File.separator + "java";
			if (this.pdgfArgs == null) {
				this.pdgfArgs = new ArrayList<String>();
			}
			this.pdgfArgs.add(javabin);

			// Handle parameter: jvm - JVM option passthru
			
			if (cmdline.hasOption("jvm")) {
				this.pdgfArgs.addAll(Arrays.asList(cmdline
						.getOptionValue("jvm").split(" ")));
			} else
				this.pdgfArgs.add(DIGen.PDGF_JVMOPTION);

			this.pdgfArgs.add("-jar");

			this.pdgfArgs.add(PDGF_JAR);

			// -load not being recognized by PDGF therefore commenting this out //BP 02032015
			/*
			this.pdgfArgs.add("-load");
			this.pdgfArgs.add(PDGF_CONFIG_DIR + File.separator + PDGF_SCHEMA_FILE);

			this.pdgfArgs.add("-load");
			this.pdgfArgs.add(PDGF_CONFIG_DIR + File.separator + PDGF_GENERATION_FILE);
			*/

			this.pdgfArgs.add("-closeWhenDone");
			this.pdgfArgs.add("-start");
			
			// Handle parameter: sf - Scale Factor 
			this.pdgfArgs.add("-sf");
			if (cmdline.hasOption("sf")) {
				String sf = cmdline.getOptionValue("sf");
				
				if (Integer.parseInt(sf) >= 3) { 
					this.pdgfArgs.add(Integer.toString(Integer.parseInt(sf) * SF_UNIT));
					this.report.setScaleFactor(sf);
				}
				else {
					// incorrect scale factor
					System.err.println("Incorect Scale factor:" + sf +  " Value must be in range: 3 - 2147483647");
					System.exit(-1);
				}
			}
			else {
				this.pdgfArgs.add(Integer.toString(DEFAULT_SF*SF_UNIT));
				this.report.setScaleFactor(Integer.toString(DEFAULT_SF));
			}

			// Handle parameter: o - output directory
			if (cmdline.hasOption("o")) {
				String outputdir = cmdline.getOptionValue("o");
				File outdir = new File(outputdir);				
				// check if the user director exist.  If not create one
				if (outdir.exists()) {  // 
					if (!outdir.isDirectory()) {  // make sure it is a directory
						System.err.println(outputdir + " is not a directory. DIGen failed.");
						System.exit(-1);
					} else if (!outdir.canWrite())  { // check if the directory is writable
						System.err.println(outputdir + " is not writable. DIGen failed.");
						System.exit(-1);
					}
				} else {
					if (!outdir.mkdirs()) {
						System.err.println("Unable to create "+outputdir+". DIGen failed.");
						System.exit(-1);
					}
				}
				this.pdgfArgs.add("-o");
				this.pdgfArgs.add("'"+outdir.getAbsolutePath()+File.separator+"'");
				System.out.println(outdir.getAbsolutePath());
				String reportfile = 
						outdir.getAbsolutePath() + File.separator + REPORT_FILE;
				this.report.setFilename(reportfile);
			}
			if (this.DEBUG)
				System.out.println(this.pdgfArgs);
		} catch (ParseException e) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("DIGen", options);
			System.exit(255);
		}
	}

	/**
	 * 
	 */
	public void executePDGF() {
		ProcessBuilder builder = new ProcessBuilder(this.pdgfArgs);
		try {
			builder.directory(new File(PDGF_HOME).getAbsoluteFile());

			final Process process = builder.start();

			// Thread to handle stderr from process
			new Thread() {
				@Override
				public void run() {
					BufferedReader in_err = new BufferedReader(
							new InputStreamReader(process.getErrorStream()));
					try {
						String line;
						while ((line = in_err.readLine()) != null) {
							System.err.println(line);
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}.start();
			// Thread to handle stdout from process
			new Thread() {
				@Override
				public void run() {
					BufferedReader in_std = new BufferedReader(
							new InputStreamReader(process.getInputStream()));
					try {
						String line;
						while ((line = in_std.readLine()) != null) {
							if (line.contains("PDGF v"))
								DIGen.this.report.setPdgf_version(line.trim());
							
							else if (line.contains(" TotalRecords "))
								DIGen.this.report.addRow_count(line);
							System.out.println(line);
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}.start();
			
			
			new Thread() {
				@Override
				public void run() {
					OutputStreamWriter out_std = new OutputStreamWriter(process.getOutputStream());
					try {
						//String line;
						BufferedReader bf = new BufferedReader(new InputStreamReader(System.in));
						String s = "";
						while(true) {
							s = bf.readLine();
							//System.out.print(s+"\r\n");
							out_std.write(s+"\r\n"); 
							out_std.flush();
						}
					} catch (IOException e) {
						if (e.getMessage().toLowerCase().contains("closed")){
							// "Stream closed" : the pdgf process finished.
							// do nothing
						}else {							
							e.printStackTrace();
						}
					}
				}
			}.start();
						
			
			
			int retcode = process.waitFor();
			if (retcode == 0) {
				System.out.println("DIGen completed successfully.");
				this.report.writeReport();
				System.exit(0);
			} else {
				System.out
						.println("DIGen failed with non-zero return code from PDGF.");
			}

		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		DIGen digen = new DIGen();
		digen.parseCommandLineOptions(args);
		digen.executePDGF();
	}
}