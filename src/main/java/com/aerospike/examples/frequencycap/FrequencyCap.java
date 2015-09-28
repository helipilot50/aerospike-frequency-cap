/* 
 * Copyright 2012-2015 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.examples.frequencycap;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.Logger;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;


/**
@author Peter Milne
 */
public class FrequencyCap {
	private static final int TOTAL_DATA = 5000;
	private static final int USER_SAMPLE = 100;
	private AerospikeClient client;
	private String seedHost;
	private int port;
	private String namespace;
	private String set;
	private WritePolicy writePolicy;
	private Policy policy;
	String[] campaigh = new String[] {
			"Shoes",
			"Cats",
			"Dogs",
			"Helicopters",
			"Computers",
			"Dresses",
			"Cars",
			"Travel",
			"RealEstate",
			"Suits",
			"Wine",
			"Food",
			"Hats",
			"Ski",
			"Boats",
			"Planes"
	};
	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

	private static Logger log = Logger.getLogger(FrequencyCap.class);
	public FrequencyCap(String host, int port, String namespace, String set) throws AerospikeException {
		this.client = new AerospikeClient(host, port);
		this.seedHost = host;
		this.port = port;
		this.namespace = namespace;
		this.set = set;
		this.writePolicy = new WritePolicy();
		this.policy = new Policy();
	}
	public FrequencyCap(AerospikeClient client, String namespace, String set) throws AerospikeException {
		this.client = client;
		this.namespace = namespace;
		this.set = set;
		this.writePolicy = new WritePolicy();
		this.policy = new Policy();
	}
	public static void main(String[] args) throws AerospikeException {
		try {
			Options options = new Options();
			options.addOption("h", "host", true, "Server hostname (default: 172.28.128.6)");
			options.addOption("p", "port", true, "Server port (default: 3000)");
			options.addOption("n", "namespace", true, "Namespace (default: test)");
			options.addOption("s", "set", true, "Set (default: demo)");
			options.addOption("u", "usage", false, "Print usage.");

			options.addOption("l", "load", false, "Load data.");

			CommandLineParser parser = new PosixParser();
			CommandLine cl = parser.parse(options, args, false);


			String host = cl.getOptionValue("h", "172.28.128.6");
			String portString = cl.getOptionValue("p", "3000");
			int port = Integer.parseInt(portString);
			String namespace = cl.getOptionValue("n", "test");
			String set = cl.getOptionValue("s", "demo");
			log.debug("Host: " + host);
			log.debug("Port: " + port);
			log.debug("Namespace: " + namespace);
			log.debug("Set: " + set);

			@SuppressWarnings("unchecked")
			List<String> cmds = cl.getArgList();
			if (cmds.size() == 0 && cl.hasOption("u")) {
				logUsage(options);
				return;
			}

			FrequencyCap as = new FrequencyCap(host, port, namespace, set);
			if (cl.hasOption("l"))
				as.generateData();
			else
				as.simulateWork();

		} catch (Exception e) {
			log.error("Critical error", e);
		}
	}
	/**
	 * Write usage to console.
	 */
	private static void logUsage(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		String syntax = FrequencyCap.class.getName() + " [<options>]";
		formatter.printHelp(pw, 100, syntax, "options:", options, 0, 2, null);
		log.info(sw.toString());
	}
	/**
	 * This method produces a random number of campaigns and views for each user
	 * over the past 10 days
	 * @throws Exception
	 */
	public void generateData() throws Exception {
		Random campaignR = new Random();
		Random dayR = new Random();
		Random viewsR = new Random();
		this.client.writePolicyDefault.expiration = 60 * 60 * 24 * 10; // 10 days


		for (int i = 0; i < TOTAL_DATA; i++){
			String userID = "user-id-"+i;
			int campaignTotal = campaignR.nextInt(campaigh.length);
			for (int c = 0; c < campaignTotal; c++){
				String campaignID = campaigh[c];
				/*
				 * Generate user activity in the past 10 days
				 */
				int noOfDays = dayR.nextInt(10);
				for (int y = noOfDays; y >= 0; y--){
					int day = dayR.nextInt(10);
					Calendar calendar = Calendar.getInstance(); 
					calendar.add(Calendar.DATE, -day);
					String dateString =  dateFormat.format(calendar.getTime());
					String keyString = userID+":"+campaignID+":"+dateString;
					Key key = new Key("test", "demo", keyString);
					this.client.put(null, key, new Bin("FREQ", viewsR.nextInt(5)));
				}
			}
			if (i % 500 == 0){
				log.info(String.format("%d processed", i));
			}
		}
		log.info(String.format("loaded %d", TOTAL_DATA));
	}
	/**
	 * For USER_SAMPLE print the frequency for each campaign
	 * @throws Exception
	 */
	public void simulateWork() throws Exception {
		log.info("Count in the last 10 days by user and campaign");
		for (int i = 0; i < USER_SAMPLE; i++){
			String userID = "user-id-"+i;
			log.info(String.format("%s", userID));
			for (int camp = 0; camp < campaigh.length; camp++){
				String campaignID = campaigh[camp];
				Calendar calendar = Calendar.getInstance(); 
				Key[] keys = new Key[10];
				for (int day = 0; day < 10; day++){
					calendar.add(Calendar.DATE, -day);
					String dateString =  dateFormat.format(calendar.getTime());
					String keyString = userID+":"+campaignID+":"+dateString;
					keys[day] = new Key("test", "demo", keyString);
				}
				/*
				 * get all the records, note that some may be missing
				 */
				Record[] records = this.client.get(null, keys);
				int count = 0;
				for (Record rec : records){
					if (rec != null){
						count += rec.getInt("FREQ");
					}

				}
				if (count > 0)
					log.info(String.format("\t%s %d", campaignID, count));
			}
		}
	}

}