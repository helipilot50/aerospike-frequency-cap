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
import java.util.ArrayList;
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
	private static final String COMPOUND_KEY_SET = "freq_compound_key";
	private static final String BIN_SET = "freq_date_in_bin";
	private static final int TOTAL_DATA = 5000;
	private static final int USER_SAMPLE = 100;
	private static final long RANDOM_SEED = 3690;
	private AerospikeClient client;
	private String seedHost;
	private int port;
	private String namespace;
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
	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");

	private static Logger log = Logger.getLogger(FrequencyCap.class);
	public FrequencyCap(String host, int port, String namespace, String set) throws AerospikeException {
		this.client = new AerospikeClient(host, port);
		this.seedHost = host;
		this.port = port;
		this.namespace = namespace;
	}
	public FrequencyCap(AerospikeClient client, String namespace, String set) throws AerospikeException {
		this.client = client;
		this.namespace = namespace;
	}
	public static void main(String[] args) throws AerospikeException {
		try {
			Options options = new Options();
			options.addOption("h", "host", true, "Server hostname (default: 172.28.128.6)");
			options.addOption("p", "port", true, "Server port (default: 3000)");
			options.addOption("n", "namespace", true, "Namespace (default: test)");
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
			if (cl.hasOption("l")){
				as.generateDataDateInKey();
				as.generateDataDateInBin();
			} else {
				as.simulateWorkDateInKey();
				as.simulateWorkDateInBin();
			}
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
	 * over the past 10 days, with the Date as part of the key.
	 * @throws Exception
	 */
	public void generateDataDateInKey() throws Exception {
		log.info("Generating data using Date in Key");
		Random campaignR = new Random(RANDOM_SEED);
		Random dayR = new Random(RANDOM_SEED);
		Random viewsR = new Random(RANDOM_SEED);
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
					Key key = new Key(this.namespace, COMPOUND_KEY_SET, keyString);
					this.client.put(null, key, new Bin("FREQ", viewsR.nextInt(5)));
				}
			}
			if (i % 500 == 0){
				log.info(String.format("%d date in Key", i));
			}
		}
		log.info(String.format("loaded %d date in key", TOTAL_DATA));
	}
	/**
	 * This method produces a random number of campaigns and views for each user
	 * over the past 10 days, with the Date as a bin
	 * @throws Exception
	 */
	public void generateDataDateInBin() throws Exception {
		log.info("Generating data using Date in Bins");
		Random campaignR = new Random(RANDOM_SEED);
		Random dayR = new Random(RANDOM_SEED);
		Random viewsR = new Random(RANDOM_SEED);
		this.client.writePolicyDefault.expiration = 60 * 60 * 24 * 10; // 10 days


		for (int i = 0; i < TOTAL_DATA; i++){
			String userID = "user-id-"+i;
			int campaignTotal = campaignR.nextInt(campaigh.length);
			for (int c = 0; c < campaignTotal; c++){
				String campaignID = campaigh[c];
				/*
				 * Generate user activity in the past 10 days
				 */
				List<Bin> binList = new ArrayList<Bin>();
				
				int noOfDays = dayR.nextInt(10);
				for (int y = noOfDays; y >= 0; y--){
					int day = dayR.nextInt(10);
					Calendar calendar = Calendar.getInstance(); 
					calendar.add(Calendar.DATE, -day);
					String binName =  dateFormat.format(calendar.getTime());
					binList.add(new Bin(binName, viewsR.nextInt(5)));
				}
				String keyString = userID+":"+campaignID;
				Key key = new Key(this.namespace, BIN_SET, keyString);
				this.client.put(null, key, binList.toArray(new Bin[0]));
			}
			if (i % 500 == 0){
				log.info(String.format("%d date in Bins", i));
			}
		}
		log.info(String.format("loaded %d date in Bins", TOTAL_DATA));
	}
	/**
	 * For USER_SAMPLE print the frequency for each campaign using Date in key
	 * @throws Exception
	 */
	public void simulateWorkDateInKey() throws Exception {
		log.info("***** Count in the last 10 days by user and campaign, Date in Key");
		for (int i = 0; i < USER_SAMPLE; i++){
			String userID = "user-id-"+i;
			long start = System.currentTimeMillis();
			int count = 0;
			for (int camp = 0; camp < campaigh.length; camp++){
				String campaignID = campaigh[camp];
				Calendar calendar = Calendar.getInstance(); 
				Key[] keys = new Key[10];
				for (int day = 0; day < 10; day++){
					calendar.add(Calendar.DATE, -day);
					String dateString =  dateFormat.format(calendar.getTime());
					String keyString = userID+":"+campaignID+":"+dateString;
					keys[day] = new Key(this.namespace, COMPOUND_KEY_SET, keyString);
				}
				/*
				 * get all the records, note that some may be missing
				 */
				Record[] records = this.client.get(null, keys);
				for (Record rec : records){
					if (rec != null){
						Long value = rec.getLong("FREQ");
						if (value != null && value > 0){
							count ++;
						}
					}
				}
			}
			long stop = System.currentTimeMillis();
			log.info(String.format("User:%s, Campaigns:%d, Time %d milliseconds", userID, count, stop-start));
		}
	}
	/**
	 * For USER_SAMPLE print the frequency for each campaign using Date in Bin
	 * @throws Exception
	 */
	public void simulateWorkDateInBin() throws Exception {
		log.info("***** Count in the last 10 days by user and campaign, Date in Bin");
		for (int i = 0; i < USER_SAMPLE; i++){
			String userID = "user-id-"+i;
			long start = System.currentTimeMillis();
			int count = 0;
			for (int camp = 0; camp < campaigh.length; camp++){
				String campaignID = campaigh[camp];
				String keyString = userID+":"+campaignID;
				List<String> binList = new ArrayList<String>();
				Key key = new Key(this.namespace, BIN_SET, keyString);
				Calendar calendar = Calendar.getInstance(); 
				for (int day = 0; day < 10; day++){
					calendar.add(Calendar.DATE, -day);
					String binName =  dateFormat.format(calendar.getTime());
					binList.add(binName);
				}
				/*
				 * get all the records, note that some may be missing
				 */
				Record record = this.client.get(null, key, binList.toArray(new String[0]));
				if (record != null){
					for (String day : binList){
						Long value = record.getLong(day);
						if (value != null && value > 0){
							count ++;
						}
					}
				}
			}
			long stop = System.currentTimeMillis();
			log.info(String.format("User:%s, Campaigns:%d, Time %d milliseconds", userID, count, stop-start));
		}
		
	}

}