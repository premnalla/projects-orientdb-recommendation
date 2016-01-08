/**
 * 
 */
package com.prem;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser; // don't remove
import org.apache.commons.cli.GnuParser; // don't remove
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;


/**
 * @author Prem
 *
 */
public class FoodReviewLoader {
	private static final Logger log = Logger.getLogger(FoodReviewLoader.class
			.getName());
	private Options m_options = new Options();
	private String[] m_args;

	static final int BATCH_SIZE = 1;
	
	private DbHelper m_h;
	private OrientGraph m_conn;
	
	// private Hashtable<String, String> m_buyerNameMap = new Hashtable<String, String>();
	// private Hashtable<String, String> m_productIdMap = new Hashtable<String, String>();
	
	// private HashMap<String, Integer> m_buyerHashMap = new HashMap<String, Integer>();
	private int m_buyerNextHashId = 1;
	private int getNextBuyerHashId() {
		return (m_buyerNextHashId++);
	}
	
	// private HashMap<String, Integer> m_productHashMap = new HashMap<String, Integer>();
	private int m_productNextHashId = 1;
	private int getNextProductHashId() {
		return (m_productNextHashId++);
	}

	private int m_numRecords = 0;
	
	/**
	 * 
	 */
	public FoodReviewLoader(String[] args) {
		m_args = args;
		m_options.addOption("h", "help", false, "show help.");
		m_options.addOption("l", "in", true,	"load data file");
		m_options.addOption("c", "in", true,	"db connection string");
		m_options.addOption("t", "in", false,	"test data");
		m_options.addOption("d", "in", false,	"generate ratings csv in ./data directory");
		// m_conn = m_h.getConn();
	}

	private void help() {
		// This prints out some help
		HelpFormatter formater = new HelpFormatter();

		formater.printHelp("Main", m_options);

		System.exit(0);
	}

	private boolean m_loadData = false;
	private String m_inFile;
	private boolean m_testData = false;
	private boolean m_generateRatings = false;
	private String m_dbConnStr;
	
	private boolean parseArgs() {
		CommandLineParser parser;
		
		// parser = new DefaultParser(); // don't use this. get run-time error !!!!!
		parser = new GnuParser();
		
		CommandLine cmd = null;
		try {
			cmd = parser.parse(m_options, m_args);

			if (cmd.hasOption("h"))
				help();

			if (cmd.hasOption("c")) {
				log.log(Level.INFO, "Using cli argument -c=" + cmd.getOptionValue("c"));
				m_dbConnStr = cmd.getOptionValue("c");
				m_h = new DbHelper(m_dbConnStr);
				m_conn = m_h.getConn();
				// Whatever you want to do with the setting goes here
			} else {
				log.log(Level.SEVERE, "Missing -c option");
				help();
			}

			if (cmd.hasOption("l")) {
				log.log(Level.INFO, "Using cli argument -l=" + cmd.getOptionValue("l"));
				m_inFile = cmd.getOptionValue("l");
				m_loadData = true;
				// Whatever you want to do with the setting goes here
			} else {
				// log.log(Level.SEVERE, "Missing -i option");
				// help();
			}

			if (cmd.hasOption("t")) {
				log.log(Level.INFO,	"Using cli argument -t");
				m_testData = true;
				// Whatever you want to do with the setting goes here
			} else {
				// log.log(Level.SEVERE, "Missing -i option");
				// help();
			}

			if (cmd.hasOption("d")) {
				log.log(Level.INFO,	"Using cli argument -d");
				m_generateRatings = true;
				// Whatever you want to do with the setting goes here
			} else {
				// log.log(Level.SEVERE, "Missing -i option");
				// help();
			}

		} catch (ParseException e) {
			log.log(Level.SEVERE, "Failed to parse comand line properties", e);
			help();
		}

		return (true);
	}

	private void test() {
		
	}
	
	private void loadData() throws Exception {
		// Stack<Record> stk = new Stack<FoodReviewLoader.Record>();
		Vector<Record> v = new Vector<FoodReviewLoader.Record>();
		// DbHelper h = new DbHelper();
		OrientGraph conn = m_conn;
		
		BufferedReader br = new BufferedReader(new FileReader(new File(m_inFile)));
		String ln = br.readLine();
		while (ln != null) {
			while (ln != null && ln.length() == 0) {
				ln = br.readLine();
			}
			if (ln == null) {
				break;
			}
			
			Record r = new Record(ln);
			ln = br.readLine();
			
			while (ln != null && ln.length() != 0) {
				r.addLine(ln);
				ln = br.readLine();
			}
			r.close();
			m_numRecords++;
			v.add(r);
			if (v.size() == BATCH_SIZE) {
				// bulk process
				bulkInsert(conn, v);
			}
			if (ln == null) {
				break;
			}
			
		}
		
		bulkInsert(conn, v);
		
		br.close();
	}

	private void bulkInsert(OrientGraph conn, Vector<Record> v) {
		while (!v.isEmpty()) {
			Record r = v.remove(0);
			m_h.createOrFindProduct(conn, r);
			m_h.createOrFindBuyer(conn, r);
			// m_h.createBoughtEdge(conn, r);
			m_h.createReviewedEdge(conn, r);
		}
		conn.commit();
		// m_buyerNameMap.clear();
		// m_productIdMap.clear();
	}
	
	private void testDbConnection() {
		DbHelper h = new DbHelper("remote:192.168.123.130/test");
		h.getConn();
		System.out.println("got connection");
	}
	
	private void testInsertVertex() {
		DbHelper h = new DbHelper("remote:192.168.123.130/test");
		OrientGraph conn = h.getConn();
		try {
			Vertex vBuyer = conn.getVertexByKey("Buyer.name", "buyer1");
			if (vBuyer == null) {
				vBuyer = conn.addVertex("class:Buyer");
				vBuyer.setProperty("name", "buyer1");
				vBuyer.setProperty("profileName", "one");
			} else {
				System.out.println("found vertex:" + vBuyer.toString());
			}
			Vertex vProduct = conn.getVertexByKey("Product.name", "prod1");
			if (vProduct == null) {
				vProduct = conn.addVertex("class:Product");
				vProduct.setProperty("name", "prod1");
			} else {
				System.out.println("found vertex:" + vProduct.toString());				
			}
			Edge eBought = conn.addEdge("class:bought", vBuyer, vProduct, "bought");
			conn.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void dumpRatings() throws Exception {
		BufferedWriter br = new BufferedWriter(new FileWriter(new File("data/ratings.csv")));
		
		int batch = 0;
		for (Vertex v : (Iterable<Vertex>) m_conn.command(
						new OCommandSQL("SELECT FROM Buyer")).execute()) {
			// System.out.println("Buyer: " + v.getProperty("name") + ", ID: " + v.getId());
			
			int bHash = 0;
			for (Vertex v2 : (Iterable<Vertex>) m_conn.command(new OCommandSQL("SELECT EXPAND(in()) from ?")).execute(v.getId())) {
				// System.out.println("hash: " + v2.getProperty("hashId"));
				bHash = v2.getProperty("hashId");
			}

			BigDecimal score = null;
			for (Edge e1 : (Iterable<Edge>) m_conn.command(new OCommandSQL("SELECT EXPAND(outE()) from ?")).execute(v.getId())) {
				// System.out.println("score: " + e1.getProperty("score"));
				score = e1.getProperty("score");
			}

			int pHash = 0;
			for (Vertex v3 : (Iterable<Vertex>) m_conn.command(new OCommandSQL("SELECT EXPAND(out().in('hashBelongsToProduct')) from ?")).execute(v.getId())) {
				// System.out.println("Product-hash: " + v3.getProperty("hashId") + "; " + v3);
				pHash = v3.getProperty("hashId");
			}

			br.write(bHash + "," + pHash + "," + score + "\n");
			batch++;
			if (batch == 1000) {
				br.flush();
				batch = 0;
			}
			
			// @formatter: off
			/*
			OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>("select in() from ?");
			List<ODocument> result = m_conn.command(query).execute(v.getId());
			for (ODocument d : result) {
				System.out.println("Document: " + d.field("hashId"));
			}
			*/
			// @formatter: on

			// for (Vertex v2 : (Iterable<Vertex>) m_conn.command(new OCommandSQL()))
		}
		br.close();
	}
	
	private void run() throws Exception {
		if (m_loadData) {
			loadData();
		} else if (m_generateRatings) {
			dumpRatings();
		} else if (m_testData) {
			test();
		} else {
			System.out.println("Nothing to do...");
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		FoodReviewLoader f = new FoodReviewLoader(args);
		if (!f.parseArgs()) {
			System.exit(-1);
		}
		log.info("Starting multi-purpose utility...");
		f.run();
		// f.testQuery();
		// f.testDbConnection();
		// f.testInsertVertex();
		log.info("Exiting multi-purpose utility...");
		// log.info("Num reviews processed:" + f.m_numRecords);

	}

	public class Record {
		private String m_strs[] = new String[10];
		private int m_idx;
		
		public String m_productId;
		public String m_buyerId;
		public String m_time;
		public String m_profileName;
		public String m_score;
		public String m_summary;
		public String m_text;
		public String m_helpful;
		public String m_unhelpful;
		
		// hash for product and user
		public int m_productIdHash;
		public int m_buyerIdHash;
		
		public Vertex m_vBuyer;
		public Vertex m_vProduct;
		
		public Record(String ln) {
			m_idx = 0;
			m_strs[m_idx++] = ln;
		}
		public void addLine(String ln) {
			m_strs[m_idx++] = ln;			
		}
		
		public void open() {
			m_idx = 0;
		}
		
		public void close() {
			String s;
			for (int i=0; i<m_idx; i++) {
				if (m_strs[i].startsWith("product/productId: ")) {
					m_productId = m_strs[i].substring("product/productId: ".length());
					// m_productIdHash = m_productId.hashCode();
					// System.out.println(s);
					
				} else if (m_strs[i].startsWith("review/userId: ")) {
					m_buyerId = m_strs[i].substring("review/userId: ".length());
					// m_buyerIdHash = m_buyerId.hashCode();
					// System.out.println(s);
					
				} else if (m_strs[i].startsWith("review/profileName: ")) {
					m_profileName = m_strs[i].substring("review/profileName: ".length());
					// System.out.println(s);
					
				} else if (m_strs[i].startsWith("review/helpfulness: ")) {
					s = m_strs[i].substring("review/helpfulness: ".length());
					String[] toks = s.split("[/]");
					try {
						m_helpful = toks[0];
						m_unhelpful = toks[1];
					} catch (Exception e) {
						
					}
					
				} else if (m_strs[i].startsWith("review/score: ")) {
					m_score = m_strs[i].substring("review/score: ".length());
					// System.out.println(s);
					
				} else if (m_strs[i].startsWith("review/time: ")) {
					m_time = m_strs[i].substring("review/time: ".length());
					// System.out.println(s);
					
				} else if (m_strs[i].startsWith("review/summary: ")) {
					m_summary = m_strs[i].substring("review/summary: ".length());
					// System.out.println(s);
					
				} else if (m_strs[i].startsWith("review/text: ")) {
					m_text = m_strs[i].substring("review/text: ".length());
										
				} else {
					System.out.println(m_strs[i]);
				}
					
				// log.info(m_strs[i]);
			}
		}
	}
	
	public class DbHelper {
				
		private OrientGraphFactory m_factory;
	
		// @formatter:off
		/*
		private DbHelper() {
			
		}
		*/
		// @formatter:on
		
		private DbHelper(String connStr) {
			m_factory = new OrientGraphFactory(connStr).setupPool(1,10);
		}
		
		private OrientGraph getConn() {
			return m_factory.getTx();
		}
		
		private void createOrFindBuyer(OrientGraph conn, Record r) {
			try {
				Vertex vBuyer = conn.getVertexByKey("Buyer.name", r.m_buyerId);
				if (vBuyer == null) {
					// if (!m_buyerNameMap.containsKey(r.m_buyerId)) {
						vBuyer = conn.addVertex("class:Buyer");
						vBuyer.setProperty("name", r.m_buyerId);
						vBuyer.setProperty("profileName", r.m_profileName);
						// m_buyerNameMap.put(r.m_buyerId, r.m_buyerId);
						Vertex vBuyerHash = conn.addVertex("class:BuyerHash");
						// vBuyerHash.setProperty("hashId", r.m_buyerIdHash);
						vBuyerHash.setProperty("hashId", FoodReviewLoader.this.getNextBuyerHashId());
						Edge eHashBelongsToBuyer = conn.addEdge("class:hashBelongsToBuyer", vBuyerHash, vBuyer, "hashBelongsToBuyer");
					// }
				}
				r.m_vBuyer = vBuyer;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		private void createOrFindProduct(OrientGraph conn, Record r) {
			try {
				Vertex vProd = conn.getVertexByKey("Product.name", r.m_productId);
				if (vProd == null) {
					// if (!m_productIdMap.containsKey(r.m_productId)) {
						vProd = conn.addVertex("class:Product");
						vProd.setProperty("name", r.m_productId);
						// m_productIdMap.put(r.m_productId, r.m_productId);
						Vertex vProductHash = conn.addVertex("class:ProductHash");
						// vProductHash.setProperty("hashId", r.m_productIdHash);
						vProductHash.setProperty("hashId", FoodReviewLoader.this.getNextProductHashId());
						Edge eHashBelongsToProduct = conn.addEdge("class:hashBelongsToProduct", vProductHash, vProd, "hashBelongsToProduct");

					// }
				}
				r.m_vProduct = vProd;
			} catch (Exception e) {
				e.printStackTrace();				
			}			
		}

		//@formatter:off
		/*
		public void createBoughtEdge(OrientGraph conn, Record r) {
			try {
				Edge eBought = conn.addEdge("class:bought", r.m_vBuyer, r.m_vProduct, "bought");
			} catch (Exception e) {
				e.printStackTrace();				
			}			
		}
		*/
		//@formatter:on

		private void createReviewedEdge(OrientGraph conn, Record r) {
			try {
				Edge eReviewed = conn.addEdge("class:reviewed", r.m_vBuyer, r.m_vProduct, "reviewed");
				try {
					eReviewed.setProperty("time", new Long(Long.parseLong(r.m_time)*1000));
				} catch (Exception e2) {
				}
				try {
					eReviewed.setProperty("helpful", Integer.parseInt(r.m_helpful));
					// System.out.println(r.m_helpful);
					// eReviewed.setProperty("helpful", r.m_helpful);
				} catch (Exception e2) {
				}
				try {
					eReviewed.setProperty("unhelpful", Integer.parseInt(r.m_unhelpful));
					// eReviewed.setProperty("unhelpful", r.m_unhelpful);
					// System.out.println(r.m_unhelpful);
				} catch (Exception e2) {
				}
				eReviewed.setProperty("score", r.m_score);
				eReviewed.setProperty("summary", r.m_summary);
				eReviewed.setProperty("text", r.m_text);
			} catch (Exception e) {
				e.printStackTrace();				
			}			
		}

	}
}
