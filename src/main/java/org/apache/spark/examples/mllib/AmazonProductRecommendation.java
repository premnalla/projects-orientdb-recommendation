/**
 * 
 */
package org.apache.spark.examples.mllib;

//$example on$
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.logging.Level;
import java.util.logging.Logger;

import scala.Tuple2;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.SparkConf;
//$example off$




import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;


/**
 * @author Prem
 *
 */
public class AmazonProductRecommendation implements Serializable {
	private static final Logger log = Logger.getLogger(AmazonProductRecommendation.class
			.getName());
	private Options m_options = new Options();
	private String[] m_args;

	private DbHelper m_h = new DbHelper();
	private OrientGraph m_conn;

    private SparkConf m_conf;
    private JavaSparkContext m_jsc;

    private MatrixFactorizationModel m_model;
    private JavaRDD<Rating> m_ratings;

    // private 
	/**
	 * 
	 */
	public AmazonProductRecommendation(String[] args) {
		m_args = args;
		m_options.addOption("h", "help", false, "show help.");
		m_options
				.addOption("i", "in", true,
						"base directory (and sub dirs) where log files to be transformed can be found");
		m_conn = m_h.getConn();

	    m_conf = new SparkConf().setAppName("Java Collaborative Filtering Example");
	    m_jsc = new JavaSparkContext(m_conf);
	    
	    
	}

	private void help() {
		// This prints out some help
		HelpFormatter formater = new HelpFormatter();

		formater.printHelp("Main", m_options);

		System.exit(0);
	}

	protected boolean parseArgs() {
		CommandLineParser parser;
		
		// parser = new DefaultParser(); // don't use this. get run-time error !!!!!
		parser = new GnuParser();
		
		CommandLine cmd = null;
		try {
			cmd = parser.parse(m_options, m_args);

			if (cmd.hasOption("h"))
				help();

			if (cmd.hasOption("i")) {
				log.log(Level.INFO,
						"Using cli argument -i=" + cmd.getOptionValue("i"));
				// m_inFile = cmd.getOptionValue("i");
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

	public void dumpRatingsFromDatabase() throws Exception {
		BufferedWriter br = new BufferedWriter(new FileWriter(new File("data/ratings.csv")));
		
		int batch = 0;
		for (Vertex v : (Iterable<Vertex>) m_conn.command(
						new OCommandSQL("SELECT FROM Buyer")).execute()) {
			// System.out.println("Buyer: " + v.getProperty("name") + ", ID: " + v.getId());
			
			int bHash = 0;
			for (Vertex v2 : (Iterable<Vertex>) m_conn.command(
					new OCommandSQL("SELECT EXPAND(in()) from ?")).execute(
					v.getId())) {
				// System.out.println("hash: " + v2.getProperty("hashId"));
				bHash = v2.getProperty("hashId");
			}

			BigDecimal score = null;
			for (Edge e1 : (Iterable<Edge>) m_conn.command(
					new OCommandSQL("SELECT EXPAND(outE()) from ?")).execute(
					v.getId())) {
				// System.out.println("score: " + e1.getProperty("score"));
				score = e1.getProperty("score");
			}

			int pHash = 0;
			for (Vertex v3 : (Iterable<Vertex>) m_conn
					.command(
							new OCommandSQL(
									"SELECT EXPAND(out().in('hashBelongsToProduct')) from ?"))
					.execute(v.getId())) {
				// System.out.println("Product-hash: " +
				// v3.getProperty("hashId") + "; " + v3);
				pHash = v3.getProperty("hashId");
			}

			br.write(bHash + "," + pHash + "," + score + "\n");
			batch++;
			if (batch == 1000) {
				br.flush();
				batch = 0;
			}
			
		}
		br.close();		
	}
	
	public void loadDataFromFile() {
	    // Load and parse the data
	    String path = "data/ratings.csv";
	    JavaRDD<String> data = m_jsc.textFile(path);
	    JavaRDD<Rating> m_ratings = data.map(
	      new Function<String, Rating>() {
	        public Rating call(String s) {
	          String[] sarray = s.split(",");
	          return new Rating(Integer.parseInt(sarray[0]), Integer.parseInt(sarray[1]),
	            Double.parseDouble(sarray[2]));
	        }
	      }
	    );		
	}
	
	public void train() {
	    // Build the recommendation model using ALS
	    int rank = 10;
	    int numIterations = 10;
	    m_model = ALS.train(JavaRDD.toRDD(m_ratings), rank, numIterations, 0.01);		
	}
	
	public void saveModel() {
	    m_model.save(m_jsc.sc(), "target/tmp/myCollaborativeFilter");
	}
	
	public void loadModel() {
		m_model = MatrixFactorizationModel.load(m_jsc.sc(),
	    	      "target/tmp/myCollaborativeFilter");
	}
	
	public void predict1(String buyer) {
		Rating[] recommendations = m_model.recommendProducts(5, 3);
		for (Rating r : recommendations) {
			System.out.println(r.product() + ": rating:" + r.rating());
		}
		
	}
	
	public void predict2(String buyer, String product) {
		
	}
	
	public void run() {
		try {
			// dumpRatingsFromDatabase();
			loadDataFromFile();
			train();
			// saveModel();
			predict1(null);
		} catch (Exception e) {
			e.printStackTrace();
		}
		// loadData();
		// train();
		// predict();
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		AmazonProductRecommendation r = new AmazonProductRecommendation(args);
		if (!r.parseArgs()) {
			System.exit(-1);
		}
		log.info("Starting recommender...");
		r.run();
		log.info("Exiting recommender...");
	}

	public class DbHelper {
		
		private OrientGraphFactory m_factory = new OrientGraphFactory("remote:192.168.123.130/test").setupPool(1,10);

		public OrientGraph getConn() {
			return m_factory.getTx();
		}
		
	}
}
