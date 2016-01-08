package org.apache.spark.examples.mllib;

import java.io.Serializable;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;

public class AmazonProductRecommendation2 implements Serializable {
	private static final Logger log = Logger.getLogger(AmazonProductRecommendation2.class
			.getName());

	private Options m_options = new Options();
	private String[] m_args;

	// private OrientGraph m_conn;
	private String m_dbConnStr;

	/*
    private SparkConf m_conf;
    private JavaSparkContext m_jsc;
     */

    
    // private MatrixFactorizationModel m_model;
    // private JavaRDD<Rating> m_ratings;
		
	private String m_userId;
	private String m_productId;
	
   private AmazonProductRecommendation2(String[] args) {
		m_args = args;
		m_options.addOption("h", "help", false, "show help.");
		m_options.addOption("c", "in", true,	"db connection string");
		m_options.addOption("u", "in", true,	"user-id(string)");
		m_options.addOption("p", "in", true,	"prodct-id(string)");
		// m_conf = new SparkConf().setAppName("Amazon product recommendation");
		// m_jsc = new JavaSparkContext(m_conf);
    }
    
	private void help() {
		// This prints out some help
		HelpFormatter formater = new HelpFormatter();

		formater.printHelp("Main", m_options);

		System.exit(0);
	}

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
				// m_conn = new OrientGraphFactory(cmd.getOptionValue("c")).setupPool(1,10).getTx();
				// Whatever you want to do with the setting goes here
			} else {
				log.log(Level.SEVERE, "Missing -c option");
				help();
			}

			if (cmd.hasOption("u")) {
				log.log(Level.INFO, "Using cli argument -u=" + cmd.getOptionValue("u"));
				m_userId = cmd.getOptionValue("u");
				// Whatever you want to do with the setting goes here
			} else {
				log.log(Level.SEVERE, "Missing -u option");
				help();
			}

			if (cmd.hasOption("p")) {
				log.log(Level.INFO, "Using cli argument -p=" + cmd.getOptionValue("p"));
				m_productId = cmd.getOptionValue("p");
				// Whatever you want to do with the setting goes here
			} else {
				// log.log(Level.SEVERE, "Missing -u option");
				// help();
			}

		} catch (ParseException e) {
			log.log(Level.SEVERE, "Failed to parse comand line properties", e);
			help();
		}

		return (true);
	}

	private JavaRDD<Rating> loadDataFromFile(JavaSparkContext jsc) {
	    // Load and parse the data
	    String path = "data/ratings.csv";
	    JavaRDD<String> data = jsc.textFile(path);
	    JavaRDD<Rating> ratings = data.map(
	      new Function<String, Rating>() {
	        public Rating call(String s) {
	          String[] sarray = s.split(",");
	          return new Rating(Integer.parseInt(sarray[0]), Integer.parseInt(sarray[1]),
	            Double.parseDouble(sarray[2]));
	        }
	      }
	    );	
	    return (ratings);
	}
	
	private void predictProducts(OrientGraph conn, MatrixFactorizationModel model) {
		
		int iUserId = -1;
		String query = "SELECT EXPAND( in() ) from Buyer where name='" + m_userId + "'";
		for (Vertex v : (Iterable<Vertex>) conn.command(new OCommandSQL(query)).execute()) {
			// System.out.println("hash: " + v2.getProperty("hashId"));
			iUserId = v.getProperty("hashId");
			break;
		}

		Rating[] recommendations = model.recommendProducts(iUserId, 10);
		System.out.println("Product recommendations for " + m_userId + " are:");
		for (Rating r : recommendations) {
			System.out.println("ProductId:" + getProductId(conn, r.product()) + ": rating:" + r.rating());
		}

	}
	
	private String getProductId(OrientGraph conn, int iProductId) {
		String ret = null;
		String query = "SELECT EXPAND( out() ) from ProductHash where hashId=" + iProductId;
		for (Vertex v : (Iterable<Vertex>) conn.command(new OCommandSQL(query)).execute()) {
			// System.out.println("hash: " + v2.getProperty("hashId"));
			ret = v.getProperty("name");
			break;
		}
		
		return (ret);
	}
	
	
	private void predictProductLikelihood() {
		
	}
	
	private MatrixFactorizationModel train(JavaRDD<Rating> ratings) {
	    int rank = 10;
	    int numIterations = 10;
	    return (ALS.train(JavaRDD.toRDD(ratings), rank, numIterations, 0.01));				
	}
	
	private void run(JavaSparkContext jsc, OrientGraph conn) {
		JavaRDD<Rating> ratings = loadDataFromFile(jsc);
		MatrixFactorizationModel model = train(ratings);
		if (m_userId != null & m_productId != null) {
			predictProductLikelihood();
		} else if (m_userId != null) {
			predictProducts(conn, model);
		} else {
			System.out.println("notning to do....");
		}
	}
	
	public static void main(String[] args) {
		AmazonProductRecommendation2 a = new AmazonProductRecommendation2(args);
		if (!a.parseArgs()) {
			System.exit(-1);
		}

		SparkConf conf = new SparkConf().setAppName("Amazon product recommendation");
	    JavaSparkContext jsc = new JavaSparkContext(conf);
		OrientGraph conn = new OrientGraphFactory(a.m_dbConnStr).setupPool(1,10).getTx();

		System.out.println("starting predictor...");
		a.run(jsc, conn);		
		System.out.println("Exiting recommender...");
	}

}
