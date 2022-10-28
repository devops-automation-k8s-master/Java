/* 
With the help of this program If Data on our database is not updated frequently but read query is very frequently then we can use some cacher for read query to get result very fast
So here First query will go to redis and check that query result is available in redis or not, if it is avaialble then directly send to user, if not then query pass to DB and send to user and store as well in redis for further use.

*/
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import java.util.List;
import java.util.*;
import java.sql.*; 

public class RedisCacher {
    private static class Md5 {
	    /**
	     * Returns a hexadecimal encoded MD5 hash for the input String.
	     * @param data
	     * @return 
	     */
	    private static String getMD5Hash(String data) {
	        String result = null;
	        try {
	            MessageDigest digest = MessageDigest.getInstance("MD5");
	            byte[] hash = digest.digest(data.getBytes("UTF-8"));
	            return bytesToHex(hash); // make it printable
	        }catch(Exception ex) {
	            ex.printStackTrace();
	        }
	        return result;
	    }
	    
	    /**
	     * Use javax.xml.bind.DatatypeConverter class in JDK to convert byte array
	     * to a hexadecimal string. Note that this generates hexadecimal in upper case.
	     * @param hash
	     * @return 
	     */
	    private static String  bytesToHex(byte[] hash) {
	        return DatatypeConverter.printHexBinary(hash);
	    }
    }

    private static JedisPool redisPool;
    private static Connection dbConnection;

    private static String dbConnection_DRIVER = "com.mysql.jdbc.Driver";
    /* below are some input prameter which is hardcoded can be take as runtime as well*/
    private static String dbConnection_URL = "JDBC DBURL";
    private static String dbConnection_USER = "DBUSERNAME";
    private static String dbConnection_PASS = "DBConnectionPassword";
    private static String redisConnection_HOST = "RedisHostUrl";
    private static int redisConnection_PORT = 6379;

    private boolean keyExists(Jedis redis, String hashKey) {
        return redis.exists(hashKey+"-EXISTS");
    }

    private static Jedis getRedisConnection() {
		JedisPoolConfig poolconfig=new JedisPoolConfig();
		redisPool = new JedisPool(poolconfig,redisConnection_HOST, redisConnection_PORT);
		return redisPool.getResource();
    }

    private static ResultSet getMySqlResultSet(String query) {
        Class.forName(dbConnection_DRIVER);
		dbConnection = DriverManager.getConnection(dbConnection_URL,dbConnection_USER,dbConnection_PASS);  
		Statement stmt=con.createStatement();  
        return stmt.executeQuery(query);
    }

    private static List<String> getRedisResultSet(Jedis redis, String hashKey) {
        return redis.lrange(hashKey+"-DATA", 0, -1);
    }

    private static String getQuery() {
        Scanner in = new Scanner(System.in);
        String line = "";

        while( line.trim() != "" ) {
            line = in.nextLine();
        }

        return line;
    }

    private static List<String> storeIntoRedis(Jedis redis, String hashKey, ResultSet resultSet) {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int colCount = metaData.getColumnCount();
        ArrayList<String> rows = new ArrayList<>();
        
        while( resultSet.next() ) {
            String[] values = new String[colCount];
            for(int i=0 ; i<colCount; i++ ) {
                values[i] = resultSet.getString(i);
            }
            rows.add(String.join(",", values));
        }

        redis.lpush(hashKey+"-DATA", rows.toArray(new String[rows.size()]));
        return rows;
    }

    private static void printResultSet(List<String> rows) {
        for(int i=0 ; i<rows.size() ; i++){
            System.out.println(rows[i]);
        }
    }

    public static void main(String[] args) {
        String query = getQuery();
        Jedis redis = getRedisConnection();
        String hashKey = Md5.getMD5Hash(query);
        List<String> strResultSet = null;

        if( keyExists(redis, hashKey) ) {
            ResultSet resultSet = getMySqlResultSet(query);
            strResultSet = storeIntoRedis(redis, hashKey, resultSet);
        } else {
            strResultSet = getRedisResultSet(redis, hashKey); 
        }

        printResultSet(strResultSet);
    }
}