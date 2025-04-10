import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import java.sql.ResultSet
import groovy.json.JsonSlurper


// Snowflake properties
baseUrl = sdc.userParams['SNOWFLAKE_URL']
warehouse = sdc.userParams['SNOWFLAKE_WH']
database = sdc.userParams['SNOWFLAKE_DB']
schema = sdc.userParams['SNOWFLAKE_SCHEMA']
user = sdc.userParams['SNOWFLAKE_USER']
password = sdc.userParams['SNOWFLAKE_PASSWORD']
role = sdc.userParams['SNOWFLAKE_ROLE']
table = sdc.userParams['SNOWFLAKE_TABLE']

// Method to create the Snowflake URL
def createSnowflakeUrl(){
  def snowflakeUrl = StringBuilder.newInstance()
  snowflakeUrl.with {
    append baseUrl
    append "/?warehouse=" + warehouse
    append "&db=" + database
    append "&schema=" + schema
    append "&role=" + role
  }
  return snowflakeUrl.toString()
}

// Method to execute a query against Snowflake to get the schema for the target table
def getSnowflakeTableSchema() {

  def conn = null
  def stmt = null
  def rs = null
  def url = createSnowflakeUrl()

  def query = "SELECT * FROM " +  database + ".information_schema.columns WHERE table_schema = '" + schema + "' and table_name = '" + table + "'"
 
  Class.forName("net.snowflake.client.jdbc.SnowflakeDriver")

  // A map of snowflake column names and types for the target table
  snowflakeTableColumnNamesAndTypes =  [:]

  try {

      conn = DriverManager.getConnection(url, user, password)
      stmt = conn.createStatement();
      rs = stmt.executeQuery(query);
      if (!rs.next()) { 
         throw new Exception("Error: the table schema for the target table was not found")
      } else {
          do {
              // Get the column names and data types
              columnName = rs.getString("COLUMN_NAME").toLowerCase(); 
              columnType = rs.getString("DATA_TYPE").toLowerCase(); 

              // Store the table column names and data types in the map
              snowflakeTableColumnNamesAndTypes[columnName] = columnType

          } while (rs.next())
      }
                      
      return snowflakeTableColumnNamesAndTypes

  } finally {
      try {
          if (rs != null) rs.close(); 
          if (stmt != null) stmt.close();
          if (conn != null) conn.close();
      } catch (Exception e) {
        // swallow
      }
  }
}

// Save the target table schema info in the sdc.state collection so it's visible in the main script
sdc.state['table_schema'] = getSnowflakeTableSchema()

// The main script will set this to false after we process the first record
sdc.state['first_record'] = true

// The main script will save the source json field names that need to be converted in this variable when it processes the first record
sdc.state['json_fields'] = []

// Create a global JsonSlurper
sdc.state['json_slurper'] = new JsonSlurper()
