// Get the global JsonSlurper
jsonSlurper = sdc.state['json_slurper']

// Method to get the source schema's json fields that need to be converted. 
// This method will only be called for the first record the pipeline sees
def getSourceJsonFieldsThatNeedToBeConverted(theFirstRecord){

    // Will hold the list of JSON column names that need to be converted
    jsonColumns = []

    // Loop through the JDBC metadata attached to the record
    for (key in theFirstRecord.attributes.keySet()){

        // If the column's data type is JSON ('1111' is the data type for JSON)
        if (theFirstRecord.attributes[key] == '1111'){

            // Extract the column name from the JDBC attribute's metadata key
            // An example JDBC metadata attribute looks like this:
            //    jdbc.col3.jdbcType:1111
            //    so the key is "jdbc.col3.jdbcType" and the type is "1111" (JSON)
            columnName = key.substring(5, key.length() - 9).toLowerCase()

            // See if the corresponding Snowflake column is a variant
            if (snowflake_schema[columnName]  == 'variant'){

                // Add the JSON column name to the list:
               jsonColumns.add(columnName.toLowerCase())
            }
        }
    }
    return jsonColumns
}


// Get the Snowflake table's schema from global state
snowflake_schema = sdc.state['table_schema']

// Main record processing loop
for (record in sdc.records) {
    try {

        // If this is the  first record the pipeline sees, save the JSON field names that need to be converted
        if (sdc.state['first_record'] == true) {
            sdc.state['first_record'] = false
            sdc.state['json_fields'] = getSourceJsonFieldsThatNeedToBeConverted(record)
        }
        
        // Convert JSON typed fields (which the pipeline sees as Strings) to JSON
        for (columName in record.value.keySet()){
            if (sdc.state['json_fields'].contains(columName)){
                record.value[columName] = jsonSlurper.parseText(record.value[columName])
                
            }
        }
        sdc.output.write(record)

    } catch (e) {
        // Write a record to the error pipeline
        sdc.log.error(e.toString(), e)
        sdc.error.write(record, e.toString())
    }
}
