package br.com.embraer.ahead.interpreter.processor.system.output;

import br.com.embraer.ahead.interpreter.config.Config;
import br.com.embraer.ahead.interpreter.processor.system.annotation.SystemOutputBuilderType;
import br.com.embraer.ahead.interpreter.processor.system.config.SubSystemConfig;
import br.com.embraer.ahead.interpreter.spark.schema.SchemaBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

@SystemOutputBuilderType(
        name = "SINGLE_ENGINE_TAXI",
        topicName = "single-engine-taxi",
        schemaPath = "/system/schema/single-engine-taxi.yaml"
)
public class SingleEngineTaxiOutputBuilder implements OutputBuilder {

    public static final int SCALE = 3;

    protected SubSystemConfig subSystemConfig;

    public SingleEngineTaxiOutputBuilder(SubSystemConfig subSystemConfig) {
        this.subSystemConfig = subSystemConfig;
    }

    @Override
    public Dataset<Row> build(Dataset<Row> dataframe) {
        Config config = Config.getInstance();

        String tailNumber = config.getTailNumber();
        tailNumber = "-".equals(tailNumber) ? null : tailNumber;
        return dataframe
                .withColumn("customer", lit(config.getCustomer()))
                .withColumn("aircraftPlatform", lit(config.getAircraftPlatform()))
                .withColumnRenamed(subSystemConfig.getMnemonic("aircraft_serial_number"), "serialNumber")
                .withColumn("tailNumber", lit(tailNumber))
                .withColumnRenamed("FLIGHT_NUMBER", "flightNumber")
                .withColumnRenamed("TIMESTAMP", "timestamp")
                .withColumn("filename", lit(config.getFilename()))
                .withColumn("fileRate", lit(config.getFileRate()))
                .withColumn("sysId", lit(config.getSysId()))
                .withColumn("tcrfId", lit(config.getTcrfId()))
                .select(SchemaBuilder.getTrendColumns(subSystemConfig.getSchema()));
    }
}

