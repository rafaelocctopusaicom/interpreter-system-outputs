package br.com.embraer.ahead.interpreter.processor.system.output;

import br.com.embraer.ahead.interpreter.config.Config;
import br.com.embraer.ahead.interpreter.processor.system.annotation.SystemOutputBuilderType;
import br.com.embraer.ahead.interpreter.processor.system.config.SubSystemConfig;
import br.com.embraer.ahead.interpreter.spark.schema.SchemaBuilder;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.*;

@SystemOutputBuilderType(
        name = "EXCEEDANCE",
        topicName = "exceedance",
        schemaPath = "/system/schema/exceedance.yaml"
)
public class ExceedanceOutputBuilder implements OutputBuilder {

    protected SubSystemConfig subSystemConfig;
    public ExceedanceOutputBuilder(SubSystemConfig subSystemConfig) {
        this.subSystemConfig = subSystemConfig;
    }

    @Override
    public Dataset<Row> build(Dataset<Row> dataframe) {
        Config config = Config.getInstance();
        String tailNumber = config.getTailNumber();
        tailNumber = "-".equals(tailNumber) ? null : tailNumber;
        return dataframe
                .withColumn("customer", lit(config.getCustomer()))
                .withColumn("filename", lit(config.getFilename()))
                .withColumn("aircraftPlatform", lit(config.getAircraftPlatform()))
                .withColumn("fileRate", lit(config.getFileRate()))
                .withColumn("sysId", lit(config.getSysId()))
                .withColumn("tcrfId", lit(config.getTcrfId()))
                .withColumn("tailNumber", lit(tailNumber))
                .withColumnRenamed(subSystemConfig.getMnemonic("aircraft_serial_number"), "serialNumber")
                .withColumn("system", lit(subSystemConfig.getSystemName()))
                .withColumn("subsystem", lit(subSystemConfig.getName()))
                .withColumnRenamed("FLIGHT_NUMBER", "flightNumber")
                .withColumnRenamed("TIMESTAMP", "timestamp")
                .withColumnRenamed("process_id", "processId")
                .withColumn("result", to_json(struct(subSystemConfig.supportingDataColumns().stream()
                        .map(String::toLowerCase)
                        .sorted(String::compareTo)
                        .map(functions::col)
                        .toArray(Column[]::new))))
                .select(SchemaBuilder.getTrendColumns(subSystemConfig.getSchema()));
    }
}

