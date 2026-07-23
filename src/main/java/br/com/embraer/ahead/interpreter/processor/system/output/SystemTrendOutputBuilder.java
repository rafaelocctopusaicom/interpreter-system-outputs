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
        name = "SYSTEM_TREND",
        topicName = "system-trends",
        schemaPath = "/system/schema/system-trend.yaml"
)
public class SystemTrendOutputBuilder implements OutputBuilder {

    public static final int SCALE = 3;

    protected SubSystemConfig subSystemConfig;

    public SystemTrendOutputBuilder(SubSystemConfig subSystemConfig) {
        this.subSystemConfig = subSystemConfig;
    }

    @Override
    public Dataset<Row> build(Dataset<Row> dataframe) {

        Config config = Config.getInstance();
        Column[] supportingColumns = subSystemConfig.supportingDataColumns().stream()
                .map(String::toUpperCase)
                .sorted(String::compareTo)
                .map(functions::col)
                .toArray(Column[]::new);

        String tailNumber = config.getTailNumber();
        tailNumber = "-".equals(tailNumber) ? null : tailNumber;
        return dataframe
                .withColumn("customer", lit(config.getCustomer()))
                .withColumn("filename", lit(config.getFilename()))
                .withColumn("system", lit(subSystemConfig.getSystemName()))
                .withColumn("subsystem", lit(subSystemConfig.getName()))
                .withColumn("aircraftPlatform", lit(config.getAircraftPlatform()))
                .withColumn("fileRate", lit(config.getFileRate()))
                .withColumn("sysId", lit(config.getSysId()))
                .withColumn("tcrfId", lit(config.getTcrfId()))
                .withColumnRenamed(subSystemConfig.getMnemonic("aircraft_serial_number"), "serialNumber")
                .withColumn("tailNumber", lit(tailNumber))
                .withColumnRenamed("FLIGHT_NUMBER", "flightNumber")
                .withColumnRenamed("TIMESTAMP", "timestamp")
                .withColumn("y", round(col(subSystemConfig.getColumn()), SCALE))
                .withColumn("supportingData", to_json(struct(supportingColumns)))
                .select(SchemaBuilder.getTrendColumns(subSystemConfig.getSchema()));
    }
}

