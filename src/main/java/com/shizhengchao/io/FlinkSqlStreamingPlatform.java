package com.shizhengchao.io;

import com.shizhengchao.io.cli.CliOptions;
import com.shizhengchao.io.cli.CliOptionsParser;
import com.shizhengchao.io.cli.SqlParser;
import org.apache.commons.cli.CommandLine;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.TableConfigOptions;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;

import static com.shizhengchao.io.cli.CliOptionsParser.OPTIONS_JOB_NAME;
import static com.shizhengchao.io.cli.CliOptionsParser.OPTIONS_SQL_FILE;

public class FlinkSqlStreamingPlatform {

    private final CliOptions options;
    private final StreamTableEnvironment stenv;
    private final StreamExecutionEnvironment env;
    private final StatementSet statementSet;

    private FlinkSqlStreamingPlatform(CliOptions options) {
        this.options = options;
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        stenv = StreamTableEnvironment.create(env, settings);
        statementSet = stenv.createStatementSet();
    }

    public static void main(String[] args) throws Exception{
        if (args == null || args.length == 0) {
            throw new RuntimeException("the sql file path must be specified: -f <sqlfile>");
        }
        CliOptions options = CliOptionsParser.parseArgument(args);
        FlinkSqlStreamingPlatform platform = new FlinkSqlStreamingPlatform(options);
        platform.run();
    }

    private void run() throws IOException {
        CommandLine commandLine = options.getCommandLine();
        setDefaultConfig();
        String jobName = commandLine.getOptionValue(OPTIONS_JOB_NAME.getOpt());
        if (jobName != null) {
            stenv.getConfig().getConfiguration().setString("job.name", jobName);
        }
        String sqlFile = commandLine.getOptionValue(OPTIONS_SQL_FILE.getOpt());
        List<String> sqls = Files.readAllLines(Paths.get(sqlFile));
        List<SqlParser.FlinkSQLCall> calls = SqlParser.parser(sqls);
        for (SqlParser.FlinkSQLCall call : calls) {
            callCommand(call);
        }
        statementSet.execute();
    }

    private void setDefaultConfig() {
        stenv.getConfig().getConfiguration().set(TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED, true);
        //checkpoint
        stenv.getConfig().addConfiguration(
                new Configuration()
                        .set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofMinutes(2))
                        .set(ExecutionCheckpointingOptions.MIN_PAUSE_BETWEEN_CHECKPOINTS, Duration.ofSeconds(10)));

        stenv.getConfig().addConfiguration(
                new Configuration()
                        .set(RestartStrategyOptions.RESTART_STRATEGY, "failure-rate")
                        .set(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_DELAY, Duration.ofSeconds(10))
                        .set(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL, Duration.ofMinutes(5))
                        .set(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL, 3)
        );

    }

    private void callCommand(SqlParser.FlinkSQLCall call) {
        switch (call.sql) {
            case SET:
                callSet(call);
                break;
            case INSERT_INTO:
            case INSERT_OVERWRITE:
                callInsert(call);
                break;
            case FLINK_SQL:
                callFlinkSql(call);
                break;
            default:
                throw new RuntimeException("Unsupported flink sql: " + call.sql);
        }

    }

    private void callSet(SqlParser.FlinkSQLCall call) {
        stenv.getConfig().getConfiguration().setString(call.operands[0], call.operands[1]);
    }

    private void callInsert(SqlParser.FlinkSQLCall call) {
        statementSet.addInsertSql(call.operands[0]);
    }

    private void callFlinkSql(SqlParser.FlinkSQLCall call) {
        stenv.executeSql(call.operands[0]);
    }
}
