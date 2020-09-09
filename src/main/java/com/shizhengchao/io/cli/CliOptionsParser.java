package com.shizhengchao.io.cli;

import org.apache.commons.cli.*;

public class CliOptionsParser {
    public static final Option OPTIONS_SQL_FILE = Option
            .builder("f")
            .required(false)
            .longOpt("sqlfile")
            .numberOfArgs(1)
            .argName("the sql file path")
            .desc("specify the sql file path")
            .build();

    public static final Option OPTIONS_JOB_NAME = Option
            .builder("jn")
            .required(false)
            .longOpt("jobname")
            .numberOfArgs(1)
            .argName("the jobname of sql task")
            .desc("specify the jobname of sql task")
            .build();

    private static final Options CLIENT_OPTIONS = getClientOptions(new Options());

    private static Options getClientOptions(Options options) {
        options.addOption(OPTIONS_SQL_FILE);
        options.addOption(OPTIONS_JOB_NAME);
        return options;
    }


    public static CliOptions parseArgument(String[] args) {
        if (args.length < 1) {
            throw new RuntimeException("the sql file path must be specified: -f <sqlfile>");
        }
        try {
            DefaultParser parser = new DefaultParser();
            CommandLine commandLine = parser.parse(CLIENT_OPTIONS, args, true);
            return new CliOptions(commandLine);
        } catch (ParseException e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}
