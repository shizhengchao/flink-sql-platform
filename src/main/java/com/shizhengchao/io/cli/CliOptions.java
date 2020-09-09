package com.shizhengchao.io.cli;

import org.apache.commons.cli.CommandLine;

public class CliOptions {
    private CommandLine commandLine;

    public CliOptions(CommandLine commandLine) {
        this.commandLine = commandLine;
    }

    public CommandLine getCommandLine() {
        return commandLine;
    }
}
