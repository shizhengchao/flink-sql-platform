package com.shizhengchao.io.cli;

import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class SqlParser {
    private SqlParser () {}

    private static final Function<String[], Optional<String[]>> NO_OPERANDS = (oprands) -> Optional.of(new String[0]);
    private static final Function<String[], Optional<String[]>> SINGLE_OPERAND = (oprands) -> Optional.of(new String[]{oprands[0]});
    private static final int DEFAULT_PATTERN_FLAGS = Pattern.CASE_INSENSITIVE | Pattern.DOTALL;

    public static List<FlinkSQLCall> parser(List<String> lines) {
        List<FlinkSQLCall> calls = new ArrayList<>();
        StringBuilder stmt = new StringBuilder();
        for (String line : lines) {
            if (line.trim().isEmpty() || line.startsWith("--")) {
                continue;
            }
            stmt.append("\n").append(line);
            if (line.endsWith(";")) {
                Optional<FlinkSQLCall> optinalCall = parser(stmt.toString());
                if (optinalCall.isPresent()) {
                    calls.add(optinalCall.get());
                } else {
                    throw new RuntimeException("Unsupported sql '" + stmt.toString() + "'");
                }
                stmt.setLength(0);
            }
        }
        return calls;
    }

    public static Optional<FlinkSQLCall> parser(String stmt) {
        stmt = stmt.trim();
        if (stmt.endsWith(";")) {
            stmt = stmt.substring(0, stmt.length() - 1).trim();
        }
        Optional<FlinkSQLCall> optionalCall = parserByRegexMatching(stmt);
        if (optionalCall.isPresent()) {
            return optionalCall;
        } else {
            return Optional.of(new FlinkSQLCall(FlinkSQL.FLINK_SQL, new String[]{stmt}));
        }
    }

    public static Optional<FlinkSQLCall> parserByRegexMatching(String stmt) {
        for (FlinkSQL flinkSql : FlinkSQL.values()) {
            if (flinkSql.hasPattern()) {
                final Matcher matcher = flinkSql.pattern.matcher(stmt);
                if (matcher.matches()) {
                    String[] groups = new String[matcher.groupCount()];
                    for (int i = 0; i < groups.length; i++) {
                        groups[i] = matcher.group(i + 1);
                    }
                    return flinkSql.operand.apply(groups).map((operands) -> {
                        String[] newOperands = operands;
                        if (flinkSql == FlinkSQL.EXPLAIN) {
                            newOperands = new String[] {"EXPLAIN PLAN FOR " + operands[0] + " " + operands[1]};
                        }
                        return new FlinkSQLCall(flinkSql, newOperands);
                    });
                }
            }
        }
        return Optional.empty();
    }

    public enum FlinkSQL {
        INSERT_INTO("(INSERT\\s+INTO.*)", SINGLE_OPERAND),
        INSERT_OVERWRITE("(INSERT\\s+OVERWRITE.*)", SINGLE_OPERAND),
        FLINK_SQL,
        EXPLAIN("EXPLAIN\\s+(.*)",
                SINGLE_OPERAND),
        SET(
                "SET(\\s+(\\S+)\\s*=(.*))?", // whitespace is only ignored on the left side of '='
                (operands) -> {
                    if (operands.length < 3) {
                        return Optional.empty();
                    } else if (operands[0] == null) {
                        return Optional.of(new String[0]);
                    }
                    return Optional.of(new String[]{operands[1].replaceAll("'", ""), operands[2].replaceAll("'", "")});
                });

        private final Pattern pattern;

        private final Function<String[], Optional<String[]>> operand;

        FlinkSQL() {
            this.pattern = null;
            this.operand = null;
        }

        FlinkSQL(String regex, Function<String[], Optional<String[]>> operand) {
            this.pattern = Pattern.compile(regex, DEFAULT_PATTERN_FLAGS);
            this.operand = operand;
        }

        public boolean hasOperands() {
            return operand != null;
        }

        public boolean hasPattern() {
            return pattern != null;
        }
    }

    /**
     * Call of SQL command with operands and command type.
     */
    public static class FlinkSQLCall {
        public final FlinkSQL sql;
        public final String[] operands;

        public FlinkSQLCall(FlinkSQL sql, String[] operands) {
            this.sql = sql;
            this.operands = operands;
        }

        public FlinkSQLCall(FlinkSQL sql) {
            this(sql, new String[0]);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FlinkSQLCall that = (FlinkSQLCall) o;
            return sql == that.sql && Arrays.equals(operands, that.operands);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(sql);
            result = 31 * result + Arrays.hashCode(operands);
            return result;
        }

        @Override
        public String toString() {
            return sql + "(" + Arrays.toString(operands) + ")";
        }
    }
}
