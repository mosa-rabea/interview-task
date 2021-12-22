package com.progressoft.tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class NormalizerImpl implements Normalizer {
    static int indexOfColumn;

    @Override
    public ScoringSummary zscore(Path csvPath, Path destPath, String colToStandardize) {

        if (!Files.exists(csvPath)) {
            throw new IllegalArgumentException("source file not found");
        }

        if (Files.exists(csvPath)) {

            try {
                try (BufferedReader Buff = new BufferedReader(new FileReader(csvPath.toFile()))) {
                    String text = Buff.readLine();

                    String[] cols = text.split(",");
                    boolean contains = Arrays.stream(cols).anyMatch(colToStandardize::equals);

                    if (!contains) {
                        throw new IllegalArgumentException("column " + colToStandardize + " not found");

                    } else {

                        // the col exists , read the rest of the rows

                        List<String> lines = Files.readAllLines(csvPath, StandardCharsets.UTF_8);
                        String[] header = lines.get(0).split(",");
                        int colIndex = java.util.Arrays.asList(header).indexOf(colToStandardize);

                        lines.remove(0);
                        List<BigDecimal> summary = new ArrayList<BigDecimal>();

                        for (String line : lines) {
                            String[] array = line.split(",");
                            summary.add(new BigDecimal(array[colIndex]));
                        }

                        addNewColumn(csvPath, destPath, summary, colToStandardize);

                        // calculation for each method from sum list that contains all values

                        return new ScoringSummary() {
                            @Override
                            public BigDecimal mean() {

                                BigDecimal result = new BigDecimal(
                                        summary.stream().mapToDouble(BigDecimal::doubleValue).sum());
                                result = result.divide(new BigDecimal(summary.size()), RoundingMode.HALF_UP);
                                result = result.setScale(2, RoundingMode.HALF_UP);
                                return result;

                            }

                            @Override
                            public BigDecimal standardDeviation() {
                                BigDecimal mean = mean();
                                BigDecimal sum = new BigDecimal(0);
                                for (BigDecimal value : summary) {
                                    BigDecimal diff = value.subtract(mean);
                                    sum = sum.add(diff.pow(2));
                                }
                                BigDecimal result = sum.divide(new BigDecimal(summary.size()), RoundingMode.DOWN);
                                MathContext mc = new MathContext(10);
                                BigDecimal sqrt = result.sqrt(mc);
                                sqrt = sqrt.setScale(2, RoundingMode.UP);
                                return sqrt;

                            }

                            @Override
                            public BigDecimal variance() {
                                BigDecimal mean = mean();
                                BigDecimal sum = new BigDecimal(0);
                                for (BigDecimal value : summary) {
                                    BigDecimal diff = value.subtract(mean);
                                    sum = sum.add(diff.pow(2));
                                }
                                BigDecimal result = sum.divide(new BigDecimal(summary.size()), RoundingMode.CEILING);
                                BigDecimal scaled = result.setScale(0, RoundingMode.HALF_UP).setScale(2,
                                        RoundingMode.UP);
                                return scaled;
                            }

                            @Override
                            public BigDecimal median() {
                                int size = summary.size();
                                BigDecimal median = new BigDecimal(
                                        summary.stream().mapToDouble(BigDecimal::doubleValue).sorted()
                                                .skip((size - 1) / 2).limit(2 - size % 2).average().orElse(Double.NaN));

                                return median.setScale(2, RoundingMode.UP);
                            }

                            @Override
                            public BigDecimal min() {
                                BigDecimal minimum = new BigDecimal(Double.MAX_VALUE);
                                for (BigDecimal value : summary) {
                                    if (value.compareTo(minimum) < 0) {
                                        minimum = value;
                                    }
                                }
                                return minimum.setScale(2, RoundingMode.UP);
                            }

                            @Override
                            public BigDecimal max() {
                                BigDecimal minimum = new BigDecimal(Double.MIN_VALUE);
                                for (BigDecimal value : summary) {
                                    if (value.compareTo(minimum) > 0) {
                                        minimum = value;
                                    }
                                }
                                return minimum.setScale(2, RoundingMode.UP);
                            }
                        };

                    }
                }

            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }

        }

        return null;

    }

    @Override
    public ScoringSummary minMaxScaling(Path csvPath, Path destPath, String colToNormalize)
            throws IllegalArgumentException {
        if (!Files.exists(csvPath)) {
            throw new IllegalArgumentException("source file not found");
        }

        if (Files.exists(csvPath)) {
            try {
                try (BufferedReader Buff = new BufferedReader(new FileReader(csvPath.toFile()))) {
                    String text = Buff.readLine();
                    String[] cols = text.split(",");
                    boolean contains = Arrays.stream(cols).anyMatch(colToNormalize::equals);

                    if (!contains) {
                        throw new IllegalArgumentException("column " + colToNormalize + " not found");
                    } else {
                        List<String> lines = Files.readAllLines(csvPath, StandardCharsets.UTF_8);
                        String[] header = lines.get(0).split(",");
                        int colIndex = java.util.Arrays.asList(header).indexOf(colToNormalize);

                        lines.remove(0);
                        List<BigDecimal> summary = new ArrayList<BigDecimal>();

                        for (String line : lines) {
                            String[] array = line.split(",");
                            summary.add(new BigDecimal(array[colIndex]));
                        }

                        if (!Files.exists(destPath)) {
                            addNewColumn_Normalize(csvPath, destPath, summary, colToNormalize);
                        }

                        // calculation for each method from summ list that contains all values
                        return new ScoringSummary() {
                            @Override
                            public BigDecimal mean() {

                                BigDecimal result = new BigDecimal(
                                        summary.stream().mapToDouble(BigDecimal::doubleValue).sum());
                                result = result.divide(new BigDecimal(summary.size()), RoundingMode.HALF_UP);
                                result = result.setScale(2, RoundingMode.HALF_UP);
                                return result;

                            }

                            @Override
                            public BigDecimal standardDeviation() {
                                BigDecimal mean = mean();
                                BigDecimal sum = new BigDecimal(0);
                                for (BigDecimal value : summary) {
                                    BigDecimal diff = value.subtract(mean);
                                    sum = sum.add(diff.pow(2));
                                }
                                BigDecimal result = sum.divide(new BigDecimal(summary.size()), RoundingMode.DOWN);
                                MathContext mc = new MathContext(10);
                                BigDecimal sqrt = result.sqrt(mc);
                                sqrt = sqrt.setScale(2, RoundingMode.UP);
                                return sqrt;

                            }

                            @Override
                            public BigDecimal variance() {
                                BigDecimal mean = mean();
                                BigDecimal sum = new BigDecimal(0);
                                for (BigDecimal value : summary) {
                                    BigDecimal diff = value.subtract(mean);
                                    sum = sum.add(diff.pow(2));
                                }
                                BigDecimal result = sum.divide(new BigDecimal(summary.size()), RoundingMode.CEILING);

                                BigDecimal scaled = result.setScale(0, RoundingMode.HALF_UP).setScale(2,
                                        RoundingMode.UP);
                                return scaled;
                            }

                            @Override
                            public BigDecimal median() {
                                int size = summary.size();
                                BigDecimal median = new BigDecimal(
                                        summary.stream().mapToDouble(BigDecimal::doubleValue).sorted()
                                                .skip((size - 1) / 2).limit(2 - size % 2).average().orElse(Double.NaN));

                                return median.setScale(2, RoundingMode.UP);
                            }

                            @Override
                            public BigDecimal min() {
                                BigDecimal minimum = new BigDecimal(Double.MAX_VALUE);
                                for (BigDecimal value : summary) {
                                    if (value.compareTo(minimum) < 0) {
                                        minimum = value;
                                    }
                                }
                                return minimum.setScale(2, RoundingMode.UP);
                            }

                            @Override
                            public BigDecimal max() {
                                BigDecimal minimum = new BigDecimal(Double.MIN_VALUE);
                                for (BigDecimal value : summary) {
                                    if (value.compareTo(minimum) > 0) {
                                        minimum = value;
                                    }
                                }
                                return minimum.setScale(2, RoundingMode.UP);
                            }
                        };

                    }
                }

            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }

        }

        return null;
    }

    // #region helper methods
    public void addNewColumn(Path csvPath, Path destPath, List<BigDecimal> data, String col) throws IOException {

        BufferedReader br = null;
        BufferedWriter bw = null;
        try {

            final String lineSep = System.getProperty("line.separator");
            Path file = csvPath;
            Path file2 = destPath;

            br = new BufferedReader(new InputStreamReader(new FileInputStream(file.toFile()), "UTF-8"));
            bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file2.toFile()), "UTF-8"));
            String line = null;
            int i = 0;
            while ((line = br.readLine()) != null) {
                if (i == 0) {
                    String[] array = line.split(",");
                    List<String> list = new ArrayList<String>(Arrays.asList(array));
                    int indexOfCol = list.indexOf(col);

                    list.add(indexOfCol + 1, col + "_z");
                    String[] array2 = list.toArray(new String[0]);
                    StringBuilder sb = new StringBuilder();
                    for (String s : array2) {
                        sb.append(s);
                        if (!(java.util.Arrays.asList(array2).indexOf(s) == array2.length - 1)) {
                            sb.append(",");
                        }
                    }
                    String result = sb.toString();
                    bw.write(result);
                    bw.write(lineSep);
                } else {

                    ScoringSummary summary = zscore(csvPath, destPath, col + "_z");

                    BigDecimal mean = summary.mean();
                    BigDecimal std = summary.standardDeviation();
                    BigDecimal value = data.get(i - 1);
                    BigDecimal z = value.subtract(mean).divide(std, RoundingMode.DOWN);
                    bw.write(line + "," + z);
                    bw.write(lineSep);

                }
                i++;

            }

        } catch (Exception e) {
            System.out.println(e);
        } finally {
            if (br != null)
                br.close();
            if (bw != null)
                bw.close();
        }
    }

    public void addNewColumn_Normalize(Path csvPath, Path destPath, List<BigDecimal> data, String col)
            throws IOException {
        BufferedReader br = null;
        BufferedWriter bw = null;

        try {

            final String lineSep = System.getProperty("line.separator");
            Path file = csvPath;
            Path file2 = destPath;

            br = new BufferedReader(new InputStreamReader(new FileInputStream(file.toFile()), "UTF-8"));
            bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file2.toFile()), "UTF-8"));
            String line = null;
            int i = 0;
            while ((line = br.readLine()) != null) {
                if (i == 0) {
                    String[] array = line.split(",");
                    List<String> list = new ArrayList<String>(Arrays.asList(array));

                    NormalizerImpl.indexOfColumn = list.indexOf(col);

                    list.add(indexOfColumn + 1, col + "_mm");
                    String[] array2 = list.toArray(new String[0]);
                    StringBuilder sb = new StringBuilder();
                    for (String s : array2) {
                        sb.append(s);
                        if (!(java.util.Arrays.asList(array2).indexOf(s) == array2.length - 1)) {
                            sb.append(",");
                        }
                    }
                    String result = sb.toString();
                    bw.write(result);
                    bw.write(lineSep);
                } else {
                    ScoringSummary summary = minMaxScaling(csvPath, destPath, col);
                    double min = summary.min().doubleValue();
                    double max = summary.max().doubleValue();
                    BigDecimal value = data.get(i - 1);
                    double scale = (value.doubleValue() - min) / (max - min);
                    BigDecimal b = new BigDecimal(scale, MathContext.DECIMAL64).setScale(2, RoundingMode.HALF_UP);
                    String[] array = line.split(",");
                    List<String> list = new ArrayList<String>(Arrays.asList(array));

                    list.add(indexOfColumn + 1, b.toString());
                    String[] array2 = list.toArray(new String[0]);
                    StringBuilder sb = new StringBuilder();
                    for (String s : array2) {
                        sb.append(s);
                        if (!(java.util.Arrays.asList(array2).indexOf(s) == array2.length - 1)) {
                            sb.append(",");
                        }
                    }
                    String result = sb.toString();
                    bw.write(result);

                    bw.write(lineSep);
                }
                i++;

            }

        } catch (Exception e) {

        } finally {
            if (br != null)
                br.close();
            if (bw != null)
                bw.close();
        }

    }

    // #endregion

}