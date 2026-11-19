
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Counter;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MR2 {

    private static final String DEFAULT_INPUT =
            "/p02/mr1/account_step";

    private static final String DEFAULT_OUTPUT =
            "/p02/mr2";

    private static final int DEFAULT_WINDOW = 24;
    private static final int DEFAULT_MIN_STEP = 1;
    private static final String VERSION = "v1";

    public enum Quality {
        EMPTY,
        VALID,
        REJECTED,
        BAD_COLUMNS,
        BAD_ACCOUNT,
        BAD_STEP,
        BAD_SUBSET,
        BAD_NUMERIC,
        DUPLICATE_PROFILE
    }

    // =========================================================
    // CLE COMPOSITE (COMPTE, STEP)
    // =========================================================

    public static class AccountStepKey
            implements WritableComparable<AccountStepKey> {

        private Text account;
        private IntWritable step;

        public AccountStepKey() {
            account = new Text();
            step = new IntWritable();
        }

        public AccountStepKey(String account, int step) {
            this.account = new Text(account);
            this.step = new IntWritable(step);
        }

        public String getAccount() {
            return account.toString();
        }

        public int getStep() {
            return step.get();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            account.write(out);
            step.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            account.readFields(in);
            step.readFields(in);
        }

        @Override
        public int compareTo(AccountStepKey other) {
            int result = account.compareTo(other.account);
            if (result != 0) return result;
            return step.compareTo(other.step);
        }
    }

    // =========================================================
    // TRI TOTAL : COMPTE PUIS STEP
    // =========================================================

    public static class AccountStepSortComparator
            extends WritableComparator {

        public AccountStepSortComparator() {
            super(AccountStepKey.class, true);
        }

        @Override
        public int compare(
                WritableComparable a,
                WritableComparable b) {

            AccountStepKey x = (AccountStepKey) a;
            AccountStepKey y = (AccountStepKey) b;

            int result = x.getAccount().compareTo(y.getAccount());

            if (result != 0) return result;

            return Integer.compare(x.getStep(), y.getStep());
        }
    }

    // =========================================================
    // REGROUPEMENT PAR COMPTE
    // =========================================================

    public static class AccountGroupingComparator
            extends WritableComparator {

        public AccountGroupingComparator() {
            super(AccountStepKey.class, true);
        }

        @Override
        public int compare(
                WritableComparable a,
                WritableComparable b) {

            AccountStepKey x = (AccountStepKey) a;
            AccountStepKey y = (AccountStepKey) b;

            return x.getAccount().compareTo(y.getAccount());
        }
    }

    // =========================================================
    // PARTITIONER PAR COMPTE
    // =========================================================

    public static class AccountPartitioner
            extends Partitioner<AccountStepKey, Text> {

        @Override
        public int getPartition(
                AccountStepKey key,
                Text value,
                int numberOfPartitions) {

            return (key.getAccount().hashCode()
                    & Integer.MAX_VALUE) % numberOfPartitions;
        }
    }

    // =========================================================
    // PROFIL MR1
    // =========================================================

    public static class Profile {

        int step;
        String subset;

        long nbTransactions;

        double sommeMontants;
        double montantMoyen;
        double montantMaximum;

        long nbPayment;
        long nbTransfer;
        long nbCashOut;
        long nbDebit;
        long nbCashIn;
        long nbOther;

        public Profile(String[] f) {

            step = Integer.parseInt(f[1].trim());
            subset = f[2].trim();

            nbTransactions = Long.parseLong(f[3].trim());

            sommeMontants = Double.parseDouble(f[4].trim());
            montantMoyen = Double.parseDouble(f[5].trim());
            montantMaximum = Double.parseDouble(f[6].trim());

            nbPayment = Long.parseLong(f[7].trim());
            nbTransfer = Long.parseLong(f[8].trim());
            nbCashOut = Long.parseLong(f[9].trim());
            nbDebit = Long.parseLong(f[10].trim());
            nbCashIn = Long.parseLong(f[11].trim());
            nbOther = Long.parseLong(f[12].trim());
        }
    }

    // =========================================================
    // MAPPER
    // =========================================================

    public static class MR2Mapper
            extends Mapper<Object, Text, AccountStepKey, Text> {

        private MultipleOutputs<AccountStepKey, Text> outputs;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {

            outputs = new MultipleOutputs<>(context);
        }

        @Override
        protected void map(
                Object key,
                Text value,
                Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            if (line.trim().isEmpty()) {
                context.getCounter(Quality.EMPTY).increment(1);
                return;
            }

            String[] f = line.split("\t", -1);

            if (f.length != 13) {
                reject(context, line, "BAD_COLUMNS");
                context.getCounter(Quality.BAD_COLUMNS).increment(1);
                return;
            }

            String account = f[0].trim();

            if (account.isEmpty() || account.startsWith("!")) {
                reject(context, line, "BAD_ACCOUNT");
                context.getCounter(Quality.BAD_ACCOUNT).increment(1);
                return;
            }

            try {
                Profile p = new Profile(f);

                if (p.step < 1) {
                    reject(context, line, "BAD_STEP");
                    context.getCounter(Quality.BAD_STEP).increment(1);
                    return;
                }

                if (!p.subset.equals("train")
                        && !p.subset.equals("validation")) {
                    reject(context, line, "BAD_SUBSET");
                    context.getCounter(Quality.BAD_SUBSET).increment(1);
                    return;
                }

                if (p.nbTransactions <= 0
                        || p.nbPayment < 0
                        || p.nbTransfer < 0
                        || p.nbCashOut < 0
                        || p.nbDebit < 0
                        || p.nbCashIn < 0
                        || p.nbOther < 0
                        || !Double.isFinite(p.sommeMontants)
                        || !Double.isFinite(p.montantMoyen)
                        || !Double.isFinite(p.montantMaximum)
                        || p.sommeMontants < 0
                        || p.montantMoyen < 0
                        || p.montantMaximum < 0) {

                    throw new IllegalArgumentException();
                }

                long totalTypes = p.nbPayment + p.nbTransfer
                        + p.nbCashOut + p.nbDebit
                        + p.nbCashIn + p.nbOther;

                if (totalTypes != p.nbTransactions) {
                    throw new IllegalArgumentException();
                }

                context.write(
                        new AccountStepKey(account, p.step),
                        new Text(line)
                );

                context.getCounter(Quality.VALID).increment(1);

            } catch (Exception e) {
                reject(context, line, "BAD_NUMERIC");
                context.getCounter(Quality.BAD_NUMERIC).increment(1);
            }
        }

        private void reject(
                Context context,
                String line,
                String reason)
                throws IOException, InterruptedException {

            outputs.write(
                    "rejected",
                    NullWritable.get(),
                    new Text(reason + "\t" + line),
                    "rejected/part"
            );

            context.getCounter(Quality.REJECTED).increment(1);
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            outputs.close();
        }
    }

    // =========================================================
    // REDUCER : FENETRE HISTORIQUE STRICTEMENT PASSEE
    // =========================================================

    public static class MR2Reducer
            extends Reducer<AccountStepKey, Text,
                            NullWritable, Text> {

        private MultipleOutputs<NullWritable, Text> outputs;

        private int window;
        private int minStep;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {

            outputs = new MultipleOutputs<>(context);

            Configuration conf = context.getConfiguration();

            window = conf.getInt("mr2.window", DEFAULT_WINDOW);
            minStep = conf.getInt("mr2.min.step", DEFAULT_MIN_STEP);
        }

        @Override
        protected void reduce(
                AccountStepKey key,
                Iterable<Text> values,
                Context context)
                throws IOException, InterruptedException {

            TreeMap<Integer, Profile> history =
                    new TreeMap<>();

            int previousStep = -1;

            for (Text value : values) {

                String line = value.toString();
                String[] fields = line.split("\t", -1);

                Profile current;

                try {
                    current = new Profile(fields);
                } catch (Exception e) {
                    continue;
                }

                int s = current.step;

                if (s == previousStep) {
                    context.getCounter(
                            Quality.DUPLICATE_PROFILE
                    ).increment(1);

                    outputs.write(
                            "rejected",
                            NullWritable.get(),
                            new Text("DUPLICATE_PROFILE\t" + line),
                            "rejected/part"
                    );

                    continue;
                }

                previousStep = s;

                // Garder uniquement les pas [s-window, s-1].
                history.headMap(s - window, false).clear();

                long nbOperations24 = 0;
                double somme24 = 0.0;
                long nbTransfer24 = 0;
                long nbCashOut24 = 0;

                for (Profile old : history.values()) {

                    nbOperations24 += old.nbTransactions;
                    somme24 += old.sommeMontants;

                    nbTransfer24 += old.nbTransfer;
                    nbCashOut24 += old.nbCashOut;
                }

                long nbHistory = nbOperations24;

                boolean incomplete = (s - window) < minStep;

                String status;
                String nbText;
                String sumText;
                String meanText;
                String deltaText;
                String transferText;
                String cashOutText;

                if (incomplete) {

                    status = "INCOMPLETE";
                    nbText = "NA";
                    sumText = "NA";
                    meanText = "NA";
                    deltaText = "NA";
                    transferText = "NA";
                    cashOutText = "NA";

                } else if (nbHistory == 0) {

                    status = "NO_HISTORY";
                    nbText = "0";
                    sumText = "0";
                    meanText = "NA";
                    deltaText = "NA";
                    transferText = "NA";
                    cashOutText = "NA";

                } else {

                    status = "OK";

                    double mean24 = somme24 / nbHistory;
                    double delta = current.montantMoyen - mean24;

                    double transferRate =
                            (double) nbTransfer24 / nbHistory;

                    double cashOutRate =
                            (double) nbCashOut24 / nbHistory;

                    nbText = String.valueOf(nbHistory);
                    sumText = format(somme24);
                    meanText = format(mean24);
                    deltaText = format(delta);
                    transferText = format(transferRate);
                    cashOutText = format(cashOutRate);
                }

                String result =
                        key.getAccount() + "\t"
                        + s + "\t"
                        + current.subset + "\t"
                        + nbText + "\t"
                        + sumText + "\t"
                        + meanText + "\t"
                        + deltaText + "\t"
                        + transferText + "\t"
                        + cashOutText + "\t"
                        + status + "\t"
                        + VERSION;

                outputs.write(
                        "features",
                        NullWritable.get(),
                        new Text(result),
                        "features/part"
                );

                outputs.write(
                        "batch",
                        NullWritable.get(),
                        new Text(result),
                        "batch/part"
                );

                // Le profil courant est ajouté APRES le calcul.
                history.put(s, current);
            }
        }

        private String format(double value) {
            return String.format(Locale.US, "%.6f", value);
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            outputs.close();
        }
    }

    // =========================================================
    // MAIN : CONFIGURATION ET EXECUTION
    // =========================================================

    public static void main(String[] args) throws Exception {

        String inputPath = args.length > 0
                ? args[0] : DEFAULT_INPUT;

        String outputRoot = args.length > 1
                ? args[1] : DEFAULT_OUTPUT;

        int window = args.length > 2
                ? Integer.parseInt(args[2]) : DEFAULT_WINDOW;

        int minStep = args.length > 3
                ? Integer.parseInt(args[3]) : DEFAULT_MIN_STEP;

        if (window <= 0 || minStep <= 0) {
            throw new IllegalArgumentException(
                    "La fenetre et le pas minimum doivent etre positifs."
            );
        }

        Configuration conf = new Configuration();

        conf.setInt("mr2.window", window);
        conf.setInt("mr2.min.step", minStep);

        FileSystem fs = FileSystem.get(conf);

        Path input = new Path(inputPath);
        Path root = new Path(outputRoot);

        Path features = new Path(outputRoot + "/features_history");
        Path rejected = new Path(outputRoot + "/rejected");
        Path quality = new Path(outputRoot + "/quality");
        Path batch = new Path(outputRoot + "/batch");
        Path staging = new Path(outputRoot + "/_mr2_staging");

        if (!fs.exists(input)) {
            throw new IOException("Entree introuvable : " + inputPath);
        }

        if (fs.exists(root)) {
            throw new IOException(
                    "La sortie existe deja : " + outputRoot
                    + ". Choisir un nouveau chemin de sortie."
            );
        }

        if (fs.exists(staging)) {
            throw new IOException(
                    "Dossier temporaire existant : " + staging
                    + ". Verifier son contenu avant toute suppression."
            );
        }

        long startTime = System.currentTimeMillis();

        Job job = Job.getInstance(conf, "MR2_PaySim_Historical_Features");

        job.setJarByClass(MR2.class);

        job.setMapperClass(MR2Mapper.class);
        job.setReducerClass(MR2Reducer.class);

        job.setMapOutputKeyClass(AccountStepKey.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setPartitionerClass(AccountPartitioner.class);
        job.setSortComparatorClass(AccountStepSortComparator.class);
        job.setGroupingComparatorClass(AccountGroupingComparator.class);

        job.setNumReduceTasks(1);

        MultipleOutputs.addNamedOutput(
                job,
                "features",
                TextOutputFormat.class,
                NullWritable.class,
                Text.class
        );


        MultipleOutputs.addNamedOutput(
                job,
                "batch",
                TextOutputFormat.class,
                NullWritable.class,
                Text.class
        );

        MultipleOutputs.addNamedOutput(
                job,
                "rejected",
                TextOutputFormat.class,
                NullWritable.class,
                Text.class
        );

        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, staging);

        System.out.println("====================================");
        System.out.println(" DEMARRAGE MR2");
        System.out.println("====================================");
        System.out.println("Input     : " + inputPath);
        System.out.println("Output    : " + outputRoot);
        System.out.println("Window    : " + window);
        System.out.println("Min step  : " + minStep);
        System.out.println("Partition : account");
        System.out.println("Sort      : account, step");
        System.out.println("Grouping  : account");
        System.out.println("Combiner  : disabled");
        System.out.println("====================================");

        boolean success = job.waitForCompletion(true);

        if (!success) {
            throw new RuntimeException("Le job MR2 a echoue.");
        }

        Path stagingFeatures =
                new Path(staging, "features");

        Path stagingRejected =
                new Path(staging, "rejected");

        Path stagingBatch =
                new Path(staging, "batch");

        fs.mkdirs(root);

        if (fs.exists(stagingFeatures)) {
            if (!fs.rename(stagingFeatures, features)) {
                throw new IOException(
                        "Publication features_history impossible."
                );
            }
        } else {
            fs.mkdirs(features);
        }

        if (fs.exists(stagingRejected)) {
            if (!fs.rename(stagingRejected, rejected)) {
                throw new IOException(
                        "Publication rejected impossible."
                );
            }
        } else {
            fs.mkdirs(rejected);
        }

        fs.mkdirs(quality);

        String schema =
                "nameOrig\tstep\tsubset\t"
                + "nbOperations24\tsommeMontants24\t"
                + "moyenneHistorique24\tecartMontantCourant\t"
                + "tauxTransfer24\ttauxCashOut24\t"
                + "historyStatus\tversion\n";

        Path schemaPath =
                new Path(quality, "mr2_schema.tsv");

        try (OutputStreamWriter writer =
                new OutputStreamWriter(
                        fs.create(schemaPath, false),
                        StandardCharsets.UTF_8)) {
            writer.write(schema);
        }

        Path parameterPath =
                new Path(quality, "mr2_parameters.properties");

        try (OutputStreamWriter writer =
                new OutputStreamWriter(
                        fs.create(parameterPath, false),
                        StandardCharsets.UTF_8)) {

            writer.write("version=" + VERSION + "\n");
            writer.write("window=" + window + "\n");
            writer.write("minimum.step=" + minStep + "\n");
            writer.write("input=" + inputPath + "\n");
            writer.write("output=" + outputRoot + "\n");
            writer.write("combiner.enabled=false\n");
            writer.write("temporal.rule=steps<s\n");
        }

        Path batchVersion = new Path(batch, VERSION);

        if (fs.exists(batchVersion)) {
            throw new IOException("La version batch existe deja.");
        }

        if (!fs.mkdirs(batch)) {
            throw new IOException("Creation du dossier batch impossible.");
        }

        if (!fs.rename(stagingBatch, batchVersion)) {
            throw new IOException("Publication batch versionnee impossible.");
        }

        long endTime = System.currentTimeMillis();

        long duration = endTime - startTime;

        Path executionFile =
                new Path(quality, "mr2_execution.txt");

        try (OutputStreamWriter writer =
                new OutputStreamWriter(
                        fs.create(executionFile, false),
                        StandardCharsets.UTF_8)) {

            writer.write("duration_ms=" + duration + "\n");
            writer.write("window=" + window + "\n");
            writer.write("version=" + VERSION + "\n");
        }

        System.out.println("====================================");
        System.out.println(" MR2 TERMINE AVEC SUCCES");
        System.out.println("====================================");
        System.out.println("Features : " + features);
        System.out.println("Batch    : " + batchVersion);
        System.out.println("Rejected : " + rejected);
        System.out.println("Quality  : " + quality);
        System.out.println("Duration : " + duration + " ms");

        for (Quality q : Quality.values()) {
            Counter c = job.getCounters().findCounter(q);
            System.out.println(q.name() + " = " + c.getValue());
        }

        fs.close();
    }
}

