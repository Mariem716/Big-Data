import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MR1 {

    // =========================================================
    // 1. PARAMETRES GENERAUX
    // =========================================================

    private static final String INPUT_PATH =
            "/p02/raw/history";

    private static final String OUTPUT_ROOT =
            "/p02/mr1";

    private static final String ACCOUNT_OUTPUT =
            OUTPUT_ROOT + "/account_step";

    private static final String REJECTED_OUTPUT =
            OUTPUT_ROOT + "/rejected";

    private static final String QUALITY_OUTPUT =
            OUTPUT_ROOT + "/quality";

    private static final String STAGING_OUTPUT =
            OUTPUT_ROOT + "/_mr1_staging";

    private static final String REJECTED_MARKER =
            "!REJECTED!";

    private static final String[] TYPES = {
            "PAYMENT",
            "TRANSFER",
            "CASH_OUT",
            "DEBIT",
            "CASH_IN",
            "OTHER"
    };

    // =========================================================
    // 2. COMPTEURS DE QUALITE
    // =========================================================

    public enum Quality {
        HEADER,
        EMPTY,
        VALID,
        REJECTED,
        BAD_COLUMNS,
        BAD_STEP,
        BAD_ACCOUNT,
        BAD_AMOUNT,
        UNKNOWN_TYPE,
        NEGATIVE_AMOUNT,
        ATYPICAL,
        UNKNOWN_SUBSET
    }

    // =========================================================
    // 3. CLE COMPOSITE : (nameOrig, step)
    // =========================================================

    public static class AccountStepKey
            implements WritableComparable<AccountStepKey> {

        private Text nameOrig;
        private IntWritable step;

        public AccountStepKey() {
            this.nameOrig = new Text();
            this.step = new IntWritable();
        }

        public AccountStepKey(String nameOrig, int step) {
            this.nameOrig = new Text(nameOrig);
            this.step = new IntWritable(step);
        }

        public String getNameOrig() {
            return nameOrig.toString();
        }

        public int getStep() {
            return step.get();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            nameOrig.write(out);
            step.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            nameOrig.readFields(in);
            step.readFields(in);
        }

        @Override
        public int compareTo(AccountStepKey other) {
            int result = nameOrig.compareTo(other.nameOrig);

            if (result != 0) {
                return result;
            }

            return step.compareTo(other.step);
        }

        @Override
        public int hashCode() {
            return nameOrig.hashCode() * 163 + step.hashCode();
        }

        @Override
        public boolean equals(Object object) {
            if (object instanceof AccountStepKey) {
                AccountStepKey other = (AccountStepKey) object;

                return nameOrig.equals(other.nameOrig)
                        && step.equals(other.step);
            }

            return false;
        }

        @Override
        public String toString() {
            return nameOrig.toString() + "\t" + step.get();
        }
    }

    // =========================================================
    // 4. VALEUR STRUCTUREE
    // =========================================================

    public static class TransactionValue implements Writable {

        private IntWritable count;
        private Text type;
        private Text subset;
        private Text reason;
        private Text originalLine;

        private double amount;
        private double maxCandidate;
        private boolean rejected;

        public TransactionValue() {
            count = new IntWritable();
            type = new Text();
            subset = new Text();
            reason = new Text();
            originalLine = new Text();

            amount = 0.0;
            maxCandidate = 0.0;
            rejected = false;
        }

        public TransactionValue(
                double amount,
                String type,
                String subset) {

            this();

            this.count.set(1);
            this.amount = amount;
            this.maxCandidate = amount;
            this.type.set(type);
            this.subset.set(subset);
            this.rejected = false;
        }

        public static TransactionValue rejected(
                String reason,
                String originalLine) {

            TransactionValue value = new TransactionValue();

            value.rejected = true;
            value.reason.set(reason);
            value.originalLine.set(originalLine);

            return value;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            count.write(out);
            type.write(out);
            subset.write(out);
            reason.write(out);
            originalLine.write(out);

            out.writeDouble(amount);
            out.writeDouble(maxCandidate);
            out.writeBoolean(rejected);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            count.readFields(in);
            type.readFields(in);
            subset.readFields(in);
            reason.readFields(in);
            originalLine.readFields(in);

            amount = in.readDouble();
            maxCandidate = in.readDouble();
            rejected = in.readBoolean();
        }

        public int getCount() {
            return count.get();
        }

        public double getAmount() {
            return amount;
        }

        public double getMaxCandidate() {
            return maxCandidate;
        }

        public String getType() {
            return type.toString();
        }

        public String getSubset() {
            return subset.toString();
        }

        public String getReason() {
            return reason.toString();
        }

        public String getOriginalLine() {
            return originalLine.toString();
        }

        public boolean isRejected() {
            return rejected;
        }
    }

    // =========================================================
    // 5. MAPPER
    // =========================================================

    public static class MR1Mapper
            extends Mapper<Object, Text,
                           AccountStepKey, TransactionValue> {

        private String subset;

        @Override
        protected void setup(Context context) throws IOException {

            FileSplit split = (FileSplit) context.getInputSplit();

            String filename = split.getPath()
                    .getName()
                    .toLowerCase(Locale.ROOT);

            if (filename.contains("train")) {
                subset = "train";
            } else if (filename.contains("validation")) {
                subset = "validation";
            } else {
                subset = "unknown";
            }
        }

        private void reject(
                Context context,
                String reason,
                String line) throws IOException, InterruptedException {

            context.getCounter(Quality.REJECTED).increment(1);

            if ("BAD_COLUMNS".equals(reason)) {
                context.getCounter(Quality.BAD_COLUMNS).increment(1);
            } else if ("BAD_STEP".equals(reason)) {
                context.getCounter(Quality.BAD_STEP).increment(1);
            } else if ("BAD_ACCOUNT".equals(reason)) {
                context.getCounter(Quality.BAD_ACCOUNT).increment(1);
            } else if ("BAD_AMOUNT".equals(reason)) {
                context.getCounter(Quality.BAD_AMOUNT).increment(1);
            } else if ("UNKNOWN_SUBSET".equals(reason)) {
                context.getCounter(Quality.UNKNOWN_SUBSET).increment(1);
            }

            AccountStepKey rejectedKey =
                    new AccountStepKey(REJECTED_MARKER, -1);

            TransactionValue rejectedValue =
                    TransactionValue.rejected(reason, line);

            context.write(rejectedKey, rejectedValue);
        }

        @Override
        protected void map(
                Object key,
                Text value,
                Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            // Ignorer les lignes vides
            if (line.trim().isEmpty()) {
                context.getCounter(Quality.EMPTY).increment(1);
                return;
            }

            // Ignorer les en-tetes CSV
            if (line.toLowerCase(Locale.ROOT)
                    .startsWith("step,type,amount,nameorig,")) {

                context.getCounter(Quality.HEADER).increment(1);
                return;
            }

            // Verifier le nombre de colonnes
            String[] columns = line.split(",", -1);

            if (columns.length != 11) {
                reject(context, "BAD_COLUMNS", line);
                return;
            }

            // Extraire uniquement les champs necessaires
            String stepText = columns[0].trim();
            String typeText = columns[1].trim().toUpperCase(Locale.ROOT);
            String amountText = columns[2].trim();
            String nameOrig = columns[3].trim();

            // Verifier le step
            int step;

            try {
                step = Integer.parseInt(stepText);

                if (step <= 0) {
                    reject(context, "BAD_STEP", line);
                    return;
                }

            } catch (NumberFormatException e) {
                reject(context, "BAD_STEP", line);
                return;
            }

            // Verifier le compte expediteur
            if (nameOrig.isEmpty()
                    || nameOrig.contains("\t")
                    || nameOrig.contains(" ")) {

                reject(context, "BAD_ACCOUNT", line);
                return;
            }

            // Verifier le montant
            double amount;

            try {
                amount = Double.parseDouble(amountText);

                if (Double.isNaN(amount)
                        || Double.isInfinite(amount)) {

                    reject(context, "BAD_AMOUNT", line);
                    return;
                }

            } catch (NumberFormatException e) {
                reject(context, "BAD_AMOUNT", line);
                return;
            }

            // Verifier le subset correspondant au fichier
            if (!"train".equals(subset)
                    && !"validation".equals(subset)) {

                reject(context, "UNKNOWN_SUBSET", line);
                return;
            }

            // Identifier les types connus
            boolean knownType = false;

            for (int i = 0; i < TYPES.length - 1; i++) {
                if (TYPES[i].equals(typeText)) {
                    knownType = true;
                    break;
                }
            }

            boolean atypical = false;

            if (!knownType) {
                typeText = "OTHER";

                context.getCounter(Quality.UNKNOWN_TYPE)
                        .increment(1);

                atypical = true;
            }

            // Les montants negatifs sont conserves
            if (amount < 0) {
                context.getCounter(Quality.NEGATIVE_AMOUNT)
                        .increment(1);

                atypical = true;
            }

            if (atypical) {
                context.getCounter(Quality.ATYPICAL)
                        .increment(1);
            }

            // Construire la cle exacte (nameOrig, step)
            AccountStepKey outputKey =
                    new AccountStepKey(nameOrig, step);

            // Le subset est conserve dans la valeur
            TransactionValue outputValue =
                    new TransactionValue(amount, typeText, subset);

            context.write(outputKey, outputValue);

            context.getCounter(Quality.VALID).increment(1);
        }
    }

    // =========================================================
    // 6. STRUCTURE D'AGREGATION
    // =========================================================

    public static class Aggregate {

        private long count;
        private double sum;
        private double max;
        private long[] histogram;

        public Aggregate() {
            count = 0;
            sum = 0.0;
            max = Double.NEGATIVE_INFINITY;
            histogram = new long[TYPES.length];
        }

        public void add(TransactionValue value) {

            count += value.getCount();

            sum += value.getAmount();

            if (value.getMaxCandidate() > max) {
                max = value.getMaxCandidate();
            }

            String type = value.getType();

            for (int i = 0; i < TYPES.length; i++) {
                if (TYPES[i].equals(type)) {
                    histogram[i]++;
                    break;
                }
            }
        }

        public String toOutput(
                String nameOrig,
                int step,
                String subset) {

            double average = count == 0 ? 0.0 : sum / count;

            StringBuilder result = new StringBuilder();

            result.append(nameOrig).append("\t");
            result.append(step).append("\t");
            result.append(subset).append("\t");
            result.append(count).append("\t");
            result.append(String.format(Locale.US, "%.2f", sum)).append("\t");
            result.append(String.format(Locale.US, "%.6f", average)).append("\t");
            result.append(String.format(Locale.US, "%.2f", max));

            for (int i = 0; i < TYPES.length; i++) {
                result.append("\t").append(histogram[i]);
            }

            return result.toString();
        }
    }

    // =========================================================
    // 7. REDUCER
    // =========================================================

    public static class MR1Reducer
            extends Reducer<AccountStepKey, TransactionValue,
                            NullWritable, Text> {

        private MultipleOutputs<NullWritable, Text> outputs;

        @Override
        protected void setup(Context context) {
            outputs = new MultipleOutputs<>(context);
        }

        @Override
        protected void reduce(
                AccountStepKey key,
                Iterable<TransactionValue> values,
                Context context)
                throws IOException, InterruptedException {

            // Cas des lignes rejetees
            if (REJECTED_MARKER.equals(key.getNameOrig())
                    && key.getStep() == -1) {

                for (TransactionValue value : values) {

                    String rejectedRow =
                            value.getReason()
                            + "\t"
                            + value.getOriginalLine();

                    outputs.write(
                            "rejected",
                            NullWritable.get(),
                            new Text(rejectedRow),
                            "rejected/part"
                    );
                }

                return;
            }

            // Agregation separee pour train et validation
            Aggregate trainAggregate = new Aggregate();
            Aggregate validationAggregate = new Aggregate();

            boolean hasTrain = false;
            boolean hasValidation = false;

            for (TransactionValue value : values) {

                if (value.isRejected()) {
                    continue;
                }

                if ("train".equals(value.getSubset())) {
                    trainAggregate.add(value);
                    hasTrain = true;

                } else if ("validation".equals(value.getSubset())) {
                    validationAggregate.add(value);
                    hasValidation = true;
                }
            }

            // Ecrire la ligne train si elle existe
            if (hasTrain) {

                String row = trainAggregate.toOutput(
                        key.getNameOrig(),
                        key.getStep(),
                        "train"
                );

                outputs.write(
                        "accountStep",
                        NullWritable.get(),
                        new Text(row),
                        "account_step/part"
                );
            }

            // Ecrire la ligne validation si elle existe
            if (hasValidation) {

                String row = validationAggregate.toOutput(
                        key.getNameOrig(),
                        key.getStep(),
                        "validation"
                );

                outputs.write(
                        "accountStep",
                        NullWritable.get(),
                        new Text(row),
                        "account_step/part"
                );
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            outputs.close();
        }
    }

    // =========================================================
    // 8. GENERATION DU FICHIER SCHEMA
    // =========================================================

    private static void generateSchema(FileSystem fs)
            throws IOException {

        Path schemaPath =
                new Path(QUALITY_OUTPUT + "/mr1_schema.txt");

        try (FSDataOutputStream stream = fs.create(schemaPath, true);
             OutputStreamWriter writer =
                     new OutputStreamWriter(stream, StandardCharsets.UTF_8)) {

            writer.write("nameOrig\n");
            writer.write("step\n");
            writer.write("subset\n");
            writer.write("nbTransactions\n");
            writer.write("sommeMontants\n");
            writer.write("montantMoyen\n");
            writer.write("montantMaximum\n");
            writer.write("nb_PAYMENT\n");
            writer.write("nb_TRANSFER\n");
            writer.write("nb_CASH_OUT\n");
            writer.write("nb_DEBIT\n");
            writer.write("nb_CASH_IN\n");
            writer.write("nb_OTHER\n");
        }
    }

    // =========================================================
    // 9. GENERATION DU FICHIER PARAMETRES
    // =========================================================

    private static void generateParameters(
            FileSystem fs,
            org.apache.hadoop.mapreduce.Counters counters)
            throws IOException {

        Path parametersPath =
                new Path(QUALITY_OUTPUT + "/mr1_parameters.properties");

        try (FSDataOutputStream stream =
                     fs.create(parametersPath, true);

             OutputStreamWriter writer =
                     new OutputStreamWriter(stream, StandardCharsets.UTF_8)) {

            writer.write("mr1.version=1.0\n");
            writer.write("input.path=" + INPUT_PATH + "\n");
            writer.write("output.accountstep=" + ACCOUNT_OUTPUT + "\n");
            writer.write("output.rejected=" + REJECTED_OUTPUT + "\n");
            writer.write("aggregation.key=nameOrig,step\n");
            writer.write("csv.columns=11\n");
            writer.write("csv.separator=,\n");
            writer.write("output.format=TSV\n");
            writer.write("combiner.enabled=false\n");
            writer.write("reducers=1\n");

            writer.write("\n");
            writer.write("# Atypical transaction policy\n");
            writer.write("negative.amount.policy=retain_and_count\n");
            writer.write("unknown.type.policy=retain_as_OTHER_and_count\n");
            writer.write("subset.location=value_not_key\n");

            writer.write("\n");
            writer.write("# Quality counters\n");

            for (Quality quality : Quality.values()) {

                Counter counter = counters.findCounter(quality);

                writer.write(
                        "counter."
                        + quality.name()
                        + "="
                        + counter.getValue()
                        + "\n"
                );
            }
        }
    }

    // =========================================================
    // 10. PUBLICATION DES RESULTATS HDFS
    // =========================================================

    private static void publishResults(FileSystem fs)
            throws IOException {

        Path stagingRoot = new Path(STAGING_OUTPUT);

        Path stagingAccount =
                new Path(STAGING_OUTPUT + "/account_step");

        Path stagingRejected =
                new Path(STAGING_OUTPUT + "/rejected");

        Path finalAccount =
                new Path(ACCOUNT_OUTPUT);

        Path finalRejected =
                new Path(REJECTED_OUTPUT);

        // Remplacer les anciens resultats
        if (fs.exists(finalAccount)) {
            fs.delete(finalAccount, true);
        }

        if (fs.exists(finalRejected)) {
            fs.delete(finalRejected, true);
        }

        // Publier account_step
        if (fs.exists(stagingAccount)) {

            if (!fs.rename(stagingAccount, finalAccount)) {
                throw new IOException(
                        "Impossible de publier account_step."
                );
            }

        } else {
            fs.mkdirs(finalAccount);
        }

        // Publier rejected
        if (fs.exists(stagingRejected)) {

            if (!fs.rename(stagingRejected, finalRejected)) {
                throw new IOException(
                        "Impossible de publier rejected."
                );
            }

        } else {
            fs.mkdirs(finalRejected);
        }

        // Supprimer le dossier temporaire restant
        if (fs.exists(stagingRoot)) {
            fs.delete(stagingRoot, true);
        }
    }

    // =========================================================
    // 11. MAIN : CONFIGURATION ET EXECUTION DU JOB
    // =========================================================

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Path input = new Path(INPUT_PATH);
        Path outputRoot = new Path(OUTPUT_ROOT);
        Path staging = new Path(STAGING_OUTPUT);
        Path quality = new Path(QUALITY_OUTPUT);

        FileSystem fs = input.getFileSystem(conf);

        // Verifier que le dossier d'entree existe
        if (!fs.exists(input)) {
            throw new IOException(
                    "Dossier d'entree introuvable : " + INPUT_PATH
            );
        }

        // Creer les dossiers necessaires
        fs.mkdirs(outputRoot);
        fs.mkdirs(quality);

        // Nettoyer uniquement le dossier temporaire
        if (fs.exists(staging)) {
            fs.delete(staging, true);
        }

        Job job = Job.getInstance(conf, "MR1_PaySim_AccountStep");

        job.setJarByClass(MR1.class);

        // Mapper et Reducer
        job.setMapperClass(MR1Mapper.class);
        job.setReducerClass(MR1Reducer.class);

        // Types de sortie du Mapper
        job.setMapOutputKeyClass(AccountStepKey.class);
        job.setMapOutputValueClass(TransactionValue.class);

        // Types de sortie du Reducer
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        // Un seul Reducer
        job.setNumReduceTasks(1);

        // Aucun Combiner
        // combiner.enabled=false

        // Sorties nommees
        MultipleOutputs.addNamedOutput(
                job,
                "accountStep",
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

        // Le Reducer utilise MultipleOutputs
        job.setOutputFormatClass(TextOutputFormat.class);

        // Dossier temporaire utilise par MultipleOutputs
        FileOutputFormat.setOutputPath(job, staging);

        // Dossier contenant train et validation
        FileInputFormat.addInputPath(job, input);

        System.out.println("======================================");
        System.out.println(" DEMARRAGE DU JOB MR1 PAYSim");
        System.out.println("======================================");
        System.out.println("Input  : " + INPUT_PATH);
        System.out.println("Output : " + OUTPUT_ROOT);
        System.out.println("Cle    : (nameOrig, step)");
        System.out.println("Subset : conserve dans la valeur");
        System.out.println("Reducers : 1");
        System.out.println("Combiner : desactive");
        System.out.println("======================================");

        boolean success = job.waitForCompletion(true);

        if (!success) {

            if (fs.exists(staging)) {
                fs.delete(staging, true);
            }

            throw new RuntimeException("Le job MR1 a echoue.");
        }

        // Recuperer les compteurs
        org.apache.hadoop.mapreduce.Counters counters =
                job.getCounters();

        // Publier les resultats
        publishResults(fs);

        // Generer les fichiers de qualite
        generateSchema(fs);
        generateParameters(fs, counters);

        System.out.println();
        System.out.println("======================================");
        System.out.println(" MR1 TERMINE AVEC SUCCES");
        System.out.println("======================================");

        System.out.println("Sortie agregee : " + ACCOUNT_OUTPUT);
        System.out.println("Sortie rejetee : " + REJECTED_OUTPUT);
        System.out.println("Schema         : "
                + QUALITY_OUTPUT + "/mr1_schema.txt");
        System.out.println("Parametres     : "
                + QUALITY_OUTPUT + "/mr1_parameters.properties");

        System.out.println();
        System.out.println("COMPTEURS DE QUALITE");

        for (Quality qualityCounter : Quality.values()) {

            Counter counter =
                    counters.findCounter(qualityCounter);

            System.out.println(
                    qualityCounter.name()
                    + " = "
                    + counter.getValue()
            );
        }

        System.out.println("======================================");

        fs.close();
    }
}
