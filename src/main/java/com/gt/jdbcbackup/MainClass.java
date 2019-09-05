package com.gt.jdbcbackup;

import com.gt.jdbcbackup.db.Backup;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.CodeSource;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;

public class MainClass {

    static {
        System.setProperty("java.util.logging.manager", "org.apache.logging.log4j.jul.LogManager");
    }

    static final org.apache.logging.log4j.Logger LOG = LogManager.getLogger(MainClass.class);

    public static void main(String[] args) throws SQLException, ClassNotFoundException, FileNotFoundException, IOException {

        configLog4j();

        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = null;
        try {
            commandLine = parser.parse(MainClass.buildOptions(), args);
        } catch (ParseException ex) {
            Logger.getLogger(MainClass.class.getName()).log(Level.SEVERE, "Error al leer las opciones", ex);
            return;
        }

        if (commandLine.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Ayuda de jdbcbackup", MainClass.buildOptions());
        } else {

            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmss");

            Backup backup = new Backup();

            backup.toZipFile(commandLine.getOptionValue("d"), commandLine.getOptionValue("j"), commandLine.getOptionValue("u"), commandLine.getOptionValue("p"), commandLine.getOptionValue("file") + sdf.format(new Date()));
        }
    }

    public static Options buildOptions() {

        Options options = new Options();

        options.addOption(Option.builder("h").longOpt("help").argName("comando").build());

        options.addOption(Option.builder("d").longOpt("driverclass").hasArg().desc("Clase java del driver jdbc").build());
        options.addOption(Option.builder("j").longOpt("jdbc").hasArg().desc("jdbc connection string").build());
        options.addOption(Option.builder("u").longOpt("uid").hasArg().desc("Nombre de usuario").build());
        options.addOption(Option.builder("p").longOpt("pass").hasArg().desc("contraseña").build());
        options.addOption(Option.builder("f").longOpt("file").hasArg().desc("base de datos").build());

        return options;
    }

    public static String getJarPath() {
        CodeSource codeSource = MainClass.class.getProtectionDomain().getCodeSource();

        String jarDir = "";
        try {
            File jarFile = new File(codeSource.getLocation().toURI().getPath());
            jarDir = jarFile.getParentFile().getPath();
        } catch (URISyntaxException ex) {
            java.util.logging.Logger.getLogger(MainClass.class.getName()).log(java.util.logging.Level.SEVERE, "Error buscando jar path", ex);
        }
        return jarDir;
    }

    private static void configLog4j() {
        configLog4j(null);
    }

    private static void configLog4j(String logFile) {
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        if (logFile == null) {
            logFile = getJarPath() + "/log4j2.xml";
        }
        Path path = Paths.get(logFile, new String[0]);

        Boolean noExiste = false;

        File log4jConfigFile;

        if (Files.exists(path, new LinkOption[0])) {
            log4jConfigFile = path.toFile();
        } else {
            noExiste = true;
            log4jConfigFile = new File("/home/prog/java_mvn/jdbcbackup/src/main/resources/log4j2.xml");
        }
        
        
        URI uri = null;
        
        if(log4jConfigFile.exists()) {
            uri = log4jConfigFile.toURI();
        } else {
            try {
                uri = MainClass.class.getClassLoader().getResource("log4j2.xml").toURI();
            } catch (URISyntaxException ex) {
                Logger.getLogger(MainClass.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        
        context.setConfigLocation(uri);

        if (noExiste) {
            LOG.log(org.apache.logging.log4j.Level.WARN, "el archivo " + logFile + " no existe");
        }
    }
}
