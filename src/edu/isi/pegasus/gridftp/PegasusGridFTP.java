package edu.isi.pegasus.gridftp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.Filter;

/**
 * Implements a command-line utility for performing file and directory operations
 * on remote GridFTP servers.
 *
 * @author Gideon Juve <juve@usc.edu>
 */
public class PegasusGridFTP {
    private static Log logger = LogFactory.getLog(ListCommand.class);
    
    public static String NAME = "pegasus-gridftp";
    
    public PegasusGridFTP() {
    }
    
    private static void usage() {
        usage(null);
    }
    
    private static void options() {
        System.err.println("Options:");
        System.err.println("  -v        Turn on verbose output");
        System.err.println("  -i FILE   Read URLs from FILE");
    }
    
    private static void usage(String command) {
        if (command == null) {
            System.err.printf("Usage: %s COMMAND\n\n", NAME);
            System.err.printf("Commands:\n" + 
                    "  ls      List remote files and directories\n" +
                    "  rm      Remove remote files and directories\n" +
                    "  mkdir   Create remote directories\n");
        }
        else if ("ls".equals(command)) {
            System.err.printf("Usage: %s ls [options] [URL...]\n\n", NAME);
            options();
            System.err.println("  -a        List all files");
            System.err.println("  -l        Long format");
        }
        else if ("rm".equals(command)) {
            System.err.printf("Usage: %s rm [options] [URL...]\n\n", NAME);
            options();
            System.err.println("  -f        Ignore errors");
            System.err.println("  -r        Recursively delete");
        }
        else if ("mkdir".equals(command)) {
            System.err.printf("Usage: %s mkdir [options] [URL...]\n\n", NAME);
            options();
            System.err.println("  -p        Create intermediate directories");
            System.err.println("  -f        Ignore error if directory exists");
        }
        else {
            System.err.printf("Unknown command: %s\n", command);
        }
    }
    
    private static List<GridFTPURL> readURLs(File file) throws MalformedURLException, IOException {
        List<GridFTPURL> urls = new LinkedList<GridFTPURL>();
        
        int lineno = 0;
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                line = line.trim();
                
                lineno++;
                
                if (line.length() == 0) {
                    continue;
                }
                
                if ('#' == line.charAt(0)) {
                    continue;
                }
                
                urls.add(new GridFTPURL(line));
            }
        } catch (MalformedURLException cause) {
            MalformedURLException e = new MalformedURLException(
                    String.format("Malformed URL at line %d in file %s", 
                            lineno, file.getPath()));
            e.initCause(cause);
            throw e;
        } finally {
            if (reader != null) {
                try {
                    reader.close(); 
                } catch (Exception e) {
                    logger.warn(e);
                }
            }
        }
        
        return urls;
    }
    
    private static void execute(String[] args) throws GridFTPException, MalformedURLException, IOException {
        Logger root = Logger.getRootLogger();
        root.removeAllAppenders();
        root.addAppender(new ConsoleAppender(new PatternLayout("%m%n")));
        root.setLevel(Level.WARN);
        
        // Ignore most logging messages from globus by default
        Logger globus = Logger.getLogger("org.globus");
        globus.setLevel(Level.FATAL);
        
        if (args.length == 0) {
            usage();
            System.exit(1);
        }
        
        String command = args[0];
        
        // Extract all the URLs from the command line and the -f argument
        // This applies to all the commands
        List<String> argv = new LinkedList<String>();
        List<GridFTPURL> urls = new LinkedList<GridFTPURL>();
        for (int i=1; i<args.length; i++) {
            String arg = args[i];
            if (arg.startsWith("gsiftp://")) {
                urls.add(new GridFTPURL(arg));
            } else if ("-i".equals(arg)) {
                i++;
                if (i<args.length) {
                    File file = new File(args[i]);
                    urls.addAll(readURLs(file));
                } else {
                    System.err.println("-i argument requires FILE");
                    System.exit(1);
                }
            } else if ("-v".equals(arg)) {
                if (root.getLevel() == Level.WARN) {
                    root.setLevel(Level.INFO);
                    globus.setLevel(Level.ERROR);
                } else {
                    root.setLevel(Level.DEBUG);
                    globus.setLevel(Level.WARN);
                }
            } else {
                argv.add(arg);
            }
        }
        
        // Can't do anything if they didn't specify any URLs
        if (urls.size() == 0) {
            usage(command);
            System.exit(1);
        }
        
        Command cmd = null;
        if ("ls".equals(command)) {
            cmd = ListCommand.fromArguments(argv);
        } else if ("rm".equals(command)) {
            cmd = RemoveCommand.fromArguments(argv);
        } else if ("mkdir".equals(command)) {
            cmd = MakeDirectoryCommand.fromArguments(argv);
        } else {
            throw new IllegalArgumentException("Unknown command: "+command);
        }
        
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Executing command '%s' on %d urls", 
                    command, urls.size()));
        }
        
        cmd.execute(urls);
    }
    
    public static void main(String[] args) {
        try {
            execute(args);
            System.exit(0);
        } catch(Exception e) {
            if (logger.isInfoEnabled()) {
                System.err.printf("%s failed\n", args[0]);
                e.printStackTrace();
            } else {
                System.err.printf("%s: %s\n", args[0], e.getMessage());
            }
            System.exit(1);
        }
    }
}
