package info.debatty.java.datasets.enron;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Stack;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.mail.MessagingException;

public class Dataset extends info.debatty.java.datasets.Dataset<Email> {

    private final String directory;

    public Dataset(String directory) {
        this.directory = directory;
    }

    public Iterator<Email> iterator() {
        return new EnronIterator(directory);
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 89 * hash + (this.directory != null ? this.directory.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Dataset other = (Dataset) obj;
        if ((this.directory == null) ? (other.directory != null) : !this.directory.equals(other.directory)) {
            return false;
        }
        return true;
    }

    

    private static class EnronIterator implements Iterator<Email> {

        private static final int BUFFER_SIZE = 10;

        /**
         * List of folders that can be processed to extract pages. Implemented
         * as a stack to get depth first search processing...
         */
        private final Stack<File> directories = new Stack<File>();

        private final LinkedList<Email> available_emails = new LinkedList<Email>();
        private final LinkedList<File> available_files = new LinkedList<File>();
        private final String root;


        public EnronIterator(String directory) {
            root = directory;
            directories.push(new File(directory));

            // Fill the files buffer
            readNextFiles();

            // Fill the pages buffer
            readNextPages();
        }

        public boolean hasNext() {
            return !available_emails.isEmpty();
        }

        public Email next() {
            Email current = available_emails.removeFirst();
            if (available_emails.isEmpty()) {
                readNextPages();
            }

            return current;
        }

        public void remove() {
            throw new UnsupportedOperationException("Not supported!");
        }

        private void readNextPages() {

            while (available_emails.size() < BUFFER_SIZE) {
                if (available_files.isEmpty()) {
                    return;
                }

                File next_file = available_files.poll();
                try {
                	@SuppressWarnings("unused")
                	String subRoot = root.substring(0, root.lastIndexOf("\\"));
                    available_emails.add(
                            new Email(
                                    readFile(next_file.getPath()),
                                    next_file.getParent().substring(subRoot.length() + 1)));

                } catch (IOException ex) {
                    Logger.getLogger(Dataset.class.getName()).log(Level.SEVERE, null, ex);

                } catch (MessagingException ex) {
                    Logger.getLogger(Dataset.class.getName()).log(Level.SEVERE, null, ex);

                } catch (Exception ex) {
                    Logger.getLogger(Dataset.class.getName()).log(Level.SEVERE, null, ex);
                }

                if (available_files.isEmpty()) {
                    readNextFiles();
                }
            }
        }

        private String readFile(final String file) throws IOException {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line = null;
            StringBuilder string_builder = new StringBuilder();
            String ls = System.getProperty("line.separator");

            try {
                while ((line = reader.readLine()) != null) {
                    string_builder.append(line);
                    string_builder.append(ls);
                }

                return string_builder.toString();
            } finally {
                reader.close();
            }
        }

        private void readNextFiles() {

            while (available_files.isEmpty()) {
                if (directories.empty()) {
                    return;
                }

                File current_folder = directories.pop();

                for (File file : current_folder.listFiles()) {
                    if (file.isDirectory()) {
                        directories.push(file);
                    } else {
                        available_files.add(file);
                    }
                }
            }
        }
    }
}