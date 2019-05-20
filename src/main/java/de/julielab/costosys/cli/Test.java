package de.julielab.costosys.cli;

import java.io.*;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class Test {
    public static void main(String args[]) throws IOException {
        try (FileSystem fs = FileSystems.newFileSystem(Paths.get("myfs.zip"), null)) {

            OutputStream os = fs.provider().newOutputStream(fs.getPath("mow/entry.txt"), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os))) {
                bw.write("Here is content!");
            }
        }
        System.out.println("Done");
    }
}
