/*
 * Copyright (c) 2014, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.tests.filesystem.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.Test;
import com.arjuna.dbplugins.filesystem.file.FileChangeDataSource;
import com.arjuna.dbutilities.testsupport.dataflownodes.dummy.DummyDataSink;

public class FileChangeDataSourceTest
{
    private static final Logger logger = Logger.getLogger(FileChangeDataSourceTest.class.getName());

    @Test
    public void fileScanner01()
    {
        try
        {
            File testDirectory = createTemporaryDirectory("Scanner01");

            String              name       = "File Change Data Source";
            Map<String, String> properties = new HashMap<String, String>();
            properties.put(FileChangeDataSource.FILENAME_PROPERYNAME, testDirectory.toString() + File.separator + "Test02");

            FileChangeDataSource      fileChangeDataSource = new FileChangeDataSource(name, properties);
            DummyDataSink             dummyDataSink        = new DummyDataSink("Dummy Data Sink", Collections.<String, String>emptyMap());
            fileChangeDataSource.getDataProvider(File.class).addDataConsumer(dummyDataSink.getDataConsumer(File.class));

            Thread.sleep(1000);

            File testFile1 = new File(testDirectory, "Test01");
            testFile1.createNewFile();
            Thread.sleep(1000);

            File testFile2 = new File(testDirectory, "Test02");
            testFile2.createNewFile();
            Thread.sleep(1000);

            File testFile3 = new File(testDirectory, "Test03");
            testFile3.createNewFile();
            Thread.sleep(1000);

            fileChangeDataSource.finish();

            assertEquals("Incorrect message number", 1, dummyDataSink.receivedData().size());
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Problem in 'fileScanner01'", throwable);

            fail("Problem in 'fileScanner01': " + throwable);
        }
    }

    private File createTemporaryDirectory(String uName)
        throws IOException
    {
        File temporaryDirectory = Files.createTempDirectory(uName).toFile();

        temporaryDirectory.deleteOnExit();

        return temporaryDirectory;
    }
}