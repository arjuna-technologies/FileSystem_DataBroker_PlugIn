/*
 * Copyright (c) 2014-2015, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.tests.filesystem.directory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.Test;
import static org.junit.Assert.*;
import com.arjuna.databroker.data.connector.ObservableDataProvider;
import com.arjuna.databroker.data.connector.ObserverDataConsumer;
import com.arjuna.databroker.data.core.jee.DataFlowNodeLifeCycleControl;
import com.arjuna.dbplugins.filesystem.directory.DirectoryChangeDataSource;
import com.arjuna.dbutilities.testsupport.dataflownodes.dummy.DummyDataSink;

public class DirectoryChangeDataSourceTest
{
    private static final Logger logger = Logger.getLogger(DirectoryChangeDataSourceTest.class.getName());

    @Test
    public void directoryScanner01()
    {
        try
        {
            File testDirectory = createTemporaryDirectory("Scanner01");

            String              name       = "Directory Change Data Source";
            Map<String, String> properties = new HashMap<String, String>();
            properties.put(DirectoryChangeDataSource.DIRECTORYNAME_PROPERYNAME, testDirectory.toString());

            DirectoryChangeDataSource directoryChangeDataSource = new DirectoryChangeDataSource(name, properties);
            DummyDataSink             dummyDataSink             = new DummyDataSink("Dummy Data Sink", Collections.<String, String>emptyMap());

            DataFlowNodeLifeCycleControl.processCreatedDataFlowNode(directoryChangeDataSource, null);

            ((ObservableDataProvider<File>) directoryChangeDataSource.getDataProvider(File.class)).addDataConsumer((ObserverDataConsumer<File>) dummyDataSink.getDataConsumer(File.class));

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

            directoryChangeDataSource.finish();

            assertEquals("Incorrect message number", 3, dummyDataSink.receivedData().size());
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Problem in 'directoryScanner01'", throwable);
            fail("Problem in 'directoryScanner01': " + throwable);
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
