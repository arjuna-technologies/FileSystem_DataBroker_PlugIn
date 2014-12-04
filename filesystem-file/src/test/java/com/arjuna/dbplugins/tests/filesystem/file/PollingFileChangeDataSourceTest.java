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
import com.arjuna.databroker.data.connector.ObservableDataProvider;
import com.arjuna.databroker.data.connector.ObserverDataConsumer;
import com.arjuna.databroker.data.jee.DataFlowNodeLifeCycleControl;
import com.arjuna.dbplugins.filesystem.file.PollingFileChangeDataSource;
import com.arjuna.dbutilities.testsupport.dataflownodes.dummy.DummyDataSink;

public class PollingFileChangeDataSourceTest
{
    private static final Logger logger = Logger.getLogger(PollingFileChangeDataSourceTest.class.getName());

    @Test
    public void fileScanner01()
    {
        try
        {
            File testDirectory = createTemporaryDirectory("Scanner01");

            String              name       = "Polling File Change Data Source";
            Map<String, String> properties = new HashMap<String, String>();
            properties.put(PollingFileChangeDataSource.FILENAME_PROPERYNAME, testDirectory.toString() + File.separator + "Test02");
            properties.put(PollingFileChangeDataSource.POLLINTERVAL_PROPERYNAME, "1000");

            PollingFileChangeDataSource pollingFileChangeDataSource = new PollingFileChangeDataSource(name, properties);
            DummyDataSink               dummyDataSink               = new DummyDataSink("Dummy Data Sink", Collections.<String, String>emptyMap());

            DataFlowNodeLifeCycleControl.processCreatedDataFlowNode(pollingFileChangeDataSource, null);

            ((ObservableDataProvider<File>) pollingFileChangeDataSource.getDataProvider(File.class)).addDataConsumer((ObserverDataConsumer<File>) dummyDataSink.getDataConsumer(File.class));

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

            pollingFileChangeDataSource.finish();

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
