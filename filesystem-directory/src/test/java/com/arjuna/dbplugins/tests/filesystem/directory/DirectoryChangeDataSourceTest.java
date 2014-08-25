/*
 * Copyright (c) 2014, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.tests.filesystem.directory;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.Test;
import com.arjuna.dbplugins.filesystem.directory.DirectoryChangeDataSource;

public class DirectoryChangeDataSourceTest
{
    private static final Logger logger = Logger.getLogger(DirectoryChangeDataSourceTest.class.getName());

    @Test
    public void directoryScanner01()
    {
        try
        {
            String              name       = "Directory Change Data Source";
            Map<String, String> properties = new HashMap<String, String>();
            properties.put(DirectoryChangeDataSource.DIRECTORYNAME_PROPERYNAME, "testDir");

            DirectoryChangeDataSource directoryChangeDataSource = new DirectoryChangeDataSource(name, properties);

            Thread.sleep(30000);

            directoryChangeDataSource.finish();
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Problem in 'directoryScanner01'", throwable);
        }
    }
}