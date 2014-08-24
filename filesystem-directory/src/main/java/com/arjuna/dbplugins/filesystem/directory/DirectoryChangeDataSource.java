/*
 * Copyright (c) 2014, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.filesystem.directory;

import java.io.File;
import java.nio.file.WatchKey;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.risbic.intraconnect.basic.BasicDataProvider;

import com.arjuna.databroker.data.DataProvider;
import com.arjuna.databroker.data.DataSource;

public class DirectoryChangeDataSource implements DataSource
{
    private static final Logger logger = Logger.getLogger(DirectoryChangeDataSource.class.getName());

    private static final String DIRECTORYNAME_PROPERYNAME = "Directory Name";

    public DirectoryChangeDataSource(String name, Map<String, String> properties)
    {
        logger.log(Level.FINE, "DirectoryDataSource: " + name + ", " + properties);

        _name       = name;
        _properties = properties;

        _dataProvider = new BasicDataProvider<File>(this);

        _watcher = new Watcher(new File(_properties.get(DIRECTORYNAME_PROPERYNAME)));
        _watcher.start();
    }

    @Override
    public String getName()
    {
        return _name;
    }

    @Override
    public Map<String, String> getProperties()
    {
        return Collections.unmodifiableMap(_properties);
    }

    @Override
    public Collection<Class<?>> getDataProviderDataClasses()
    {
        Set<Class<?>> dataProviderDataClasses = new HashSet<Class<?>>();

        dataProviderDataClasses.add(File.class);

        return dataProviderDataClasses;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> DataProvider<T> getDataProvider(Class<T> dataClass)
    {
        if (dataClass == File.class)
            return (DataProvider<T>) _dataProvider;
        else
            return null;
    }

    private class Watcher extends Thread
    {
    	public Watcher(File directory)
    	{
    		_directory = directory;
    	}

    	@Override
    	public void run()
    	{
    		try
    		{
    			WatchKey key = dir.register(watcher, ENTRY_CREATE, ENTRY_MODIFY);
    		}
    		catch (Throwable throwable)
    		{
    			logger.log(Level.WARNING, "Problem while watching directory", throwable);
    		}
    	}

    	private File _directory;
    }

    private String              _name;
    private Map<String, String> _properties;
    private DataProvider<File>  _dataProvider;

    private Watcher _watcher;
}