/*
 * Copyright (c) 2014, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.filesystem.file;

import java.io.File;
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

public class FileChangeDataSource implements DataSource
{
    private static final Logger logger = Logger.getLogger(FileChangeDataSource.class.getName());

    public FileChangeDataSource(String name, Map<String, String> properties)
    {
        logger.log(Level.FINE, "FileChangeDataSource: " + name + ", " + properties);

        _name       = name;
        _properties = properties;

        _dataProvider = new BasicDataProvider<File>(this);
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

    private String              _name;
    private Map<String, String> _properties;
    private DataProvider<File>  _dataProvider;
}