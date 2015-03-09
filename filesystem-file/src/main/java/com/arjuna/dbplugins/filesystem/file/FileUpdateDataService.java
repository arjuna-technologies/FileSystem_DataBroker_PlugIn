/*
 * Copyright (c) 2014-2015, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.filesystem.file;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.arjuna.databroker.data.DataFlow;
import com.arjuna.databroker.data.DataConsumer;
import com.arjuna.databroker.data.DataProvider;
import com.arjuna.databroker.data.DataService;
import com.arjuna.databroker.data.jee.annotation.DataConsumerInjection;
import com.arjuna.databroker.data.jee.annotation.DataProviderInjection;
import com.arjuna.databroker.data.jee.annotation.PostConfig;
import com.arjuna.databroker.data.jee.annotation.PostCreated;

public class FileUpdateDataService implements DataService
{
    private static final Logger logger = Logger.getLogger(FileUpdateDataService.class.getName());

    public static final String FILENAME_PROPERYNAME = "File Name";

    public FileUpdateDataService()
    {
        logger.log(Level.FINE, "FileUpdateDataService");

        _file = null;
    }

    public FileUpdateDataService(String name, Map<String, String> properties)
    {
        logger.log(Level.FINE, "FileUpdateDataService: " + name + ", " + properties);

        _name       = name;
        _properties = properties;
        
        _file = null;
    }

    @Override
    public DataFlow getDataFlow()
    {
        return _dataFlow;
    }

    @Override
    public void setDataFlow(DataFlow dataFlow)
    {
        _dataFlow = dataFlow;
    }

    @Override
    public String getName()
    {
        return _name;
    }

    @Override
    public void setName(String name)
    {
        _name = name;
    }

    @Override
    public Map<String, String> getProperties()
    {
        return Collections.unmodifiableMap(_properties);
    }

    @Override
    public void setProperties(Map<String, String> properties)
    {
        _properties = properties;
    }

    @PostCreated
    @PostConfig
    public void config()
    {
        try
        {
            _file = new File(_properties.get(FILENAME_PROPERYNAME));
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Problem configuring: " + _properties, throwable);
        }
    }

    public void update(String data)
    {
        try
        {
            if (_file != null)
            {
                FileOutputStream fileOutputStream = new FileOutputStream(_file);
                fileOutputStream.write(data.getBytes());
                fileOutputStream.close();
            }
            else
                logger.log(Level.WARNING, "Problem writing data, file 'null'");
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Problem while updating file: " + _file, throwable);
        }
    }

    @Override
    public Collection<Class<?>> getDataConsumerDataClasses()
    {
        Set<Class<?>> dataConsumerDataClasses = new HashSet<Class<?>>();

        dataConsumerDataClasses.add(String.class);
        dataConsumerDataClasses.add(byte[].class);

        return dataConsumerDataClasses;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> DataConsumer<T> getDataConsumer(Class<T> dataClass)
    {
        if (String.class.isAssignableFrom(dataClass))
            return (DataConsumer<T>) _dataConsumerString;
        else if (byte[].class.isAssignableFrom(dataClass))
            return (DataConsumer<T>) _dataConsumerBytes;
        else
            return null;
    }

    @Override
    public Collection<Class<?>> getDataProviderDataClasses()
    {
        Set<Class<?>> dataProviderDataClasses = new HashSet<Class<?>>();

        dataProviderDataClasses.add(String.class);

        return dataProviderDataClasses;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> DataProvider<T> getDataProvider(Class<T> dataClass)
    {
        if (String.class.isAssignableFrom(dataClass))
            return (DataProvider<T>) _dataProvider;
        else
            return null;
    }

    private File _file;
    
    private DataFlow             _dataFlow;
    private String               _name;
    private Map<String, String>  _properties;
    @DataConsumerInjection(methodName="updateString")
    private DataConsumer<String> _dataConsumerString;
    @DataConsumerInjection(methodName="updateBytes")
    private DataConsumer<byte[]> _dataConsumerBytes;
    @DataProviderInjection
    private DataProvider<String> _dataProvider;
}
