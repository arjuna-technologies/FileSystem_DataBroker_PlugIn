/*
 * Copyright (c) 2014-2015, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.filesystem.directory;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
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

public class DirectoryUpdateDataService implements DataService
{
    private static final Logger logger = Logger.getLogger(DirectoryUpdateDataService.class.getName());

    public static final String DIRECTORYNAME_PROPERYNAME   = "Directory Name";
    public static final String FILENAMEPREFIX_PROPERYNAME  = "File Name Prefix";
    public static final String FILENAMEPOSTFIX_PROPERYNAME = "File Name Postfix";

    public DirectoryUpdateDataService()
    {
        logger.log(Level.FINE, "DirectoryUpdateDataService");

        _directory = null;
    }

    public DirectoryUpdateDataService(String name, Map<String, String> properties)
    {
        logger.log(Level.FINE, "DirectoryUpdateDataService: " + name + ", " + properties);

        _name       = name;
        _properties = properties;

        _directory = null;
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
            _directory       = new File(_properties.get(DIRECTORYNAME_PROPERYNAME));
            _fileNamePrefix  = _properties.get(FILENAMEPREFIX_PROPERYNAME);
            _fileNamePostfix = _properties.get(FILENAMEPOSTFIX_PROPERYNAME);
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Problem configuring: " + _properties, throwable);
        }
    }

    public void updateString(String data)
    {
        try
        {
            if (_directory != null)
            {
                FileOutputStream fileOutputStream = new FileOutputStream(new File(_directory, _fileNamePrefix + UUID.randomUUID().toString() + _fileNamePostfix));
                fileOutputStream.write(data.getBytes());
                fileOutputStream.close();
            }
            else
                logger.log(Level.WARNING, "Problem writing data, directory 'null'");
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Problem while updating file in directory: " + _directory, throwable);
        }
    }

    public void updateBytes(byte[] data)
    {
        try
        {
            if (_directory != null)
            {
                FileOutputStream fileOutputStream = new FileOutputStream(new File(_directory, _fileNamePrefix + UUID.randomUUID().toString() + _fileNamePostfix));
                fileOutputStream.write(data);
                fileOutputStream.close();
            }
            else
                logger.log(Level.WARNING, "Problem writing data, directory 'null'");
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Problem while updating file in directory: " + _directory, throwable);
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

    private File   _directory;
    private String _fileNamePrefix;
    private String _fileNamePostfix;

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
