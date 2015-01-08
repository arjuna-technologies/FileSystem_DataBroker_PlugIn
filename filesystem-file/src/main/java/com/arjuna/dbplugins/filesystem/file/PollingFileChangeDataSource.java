/*
 * Copyright (c) 2014-2015, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
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
import com.arjuna.databroker.data.DataFlow;
import com.arjuna.databroker.data.DataProvider;
import com.arjuna.databroker.data.DataSource;
import com.arjuna.databroker.data.jee.annotation.DataProviderInjection;
import com.arjuna.databroker.data.jee.annotation.PreActivated;
import com.arjuna.databroker.data.jee.annotation.PreDeactivated;

public class PollingFileChangeDataSource implements DataSource
{
    private static final Logger logger = Logger.getLogger(PollingFileChangeDataSource.class.getName());

    public static final String FILENAME_PROPERYNAME     = "File Name";
    public static final String POLLINTERVAL_PROPERYNAME = "Poll Interval (ms)";

    public PollingFileChangeDataSource()
    {
        logger.log(Level.FINE, "FileChangeDataSource");
    }

    public PollingFileChangeDataSource(String name, Map<String, String> properties)
    {
        logger.log(Level.FINE, "FileChangeDataSource: " + name + ", " + properties);

        _name       = name;
        _properties = properties;
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

    @PreActivated
    public void start()
    {
        File file         = (new File(_properties.get(FILENAME_PROPERYNAME))).getAbsoluteFile();
        long pollInterval = Long.parseLong(_properties.get(POLLINTERVAL_PROPERYNAME));

        _poller = new Poller(file, pollInterval);
        _poller.start();
    }

    @PreDeactivated
    public void finish()
    {
        _poller.finish();
    }
    
    private class Poller extends Thread
    {
        public Poller(File file, long pollInterval)
        {
            _file          = file;
            _pollInterval  = pollInterval;
            _finish        = false;
        }

        @Override
        public void run()
        {
            try
            {
                long oldLastModified = _file.lastModified();

                while (! _finish)
                {
                    long newLastModified = _file.lastModified();
                    
                    if (oldLastModified != newLastModified)
                    {
                        _dataProvider.produce(_file);
                        oldLastModified = newLastModified;
                    }

                    try
                    {
                        Thread.sleep(_pollInterval);
                    }
                    catch (InterruptedException interruptedException)
                    {
                    }
                    catch (Throwable throwable)
                    {
                        logger.log(Level.WARNING, "Problem while sleeping", throwable);
                    }
                }
            }
            catch (Throwable throwable)
            {
                logger.log(Level.WARNING, "Problem while polling file", throwable);
            }
        }

        public void finish()
        {
            try
            {
                _finish = true;
                this.interrupt();
                this.join();
            }
            catch (Throwable throwable)
            {
                logger.log(Level.WARNING, "Problem during file watcher shutdown", throwable);
            }
        }

        private File    _file;
        private long    _pollInterval;
        private boolean _finish;
    }

    private DataFlow            _dataFlow;
    private String              _name;
    private Map<String, String> _properties;
    @DataProviderInjection
    private DataProvider<File>  _dataProvider;

    private Poller _poller;
}
