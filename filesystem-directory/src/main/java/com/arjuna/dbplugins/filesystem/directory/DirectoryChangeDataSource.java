/*
 * Copyright (c) 2014, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.filesystem.directory;

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.risbic.intraconnect.basic.BasicDataProvider;
import com.arjuna.databroker.data.DataProvider;
import com.arjuna.databroker.data.DataSource;

public class DirectoryChangeDataSource implements DataSource
{
    private static final Logger logger = Logger.getLogger(DirectoryChangeDataSource.class.getName());

    public static final String DIRECTORYNAME_PROPERYNAME = "Directory Name";

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

    public void finish()
    {
        _watcher.finish();
    }
    
    private class Watcher extends Thread
    {
        public Watcher(File directory)
        {
            _directoryPath = directory.toPath();
        }

        @Override
        public void run()
        {
            try
            {
                WatchService watchService = FileSystems.getDefault().newWatchService();
                _directoryPath.register(watchService, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY);

                while (! _finish)
                {
                    WatchKey watchKey = watchService.take();

                    if (watchKey != null)
                    {
                        for (WatchEvent<?> watchEvent : watchKey.pollEvents())
                        {
                            final Kind<?> kind = watchEvent.kind();
        
                            if (kind == StandardWatchEventKinds.ENTRY_CREATE)
                            {
                                logger.log(Level.WARNING, "Found: " + ((Path) watchEvent.context()).toFile());
                                _dataProvider.produce(((Path) watchEvent.context()).toFile());
                            }
                        }

                        if (! watchKey.reset())
                            _finish = true;
                    }
                }

                watchService.close();
            }
            catch (InterruptedException interruptedException)
            {
            }
            catch (Throwable throwable)
            {
                logger.log(Level.WARNING, "Problem while watching directory", throwable);
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
                logger.log(Level.WARNING, "Problem during watcher shutdown", throwable);
            }
        }

        private Path    _directoryPath;
        private boolean _finish;
    }

    private String              _name;
    private Map<String, String> _properties;
    private DataProvider<File>  _dataProvider;

    private Watcher _watcher;
}