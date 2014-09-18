/*
 * Copyright (c) 2014, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.filesystem.file;

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.WatchEvent.Kind;
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

public class FileChangeDataSource implements DataSource
{
    private static final Logger logger = Logger.getLogger(FileChangeDataSource.class.getName());

    public static final String FILENAME_PROPERYNAME = "File Name";

    public FileChangeDataSource(String name, Map<String, String> properties)
    {
        logger.log(Level.FINE, "FileChangeDataSource: " + name + ", " + properties);

        _name       = name;
        _properties = properties;

        _watcher = new Watcher(new File(_properties.get(FILENAME_PROPERYNAME)));
        _watcher.start();
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

    public void finish()
    {
        _watcher.finish();
    }
    
    private class Watcher extends Thread
    {
        public Watcher(File file)
        {
            _filePath      = file.toPath().toAbsolutePath();
            _directoryPath = file.getParentFile().toPath().toAbsolutePath();
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
                            Kind<?> kind = watchEvent.kind();

                            Path context = _directoryPath.resolve((Path) watchEvent.context());
                            if (context.equals(_filePath) && ((kind == StandardWatchEventKinds.ENTRY_CREATE) || (kind == StandardWatchEventKinds.ENTRY_MODIFY)))
                                _dataProvider.produce(context.toFile());
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
                logger.log(Level.WARNING, "Problem while watching file", throwable);
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

        private Path    _filePath;
        private Path    _directoryPath;
        private boolean _finish;
    }

    private DataFlow            _dataFlow;
    private String              _name;
    private Map<String, String> _properties;
    @DataProviderInjection
    private DataProvider<File>  _dataProvider;

    private Watcher _watcher;
}