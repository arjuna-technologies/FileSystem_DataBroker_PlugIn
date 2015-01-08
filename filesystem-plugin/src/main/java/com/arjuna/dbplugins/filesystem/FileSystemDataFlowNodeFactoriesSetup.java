/*
 * Copyright (c) 2014-2015, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.filesystem;

import java.util.Collections;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.ejb.EJB;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import com.arjuna.databroker.data.DataFlowNodeFactory;
import com.arjuna.databroker.data.DataFlowNodeFactoryInventory;
import com.arjuna.dbplugins.filesystem.file.FileChangeDataSourceFactory;
import com.arjuna.dbplugins.filesystem.directory.DirectoryChangeDataSourceFactory;
import com.arjuna.dbplugins.filesystem.file.FileReaderDataProcessorFactory;
import com.arjuna.dbplugins.filesystem.file.FileUpdateDataServiceFactory;
import com.arjuna.dbplugins.filesystem.file.PollingFileChangeDataSourceFactory;
import com.arjuna.dbplugins.filesystem.directory.DirectoryUpdateDataServiceFactory;

@Startup
@Singleton
public class FileSystemDataFlowNodeFactoriesSetup
{
    @PostConstruct
    public void setup()
    {
        DataFlowNodeFactory fileChangeDataSourceFactory        = new FileChangeDataSourceFactory("File Change Data Source Factory", Collections.<String, String>emptyMap());
        DataFlowNodeFactory pollingFileChangeDataSourceFactory = new PollingFileChangeDataSourceFactory("Polling File Change Data Source Factory", Collections.<String, String>emptyMap());
        DataFlowNodeFactory directoryChangeDataSourceFactory   = new DirectoryChangeDataSourceFactory("Directory Change Data Source Factory", Collections.<String, String>emptyMap());
        DataFlowNodeFactory fileReaderDataProcessorFactory     = new FileReaderDataProcessorFactory("File Reader Data Processor Factory", Collections.<String, String>emptyMap());
        DataFlowNodeFactory fileUpdateDataServiceFactory       = new FileUpdateDataServiceFactory("File Update Data Service Factory", Collections.<String, String>emptyMap());
        DataFlowNodeFactory directoryUpdateDataServiceFactory  = new DirectoryUpdateDataServiceFactory("Directory Update Data Service Factory", Collections.<String, String>emptyMap());

        _dataFlowNodeFactoryInventory.addDataFlowNodeFactory(fileChangeDataSourceFactory);
        _dataFlowNodeFactoryInventory.addDataFlowNodeFactory(pollingFileChangeDataSourceFactory);
        _dataFlowNodeFactoryInventory.addDataFlowNodeFactory(fileReaderDataProcessorFactory);
        _dataFlowNodeFactoryInventory.addDataFlowNodeFactory(fileUpdateDataServiceFactory);
        _dataFlowNodeFactoryInventory.addDataFlowNodeFactory(directoryChangeDataSourceFactory);
        _dataFlowNodeFactoryInventory.addDataFlowNodeFactory(directoryUpdateDataServiceFactory);
    }

    @PreDestroy
    public void cleanup()
    {
        _dataFlowNodeFactoryInventory.removeDataFlowNodeFactory("File Change Data Source Factory");
        _dataFlowNodeFactoryInventory.removeDataFlowNodeFactory("Polling File Change Data Source Factory");
        _dataFlowNodeFactoryInventory.removeDataFlowNodeFactory("Directory Change Data Source Factory");
        _dataFlowNodeFactoryInventory.removeDataFlowNodeFactory("File Reader Data Processor Factory");
        _dataFlowNodeFactoryInventory.removeDataFlowNodeFactory("File Update Data Service Factory");
        _dataFlowNodeFactoryInventory.removeDataFlowNodeFactory("Directory Update Data Service Factory");
    }

    @EJB(lookup="java:global/databroker/data-core-jee/DataFlowNodeFactoryInventory")
    private DataFlowNodeFactoryInventory _dataFlowNodeFactoryInventory;
}
