/*
 * Copyright (c) 2014, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
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

@Startup
@Singleton
public class FileSystemDataFlowNodeFactoriesSetup
{
    @PostConstruct
    public void setup()
    {
        DataFlowNodeFactory fileChangeDataSourceFactory      = new FileChangeDataSourceFactory("File Change Data Source Factory", Collections.<String, String>emptyMap());
        DataFlowNodeFactory directoryChangeDataSourceFactory = new DirectoryChangeDataSourceFactory("Directory Change Data Source Factory", Collections.<String, String>emptyMap());

        _dataFlowNodeFactoryInventory.addDataFlowNodeFactory(fileChangeDataSourceFactory);
        _dataFlowNodeFactoryInventory.addDataFlowNodeFactory(directoryChangeDataSourceFactory);
    }

    @PreDestroy
    public void cleanup()
    {
        _dataFlowNodeFactoryInventory.removeDataFlowNodeFactory("File Change Data Source Factory");
        _dataFlowNodeFactoryInventory.removeDataFlowNodeFactory("Directory Change Data Source Factory");
    }

    @EJB(lookup="java:global/databroker/control-core/DataFlowNodeFactoryInventory")
    private DataFlowNodeFactoryInventory _dataFlowNodeFactoryInventory;
}
