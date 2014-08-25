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
import com.arjuna.dbplugins.filesystem.file.FileChangeDataFlowNodeFactory;
import com.arjuna.dbplugins.filesystem.directory.DirectoryChangeDataFlowNodeFactory;

@Startup
@Singleton
public class FileSystemDataFlowNodeFactoriesSetup
{
    @PostConstruct
    public void setup()
    {
        DataFlowNodeFactory fileChangeDataFlowNodeFactory      = new FileChangeDataFlowNodeFactory("File Change Data Flow Node Factories", Collections.<String, String>emptyMap());
        DataFlowNodeFactory directoryChangeDataFlowNodeFactory = new DirectoryChangeDataFlowNodeFactory("Directory Change Data Flow Node Factories", Collections.<String, String>emptyMap());

        _dataFlowNodeFactoryInventory.addDataFlowNodeFactory(fileChangeDataFlowNodeFactory);
        _dataFlowNodeFactoryInventory.addDataFlowNodeFactory(directoryChangeDataFlowNodeFactory);
    }

    @PreDestroy
    public void cleanup()
    {
        _dataFlowNodeFactoryInventory.removeDataFlowNodeFactory("File Change Data Flow Node Factories");
        _dataFlowNodeFactoryInventory.removeDataFlowNodeFactory("Directory Change Data Flow Node Factories");
    }

    @EJB(lookup="java:global/databroker/control-core/DataFlowNodeFactoryInventory")
    private DataFlowNodeFactoryInventory _dataFlowNodeFactoryInventory;
}
