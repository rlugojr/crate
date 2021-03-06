/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.module;

import io.crate.Version;
import io.crate.plugin.PluginLoader;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.PreProcessModule;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

public class PluginLoaderModule extends AbstractModule implements PreProcessModule {

    private final ESLogger logger;
    private final PluginLoader pluginLoader;


    public PluginLoaderModule(Settings settings, PluginLoader pluginLoader) {
        logger = Loggers.getLogger(getClass().getPackage().getName(), settings);
        this.pluginLoader = pluginLoader;
    }

    @Override
    protected void configure() {
        Version version = Version.CURRENT;
        logger.info("configuring crate. version: {}", version);
    }

    @Override
    public void processModule(Module module) {
        pluginLoader.processModule(module);
    }
}
