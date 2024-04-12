/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.polycat.catalog.server;

import java.lang.reflect.InvocationTargetException;

import io.polycat.catalog.common.Logger;
import io.polycat.metrics.PolyCatMetricsService;
import io.polycat.metrics.MethodStageDurationCollector;

import io.micrometer.core.instrument.MeterRegistry;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@MapperScan(basePackages = {"io.polycat.catalog.store.mapper"})
@ComponentScan(basePackages = {"io.polycat"})
@ServletComponentScan
public class CatalogApplication extends SpringBootServletInitializer {

    private static final Logger log = Logger.getLogger(CatalogApplication.class);
    private static PolyCatMetricsService metricsService;

    /**
     * main function
     *
     * @param args arguments
     */
    public static void main(String[] args) throws InvocationTargetException, IllegalAccessException {
        log.info("******CatalogApplication start...******");

        MethodStageDurationCollector.initMetricsCollectors();
        metricsService = new PolyCatMetricsService(8085);
        metricsService.start();

        SpringApplication.run(CatalogApplication.class, args);
    }

    @Bean
    MeterRegistryCustomizer<MeterRegistry> configurer(@Value("${spring.application.name}") String applicationName) {
        return registry -> registry.config().commonTags("application", applicationName);
    }
}
