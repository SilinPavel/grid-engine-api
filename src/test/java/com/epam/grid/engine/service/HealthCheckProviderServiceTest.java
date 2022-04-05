/*
 *
 *  * Copyright 2022 EPAM Systems, Inc. (https://www.epam.com/)
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package com.epam.grid.engine.service;

import com.epam.grid.engine.cmd.GridEngineCommandCompilerImpl;
import com.epam.grid.engine.cmd.SimpleCmdExecutor;
import com.epam.grid.engine.entity.EngineType;
import com.epam.grid.engine.entity.healthcheck.GridEngineStatus;
import com.epam.grid.engine.entity.healthcheck.HealthCheckInfo;
import com.epam.grid.engine.entity.healthcheck.StatusInfo;
import com.epam.grid.engine.provider.healthcheck.HealthCheckProvider;
import com.epam.grid.engine.provider.healthcheck.sge.SgeHealthCheckProvider;
import com.epam.grid.engine.provider.healthcheck.slurm.SlurmHealthCheckProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.thymeleaf.spring5.SpringTemplateEngine;

import java.time.LocalDateTime;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest
public class HealthCheckProviderServiceTest {

    private static final String SOME_INFO = "SomeInfo";

    @Autowired
    private HealthCheckProviderService healthCheckProviderService;
    @Autowired
    private HealthCheckProvider healthCheckProvider;

    @Configuration
    protected static class HealthCheckConfig {

        @Bean
        @ConditionalOnProperty(name = "ENGINE_TYPE", havingValue = "SGE")
        public HealthCheckProviderService sgeHealthCheckProviderService() {
            return Mockito.spy(new HealthCheckProviderService(EngineType.SGE));
        }

        @Bean
        @ConditionalOnProperty(name = "ENGINE_TYPE", havingValue = "SGE")
        public HealthCheckProvider sgeHealthCheckProvider() {
            return Mockito.spy(new SgeHealthCheckProvider("6444",
                    "/opt/sge/default/common/act_qmaster", new SimpleCmdExecutor(),
                    new GridEngineCommandCompilerImpl(new SpringTemplateEngine())));
        }

        @Bean
        @ConditionalOnProperty(name = "ENGINE_TYPE", havingValue = "SLURM")
        public HealthCheckProviderService slurmHealthCheckProviderService() {
            return Mockito.spy(new HealthCheckProviderService(EngineType.SLURM));
        }

        @Bean
        @ConditionalOnProperty(name = "ENGINE_TYPE", havingValue = "SLURM")
        public HealthCheckProvider slurmHealthCheckProvider() {
            return Mockito.spy(new SlurmHealthCheckProvider(new SimpleCmdExecutor(),
                    new GridEngineCommandCompilerImpl(new SpringTemplateEngine())));
        }
    }

    @Test
    public void shouldReturnCorrectHealthCheckInfo() {
        final StatusInfo expectedStatusInfo = StatusInfo.builder()
                .code(0L)
                .status(GridEngineStatus.OK)
                .info(SOME_INFO)
                .build();
        final HealthCheckInfo expectedHealthCheckInfo = HealthCheckInfo.builder()
                .statusInfo(expectedStatusInfo)
                .startTime(LocalDateTime.of(1992, 12, 18, 4, 0, 0))
                .checkTime(LocalDateTime.now())
                .build();

        doReturn(expectedHealthCheckInfo).when(healthCheckProvider).checkHealth();
        Assertions.assertEquals(expectedHealthCheckInfo, healthCheckProviderService.checkHealth());
        verify(healthCheckProvider, times(1)).checkHealth();
    }
}
