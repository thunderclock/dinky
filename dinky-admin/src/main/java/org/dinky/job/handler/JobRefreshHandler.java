/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.dinky.job.handler;

import static org.dinky.utils.JsonUtils.objectMapper;

import org.dinky.api.FlinkAPI;
import org.dinky.assertion.Asserts;
import org.dinky.cluster.FlinkClusterInfo;
import org.dinky.context.SpringContextUtils;
import org.dinky.context.TenantContextHolder;
import org.dinky.data.constant.FlinkRestResultConstant;
import org.dinky.data.dto.ClusterConfigurationDTO;
import org.dinky.data.dto.JobDataDto;
import org.dinky.data.enums.GatewayType;
import org.dinky.data.enums.JobStatus;
import org.dinky.data.flink.backpressure.FlinkJobNodeBackPressure;
import org.dinky.data.flink.checkpoint.CheckPointOverView;
import org.dinky.data.flink.config.CheckpointConfigInfo;
import org.dinky.data.flink.config.FlinkJobConfigInfo;
import org.dinky.data.flink.exceptions.FlinkJobExceptionsDetail;
import org.dinky.data.flink.job.FlinkJobDetailInfo;
import org.dinky.data.flink.watermark.FlinkJobNodeWaterMark;
import org.dinky.data.model.ClusterInstance;
import org.dinky.data.model.SystemConfiguration;
import org.dinky.data.model.ext.JobInfoDetail;
import org.dinky.data.model.job.JobInstance;
import org.dinky.gateway.Gateway;
import org.dinky.gateway.config.GatewayConfig;
import org.dinky.gateway.exception.NotSupportGetStatusException;
import org.dinky.gateway.model.FlinkClusterConfig;
import org.dinky.init.FlinkHistoryServer;
import org.dinky.job.JobConfig;
import org.dinky.service.ClusterInstanceService;
import org.dinky.service.HistoryService;
import org.dinky.service.JobHistoryService;
import org.dinky.service.JobInstanceService;
import org.dinky.utils.JsonUtils;
import org.dinky.utils.TimeUtil;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson2.JSON;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.type.CollectionType;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
@Component
@DependsOn("springContextUtils")
public class JobRefreshHandler {

    private static final JobInstanceService jobInstanceService;
    private static final JobHistoryService jobHistoryService;
    private static final ClusterInstanceService clusterInstanceService;
    private static final HistoryService historyService;

    static {
        jobInstanceService = SpringContextUtils.getBean("jobInstanceServiceImpl", JobInstanceService.class);
        jobHistoryService = SpringContextUtils.getBean("jobHistoryServiceImpl", JobHistoryService.class);
        clusterInstanceService = SpringContextUtils.getBean("clusterInstanceServiceImpl", ClusterInstanceService.class);
        historyService = SpringContextUtils.getBean("historyServiceImpl", HistoryService.class);
    }

    /**
     * Refresh the job
     * It receives two parameters: {@link JobInfoDetail} and needSave and returns a Boolean value.
     * When the return value is true, the job has completed and needs to be removed from the thread pool,
     * otherwise it means that the next round of flushing continues
     *
     * @param jobInfoDetail job info detail.
     * @param needSave      Indicates if the job needs to be saved.
     * @return True if the job is done, false otherwise.
     */
    public static boolean refreshJob(JobInfoDetail jobInfoDetail, boolean needSave) {
        // Add null check to prevent NPE
        if (Asserts.isNull(jobInfoDetail)) {
            log.warn("JobInfoDetail is null, skip refresh");
            return true;
        }
        
        JobInstance jobInstance = jobInfoDetail.getInstance();
        if (Asserts.isNull(jobInstance)) {
            log.warn("JobInstance is null, skip refresh for jobInfoDetail id: {}", jobInfoDetail.getId());
            return true;
        }
        
        if (Asserts.isNull(TenantContextHolder.get())) {
            jobInstanceService.initTenantByJobInstanceId(jobInstance.getId());
        }
        log.debug(
                "Start to refresh job: {}->{}",
                jobInstance.getId(),
                jobInstance.getName());

        JobDataDto jobDataDto = jobInfoDetail.getJobDataDto();
        // Initialize jobDataDto if null to prevent NPE
        if (Asserts.isNull(jobDataDto)) {
            jobDataDto = JobDataDto.builder().id(jobInstance.getId()).build();
            jobInfoDetail.setJobDataDto(jobDataDto);
        }
        
        String oldStatus = jobInstance.getStatus();

        // Cluster information is missing and cannot be monitored
        if (Asserts.isNull(jobInfoDetail.getClusterInstance())) {
            jobInstance.setStatus(JobStatus.UNKNOWN.getValue());
            jobInstanceService.updateById(jobInstance);
            return true;
        }

        checkAndRefreshCluster(jobInfoDetail);

        // Update the value of JobData from the flink api while ignoring the null value to prevent
        // some other configuration from being overwritten
        String jobManagerHost = jobInfoDetail.getClusterInstance().getJobManagerHost();
        String jid = jobInstance.getJid();
        
        // Add null checks to prevent NPE
        if (Asserts.isNull(jobManagerHost) || Asserts.isNull(jid)) {
            log.warn("JobManagerHost or JID is null for job {}, jobManagerHost: {}, jid: {}", 
                    jobInstance.getId(), jobManagerHost, jid);
            jobDataDto.setError(true);
            jobDataDto.setErrorMsg("JobManagerHost or JID is null");
        } else {
            JobDataDto newJobDataDto = getJobData(
                    jobInstance.getId(),
                    jobManagerHost,
                    jid);
            
            // Ensure newJobDataDto is not null before copying
            if (Asserts.isNotNull(newJobDataDto)) {
                BeanUtil.copyProperties(
                        newJobDataDto,
                        jobDataDto,
                        CopyOptions.create().ignoreNullValue());
            }
        }

        if (Asserts.isNull(jobDataDto.getJob()) || jobDataDto.isError()) {
            // Try to get job status from Gateway first (this works even when JobManager is down)
            Optional<JobStatus> jobStatus = getJobStatus(jobInfoDetail);
            
            // Check if connection failed due to JobManager shutdown (Connection refused or File not found)
            boolean isConnectionRefused = jobDataDto.isError() 
                    && jobDataDto.getErrorMsg() != null 
                    && (jobDataDto.getErrorMsg().contains("Connection refused")
                            || jobDataDto.getErrorMsg().contains("File not found")
                            || jobDataDto.getErrorMsg().contains("ConnectException"));
            
            if (jobStatus.isPresent() && JobStatus.isDone(jobStatus.get().getValue())) {
                // If we can get final status from Gateway, use it
                jobInstance.setStatus(jobStatus.get().getValue());
                log.info("Job {} status updated to {} via Gateway (JobManager connection failed)", 
                        jobInstance.getId(), jobStatus.get().getValue());
            } else if (isConnectionRefused) {
                // Connection refused usually means JobManager is down, which happens when job is finished
                // Always try to get status from HistoryServer when connection fails
                String currentStatus = jobInstance.getStatus();
                boolean triedHistoryServer = tryGetJobStatusFromHistoryServer(jobInstance, jobDataDto, jid);
                
                if (!triedHistoryServer) {
                    // If HistoryServer also failed, check if current status is already done
                    if (JobStatus.isDone(currentStatus)) {
                        // Job is already marked as done, keep the status
                        log.debug("Job {} connection refused but status is already done: {}", 
                                jobInstance.getId(), currentStatus);
                    } else {
                        // Try to get final status from Gateway one more time
                        if (jobStatus.isPresent()) {
                            jobInstance.setStatus(jobStatus.get().getValue());
                            log.info("Job {} connection refused, status updated to {} via Gateway", 
                                    jobInstance.getId(), jobStatus.get().getValue());
                        } else {
                            // Cannot get status from Gateway or HistoryServer, keep current status
                            log.debug("Job {} connection refused, cannot get status from Gateway or HistoryServer, will retry", 
                                    jobInstance.getId());
                        }
                    }
                }
            } else {
                // For INITIALIZING and CREATED status, keep the status and continue refreshing
                // This is especially important for batch jobs which may take time to initialize
                String currentStatus = jobInstance.getStatus();
                if (JobStatus.INITIALIZING.getValue().equals(currentStatus)
                        || JobStatus.CREATED.getValue().equals(currentStatus)) {
                    // Keep INITIALIZING/CREATED status and continue refreshing
                    // Only set RECONNECTING if the job has been in this state for more than 2 minutes
                    LocalDateTime createTime = jobInstance.getCreateTime();
                    if (createTime != null) {
                        long durationSeconds = Duration.between(createTime, LocalDateTime.now()).getSeconds();
                        long durationMinutes = Duration.between(createTime, LocalDateTime.now()).toMinutes();
                        
                        // For very short-lived jobs (completed within 1 second), try to get final status
                        // This handles cases where jobs finish before status can be properly refreshed
                        if (durationSeconds <= 1 && jobStatus.isPresent()) {
                            // If we can get status from gateway, use it even if jobData is not available
                            jobInstance.setStatus(jobStatus.get().getValue());
                            log.debug("Job {} completed quickly ({}s), status updated to {}", 
                                    jobInstance.getId(), durationSeconds, jobStatus.get().getValue());
                        } else if (jobDataDto.isError()) {
                            // If any error occurred (including BackPressure errors), try to get status from HistoryServer
                            // This handles cases where job finished but status refresh failed due to API errors
                            boolean triedHistoryServer = tryGetJobStatusFromHistoryServer(jobInstance, jobDataDto, jid);
                            
                            if (!triedHistoryServer) {
                                // If HistoryServer also failed, check if we can get status from Gateway
                                if (jobStatus.isPresent()) {
                                    jobInstance.setStatus(jobStatus.get().getValue());
                                    log.info("Job {} INITIALIZING with error, status updated to {} via Gateway", 
                                            jobInstance.getId(), jobStatus.get().getValue());
                                } else if (durationMinutes > 2) {
                                    // If still can't get status after 2 minutes, set to RECONNECTING
                                    jobInstance.setStatus(JobStatus.RECONNECTING.getValue());
                                    jobInstance.setError(jobDataDto.getErrorMsg());
                                    jobInfoDetail.getJobDataDto().setError(true);
                                    jobInfoDetail.getJobDataDto().setErrorMsg(jobDataDto.getErrorMsg());
                                } else {
                                    // Keep the current status and continue refreshing
                                    jobInfoDetail.getJobDataDto().setError(true);
                                    jobInfoDetail.getJobDataDto().setErrorMsg(jobDataDto.getErrorMsg());
                                }
                            }
                        } else if (durationMinutes > 2) {
                            // If INITIALIZING/CREATED for more than 2 minutes and still can't get status,
                            // set to RECONNECTING
                            jobInstance.setStatus(JobStatus.RECONNECTING.getValue());
                            jobInstance.setError(jobDataDto.getErrorMsg());
                            jobInfoDetail.getJobDataDto().setError(true);
                            jobInfoDetail.getJobDataDto().setErrorMsg(jobDataDto.getErrorMsg());
                        } else {
                            // Keep the current status and continue refreshing
                            jobInfoDetail.getJobDataDto().setError(true);
                            jobInfoDetail.getJobDataDto().setErrorMsg(jobDataDto.getErrorMsg());
                        }
                    } else {
                        // If createTime is null, keep current status
                        jobInfoDetail.getJobDataDto().setError(true);
                        jobInfoDetail.getJobDataDto().setErrorMsg(jobDataDto.getErrorMsg());
                    }
                } else {
                    // For other statuses, set to RECONNECTING as before
                    jobInstance.setStatus(JobStatus.RECONNECTING.getValue());
                    jobInstance.setError(jobDataDto.getErrorMsg());
                    jobInfoDetail.getJobDataDto().setError(true);
                    jobInfoDetail.getJobDataDto().setErrorMsg(jobDataDto.getErrorMsg());
                }
            }
            // Set finish time for done jobs or when connection is refused (JobManager shutdown)
            if (jobInstance.getFinishTime() == null || TimeUtil.localDateTimeToLong(jobInstance.getFinishTime()) < 1) {
                String currentStatus = jobInstance.getStatus();
                
                // If job is done or connection refused (likely job finished), set finish time
                // Note: isConnectionRefused is already defined above, reuse it
                if (JobStatus.isDone(currentStatus) || isConnectionRefused) {
                    jobInstance.setFinishTime(LocalDateTime.now());
                } else if (!JobStatus.INITIALIZING.getValue().equals(currentStatus)
                        && !JobStatus.CREATED.getValue().equals(currentStatus)) {
                    jobInstance.setFinishTime(LocalDateTime.now());
                } else {
                    // For INITIALIZING/CREATED, set finish time if connection failed and job has been running for a while
                    // This handles cases where job finished but status wasn't updated
                    LocalDateTime createTime = jobInstance.getCreateTime();
                    if (createTime != null) {
                        long durationSeconds = Duration.between(createTime, LocalDateTime.now()).getSeconds();
                        long durationMinutes = Duration.between(createTime, LocalDateTime.now()).toMinutes();
                        // If connection failed and job has been running for more than 30 seconds, set finish time
                        if (jobDataDto.isError() && durationSeconds > 30) {
                            jobInstance.setFinishTime(LocalDateTime.now());
                        } else if (durationMinutes > 2) {
                            jobInstance.setFinishTime(LocalDateTime.now());
                        }
                    }
                }
            }
        } else {
            jobInfoDetail.setJobDataDto(jobDataDto);
            FlinkJobDetailInfo flinkJobDetailInfo = jobDataDto.getJob();
            jobInstance.setStatus(flinkJobDetailInfo.getState());
            jobInstance.setDuration(flinkJobDetailInfo.getDuration());
            jobInstance.setCreateTime(TimeUtil.toLocalDateTime(flinkJobDetailInfo.getStartTime()));
            // if the job is still running the end-time is -1
            jobInstance.setFinishTime(TimeUtil.toLocalDateTime(flinkJobDetailInfo.getEndTime()));
        }
        jobInstance.setUpdateTime(LocalDateTime.now());

        // The transition status include failed and reconnecting ( Dinky custom )
        // The done status include failed and canceled and finished and unknown ( Dinky custom )
        // The task status of batch job which network unstable: run -> transition -> run -> transition -> done
        // The task status of stream job which automatically restart after failure: run -> transition -> run ->
        // transition -> run
        // Set to true if the job status which is done has completed
        // If the job status is transition and the status fails to be updated for 1 minute, set to true and discard the
        // update

        boolean isTransition = false;

        if (JobStatus.isTransition(jobInstance.getStatus())) {
            Long finishTime = TimeUtil.localDateTimeToLong(jobInstance.getFinishTime());
            long duration = Duration.between(jobInstance.getFinishTime(), LocalDateTime.now())
                    .toMinutes();
            if (finishTime > 0 && duration < 1) {
                log.debug("Job is transition: {}->{}", jobInstance.getId(), jobInstance.getName());
                isTransition = true;
            } else if (JobStatus.RECONNECTING.getValue().equals(jobInstance.getStatus())) {
                log.debug(
                        "Job is not reconnected success at the specified time,set as UNKNOWN: {}->{}",
                        jobInstance.getId(),
                        jobInstance.getName());
                jobInstance.setStatus(JobStatus.UNKNOWN.getValue());
            }
        }

        boolean isDone = (JobStatus.isDone(jobInstance.getStatus()))
                || (TimeUtil.localDateTimeToLong(jobInstance.getFinishTime()) > 0
                        && Duration.between(jobInstance.getFinishTime(), LocalDateTime.now())
                                        .toMinutes()
                                >= 1);

        isDone = !isTransition && isDone;

        if (!oldStatus.equals(jobInstance.getStatus()) || isDone || needSave) {
            log.debug("Dump JobInfo to database: {}->{}", jobInstance.getId(), jobInstance.getName());
            if (jobInstance.getStatus().equals(JobStatus.UNKNOWN.getValue())
                    || jobInstance.getStatus().equals(JobStatus.RECONNECTING.getValue())) {
                JobInstance fromDb = jobInstanceService.getById(jobInstance.getId());
                // If the job status is unknown and the job status in the database is not done, update the job status
                // just prevent the task from being mistakenly updated to UNKNOWN
                if (JobStatus.valueOf(fromDb.getStatus()).isDone()) {
                    // if status is RECONNECTING, ignore it
                    isDone = true;
                } else {
                    jobInstanceService.updateById(jobInstance);
                    jobHistoryService.updateById(jobInfoDetail.getJobDataDto().toJobHistory());
                }
            } else {
                jobInstanceService.updateById(jobInstance);
                jobHistoryService.updateById(jobInfoDetail.getJobDataDto().toJobHistory());
            }
        }

        if (isDone) {
            try {
                log.debug("Job is done: {}->{}", jobInstance.getId(), jobInstance.getName());
                handleJobDone(jobInfoDetail);
            } catch (Exception e) {
                log.error("failed handel job done：", e);
            }
        }
        return isDone;
    }

    /**
     * Retrieves job history.
     * getJobStatusInformationFromFlinkRestAPI
     *
     * @param id             The job ID.
     * @param jobManagerHost The job manager host.
     * @param jobId          The job ID.
     * @return {@link org.dinky.data.dto.JobDataDto}.
     */
    public static JobDataDto getJobData(Integer id, String jobManagerHost, String jobId) {
        // Add null checks to prevent NPE
        if (Asserts.isNull(jobId) || Asserts.isNull(jobManagerHost)) {
            log.warn("JobId or JobManagerHost is null, jobId: {}, jobManagerHost: {}", jobId, jobManagerHost);
            return JobDataDto.builder()
                    .id(id)
                    .error(true)
                    .errorMsg("JobId or JobManagerHost is null")
                    .build();
        }
        
        if (FlinkHistoryServer.HISTORY_JOBID_SET.contains(jobId)
                && SystemConfiguration.getInstances().getUseFlinkHistoryServer().getValue()) {
            jobManagerHost = "127.0.0.1:"
                    + SystemConfiguration.getInstances()
                            .getFlinkHistoryServerPort()
                            .getValue();
        }
        JobDataDto.JobDataDtoBuilder builder = JobDataDto.builder();
        FlinkAPI api = FlinkAPI.build(jobManagerHost);
        try {
            JsonNode jobInfo = FlinkAPI.build(jobManagerHost).getJobInfo(jobId);
            if (jobInfo.has(FlinkRestResultConstant.ERRORS)) {
                throw new Exception(String.valueOf(jobInfo.get(FlinkRestResultConstant.ERRORS)));
            }

            FlinkJobConfigInfo jobConfigInfo =
                    JSON.parseObject(api.getJobsConfig(jobId).toString()).toJavaObject(FlinkJobConfigInfo.class);

            FlinkJobDetailInfo flinkJobDetailInfo =
                    JSON.parseObject(jobInfo.toString()).toJavaObject(FlinkJobDetailInfo.class);
            // 获取 WATERMARK  & BACKPRESSURE 信息
            // Skip backpressure and watermark for INITIALIZING/CREATED jobs as they may not have data ready
            String jobState = flinkJobDetailInfo.getState();
            boolean skipBackpressure = JobStatus.INITIALIZING.getValue().equals(jobState)
                    || JobStatus.CREATED.getValue().equals(jobState);
            
            api.getVertices(jobId).forEach(vertex -> {
                flinkJobDetailInfo.getPlan().getNodes().forEach(planNode -> {
                    if (planNode.getId().equals(vertex)) {
                        try {
                            CollectionType listType = objectMapper
                                    .getTypeFactory()
                                    .constructCollectionType(ArrayList.class, FlinkJobNodeWaterMark.class);
                            List<FlinkJobNodeWaterMark> watermark =
                                    objectMapper.readValue(api.getWatermark(jobId, vertex), listType);
                            planNode.setWatermark(watermark);
                        } catch (Exception ignored) {
                        }
                        // Add exception handling for backpressure to avoid NoSuchElementException
                        // when job is in INITIALIZING/CREATED state or just started/finished
                        if (!skipBackpressure) {
                            try {
                                String backPressureResponse = api.getBackPressure(jobId, vertex);
                                // Check if response is valid JSON before parsing
                                if (backPressureResponse == null || backPressureResponse.trim().isEmpty()) {
                                    log.debug("BackPressure API returned empty response for job {} vertex {}, skipping", 
                                            jobId, vertex);
                                } else {
                                    // Check if response contains errors before parsing
                                    JsonNode backPressureJson = objectMapper.readTree(backPressureResponse);
                                    if (backPressureJson.has(FlinkRestResultConstant.ERRORS) 
                                            || backPressureJson.findParent("errors") != null) {
                                        log.debug("BackPressure API returned errors for job {} vertex {}, skipping", 
                                                jobId, vertex);
                                    } else {
                                        planNode.setBackpressure(JsonUtils.toJavaBean(
                                                backPressureResponse, FlinkJobNodeBackPressure.class));
                                    }
                                }
                            } catch (Exception e) {
                                // Log debug message for backpressure fetch failures
                                // This is expected for jobs that just started or finished, or when Flink
                                // internal error occurs (e.g., NoSuchElementException in getMaxBackPressureRatio)
                                // Do not let BackPressure errors affect job status refresh
                                log.debug("Failed to get backpressure for job {} vertex {}: {} (this will not affect job status)", 
                                        jobId, vertex, e.getMessage());
                            }
                        }
                    }
                });
            });
            JsonNode checkPoints = api.getCheckPoints(jobId);
            if (checkPoints.findParent("errors") == null) {
                builder.checkpoints(JsonUtils.parseObject(checkPoints.toString(), CheckPointOverView.class));
            }
            JsonNode checkpointConfigInfo = api.getCheckPointsConfig(jobId);
            if (checkpointConfigInfo.findParent("errors") == null) {
                builder.checkpointsConfig(
                        JsonUtils.parseObject(checkpointConfigInfo.toString(), CheckpointConfigInfo.class));
            }
            return builder.id(id)
                    .exceptions(
                            JsonUtils.parseObject(api.getException(jobId).toString(), FlinkJobExceptionsDetail.class))
                    .job(flinkJobDetailInfo)
                    .config(jobConfigInfo)
                    .build();
        } catch (Exception e) {
            // Use safe error message handling to prevent NPE
            String errorMsg = e.getMessage();
            if (errorMsg == null) {
                errorMsg = e.getClass().getName() + (e.getCause() != null ? ": " + e.getCause().getMessage() : "");
            }
            
            // Connection refused and File not found are normal when JobManager is down (job finished)
            // Use debug level instead of warn to reduce noise in logs
            if (errorMsg.contains("Connection refused") 
                    || errorMsg.contains("ConnectException")
                    || errorMsg.contains("File not found")) {
                log.debug("Connect {} failed (JobManager may be down): {}", jobManagerHost, errorMsg);
            } else {
                log.warn("Connect {} failed, {}", jobManagerHost, errorMsg);
            }
            return builder.id(id).error(true).errorMsg(errorMsg).build();
        }
    }

    /**
     * Try to get job status from HistoryServer.
     *
     * @param jobInstance The job instance.
     * @param jobDataDto The job data DTO to update.
     * @param jid The job ID.
     * @return True if successfully got status from HistoryServer, false otherwise.
     */
    private static boolean tryGetJobStatusFromHistoryServer(
            JobInstance jobInstance, JobDataDto jobDataDto, String jid) {
        if (!SystemConfiguration.getInstances().getUseFlinkHistoryServer().getValue() 
                || Asserts.isNull(jid)) {
            return false;
        }
        
        try {
            String historyServerHost = "127.0.0.1:"
                    + SystemConfiguration.getInstances()
                            .getFlinkHistoryServerPort()
                            .getValue();
            JobDataDto historyJobData = getJobData(
                    jobInstance.getId(),
                    historyServerHost,
                    jid);
            
            if (historyJobData != null && !historyJobData.isError() 
                    && historyJobData.getJob() != null) {
                // Successfully got status from HistoryServer
                FlinkJobDetailInfo historyJobInfo = historyJobData.getJob();
                jobInstance.setStatus(historyJobInfo.getState());
                jobInstance.setDuration(historyJobInfo.getDuration());
                jobInstance.setCreateTime(TimeUtil.toLocalDateTime(historyJobInfo.getStartTime()));
                jobInstance.setFinishTime(TimeUtil.toLocalDateTime(historyJobInfo.getEndTime()));
                
                // Update jobDataDto with history server data
                BeanUtil.copyProperties(
                        historyJobData,
                        jobDataDto,
                        CopyOptions.create().ignoreNullValue());
                
                log.info("Job {} connection failed, status updated to {} via HistoryServer", 
                        jobInstance.getId(), historyJobInfo.getState());
                return true;
            }
        } catch (Exception e) {
            log.debug("Failed to get job status from HistoryServer for job {}: {}", 
                    jobInstance.getId(), e.getMessage());
        }
        return false;
    }

    /**
     * Gets the job status.
     *
     * @param jobInfoDetail The job info detail.
     * @return The job status.
     */
    private static Optional<JobStatus> getJobStatus(JobInfoDetail jobInfoDetail) {

        ClusterConfigurationDTO clusterCfg = jobInfoDetail.getClusterConfiguration();
        ClusterInstance clusterInstance = jobInfoDetail.getClusterInstance();
        if (!Asserts.isNull(clusterCfg)
                && (GatewayType.YARN_PER_JOB.getLongValue().equals(clusterInstance.getType())
                        || GatewayType.YARN_APPLICATION.getLongValue().equals(clusterInstance.getType()))) {
            try {
                String appId = jobInfoDetail.getClusterInstance().getName();

                GatewayConfig gatewayConfig = GatewayConfig.build(clusterCfg.getConfig());
                gatewayConfig.getClusterConfig().setAppId(appId);
                gatewayConfig
                        .getFlinkConfig()
                        .setJobName(jobInfoDetail.getInstance().getName());

                Gateway gateway = Gateway.build(gatewayConfig);
                return Optional.of(gateway.getJobStatusById(appId));
            } catch (NotSupportGetStatusException ignored) {
                // if the gateway does not support get status, then use the api to get job status
                // ignore to do something here
            }
        }
        return Optional.empty();
    }

    /**
     * Handles job completion.
     *
     * @param jobInfoDetail The job info detail.
     */
    private static void handleJobDone(JobInfoDetail jobInfoDetail) {
        JobInstance jobInstance = jobInfoDetail.getInstance();
        JobDataDto jobDataDto = jobInfoDetail.getJobDataDto();
        String clusterType = jobInfoDetail.getClusterInstance().getType();

        if (GatewayType.isDeployCluster(clusterType)) {
            JobConfig jobConfig = new JobConfig();
            FlinkClusterConfig configJson = jobDataDto.getClusterConfiguration().getConfigJson();
            jobConfig.buildGatewayConfig(configJson);
            jobConfig.getGatewayConfig().setType(GatewayType.get(clusterType));
            jobConfig.getGatewayConfig().getFlinkConfig().setJobName(jobInstance.getName());
            Gateway.build(jobConfig.getGatewayConfig()).onJobFinishCallback(jobInstance.getStatus());
        }
    }

    /**
     * In a YARN cluster with HA mode enabled,
     * if the jobManagerHost cannot be connected,
     * attempt to retrieve the latest address of the jobManagerHost from ZK
     *
     * @param jobInfoDetail The job info detail.
     * @return The job status.
     */
    private static void checkAndRefreshCluster(JobInfoDetail jobInfoDetail) {
        if (!GatewayType.isDeployYarnCluster(jobInfoDetail.getClusterInstance().getType())) {
            return;
        }

        FlinkClusterInfo flinkClusterInfo = clusterInstanceService.checkHeartBeat(
                jobInfoDetail.getClusterInstance().getHosts(),
                jobInfoDetail.getClusterInstance().getJobManagerHost());
        if (!flinkClusterInfo.isEffective()) {
            ClusterConfigurationDTO clusterCfg = jobInfoDetail.getClusterConfiguration();
            ClusterInstance clusterInstance = jobInfoDetail.getClusterInstance();
            if (!Asserts.isNull(clusterCfg)) {
                String appId = jobInfoDetail.getClusterInstance().getName();

                GatewayConfig gatewayConfig = GatewayConfig.build(clusterCfg.getConfig());
                gatewayConfig.getClusterConfig().setAppId(appId);
                gatewayConfig
                        .getFlinkConfig()
                        .setJobName(jobInfoDetail.getInstance().getName());

                Gateway gateway = Gateway.build(gatewayConfig);
                String latestJobManageHost = gateway.getLatestJobManageHost(appId, clusterInstance.getJobManagerHost());

                if (Asserts.isNotNull(latestJobManageHost)) {
                    clusterInstance.setHosts(latestJobManageHost);
                    clusterInstance.setJobManagerHost(latestJobManageHost);
                    clusterInstanceService.updateById(clusterInstance);
                    if (Asserts.isNotNull(jobInfoDetail.getHistory())) {
                        jobInfoDetail.getHistory().setJobManagerAddress(latestJobManageHost);
                        historyService.updateById(jobInfoDetail.getHistory());
                    }
                }
            }
        }
    }
}
