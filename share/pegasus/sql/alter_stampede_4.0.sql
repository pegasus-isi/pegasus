-- UPDATE THE STAMPEDE DATABASE TO ADD CASCADED DELETES
-- TO BE RUN ONLY BY AN ADMIN on existing V4.0 Stampede SCHEMA DB in MYSQL
-- Example invocation is mysql -u username -p -h hostname databasename < upgrade_stampede_4.0.sql
 
--
-- Table structure for table `file`
--

ALTER TABLE `file` DROP FOREIGN KEY `file_ibfk_1`;
ALTER TABLE `file` ADD CONSTRAINT `file_ibfk_1` FOREIGN KEY (`task_id`) REFERENCES `task` (`task_id`) ON DELETE CASCADE;

--
-- Table structure for table `host`
-- 

ALTER TABLE `host` DROP FOREIGN KEY `host_ibfk_1`;
ALTER TABLE `host` ADD CONSTRAINT `host_ibfk_1` FOREIGN KEY (`wf_id`) REFERENCES `workflow` (`wf_id`) ON DELETE CASCADE;

--
-- Table structure for table `invocation`
-- 

ALTER TABLE `invocation` DROP FOREIGN KEY `invocation_ibfk_1`;
ALTER TABLE `invocation` DROP FOREIGN KEY `invocation_ibfk_2`;
ALTER TABLE `invocation` ADD CONSTRAINT `invocation_ibfk_1` FOREIGN KEY (`job_instance_id`) REFERENCES `job_instance` (`job_instance_id`) ON DELETE CASCADE;
ALTER TABLE `invocation` ADD CONSTRAINT `invocation_ibfk_2` FOREIGN KEY (`wf_id`) REFERENCES `workflow` (`wf_id`) ON DELETE CASCADE;
 
--
-- Table structure for table `job`
-- 

ALTER TABLE `job` DROP FOREIGN KEY `job_ibfk_1`;
ALTER TABLE `job` ADD CONSTRAINT `job_ibfk_1` FOREIGN KEY (`wf_id`) REFERENCES `workflow` (`wf_id`) ON DELETE CASCADE;

--
-- Table structure for table `job_edge`
-- 

ALTER TABLE `job_edge` DROP FOREIGN KEY `job_edge_ibfk_1`;
ALTER TABLE `job_edge` ADD CONSTRAINT `job_edge_ibfk_1` FOREIGN KEY (`wf_id`) REFERENCES `workflow` (`wf_id`) ON DELETE CASCADE;

--
-- Table structure for table `job_instance`
-- 

ALTER TABLE `job_instance` DROP FOREIGN KEY `job_instance_ibfk_1`;
ALTER TABLE `job_instance` ADD CONSTRAINT `job_instance_ibfk_1` FOREIGN KEY (`job_id`) REFERENCES `job` (`job_id`) ON DELETE CASCADE;

--
-- Table structure for table `jobstate`
-- 

ALTER TABLE `jobstate` DROP FOREIGN KEY `jobstate_ibfk_1`;
ALTER TABLE `jobstate` ADD  CONSTRAINT `jobstate_ibfk_1` FOREIGN KEY (`job_instance_id`) REFERENCES `job_instance` (`job_instance_id`) ON DELETE CASCADE;

--
-- Table structure for table `task`
-- 

ALTER TABLE `task` DROP FOREIGN KEY `task_ibfk_2`;
ALTER TABLE `task` ADD CONSTRAINT `task_ibfk_2` FOREIGN KEY (`wf_id`) REFERENCES `workflow` (`wf_id`) ON DELETE CASCADE;

--
-- Table structure for table `task_edge`
-- 

ALTER TABLE `task_edge` DROP FOREIGN KEY `task_edge_ibfk_1`;
ALTER TABLE `task_edge` ADD CONSTRAINT `task_edge_ibfk_1` FOREIGN KEY (`wf_id`) REFERENCES `workflow` (`wf_id`) ON DELETE CASCADE;

--
-- Table structure for table `workflow`
-- 

ALTER TABLE `workflow` DROP FOREIGN KEY `workflow_ibfk_1`;
ALTER TABLE `workflow` ADD CONSTRAINT `workflow_ibfk_1` FOREIGN KEY (`parent_wf_id`) REFERENCES `workflow` (`wf_id`) ON DELETE CASCADE;

--
-- Table structure for table `workflowstate`
-- 

ALTER  TABLE `workflowstate` DROP FOREIGN KEY `workflowstate_ibfk_1`;
ALTER  TABLE `workflowstate` ADD CONSTRAINT `workflowstate_ibfk_1` FOREIGN KEY (`wf_id`) REFERENCES `workflow` (`wf_id`) ON DELETE CASCADE;