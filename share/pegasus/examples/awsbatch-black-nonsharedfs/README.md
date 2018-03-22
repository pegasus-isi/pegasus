Pegasus AWS Batch Example
--------------------------

This example illustrates how to execute a canonical blackdiamond
worklfow using Pegasus on AWS Batch. Starting 4.9 release, Pegasus has
support for executing horizontally clustered jobs on Amazon AWS Batch
Service using the command line tool pegasus-aws-batch. 

## Setup

### Credentials

You need to have both credentials in ~/.aws/credentials and ~/.s3cfg
present for the example to run

### Setting up Container Image which your jobs run on

All jobs in AWS Batch are run in a container via the Amazon EC2
container service. The Amazon EC2 container service does not give
control over the docker run command for a container. Hence, Pegasus
runs jobs on container that is based on the [Amazon Fetch and Run
Example](https://aws.amazon.com/blogs/compute/creating-a-simple-fetch-and-run-aws-batch-job/)
. This container image allows us to fetch user executables
automatically from S3. All container images referred used for Pegasus
workflows must be based on the above example. Additionally, the Docker
file for your container image should include these additional Docker
run commands to install the yum packages that Pegasus requires.

```dockerfile
RUN yum -y install perl findutils
```

### One time setup

Follow the various steps listed in the  guide [Setting up with AWS
Batch](https://docs.aws.amazon.com/batch/latest/userguide/get-set-up-for-aws-batch.html).

If you are using AWS Batch for the very first time, then you need to
use the Amazon Web console to create a role with your user that will
give the AWS Batch services privileges to execute to access other AWS
services such as EC2 Container Service , CloudWatchLogs etc.

The following roles need to be created
1. AWS Batch Service IAM Role: For convenience and ease of use make
sure you name the role AWSBatchServiceRole , so that you don't have to
make other changes.  Complete the procedures listed at [AWS Batch Service IAM Role](https://docs.aws.amazon.com/batch/latest/userguide/service_IAM_role.html). 

2. Amazon ECS Instance Role before creating your first compute
environment. AWS Batch compute environments are populated with Amazon
ECS container instances, and they run the Amazon ECS container agent
locally. The Amazon ECS container agent makes calls to various AWS
APIs on your behalf, so container instances that run the agent require
an IAM policy and role for these services to know that the agent 
belongs to you. Complete the procedures listed at [Amazon ECS Instance
Role](https://docs.aws.amazon.com/batch/latest/userguide/instance_IAM_role.html)

3. Whenever a Pegasus job runs via AWS Batch it needs to fetch data
from S3 and push data back to S3. To create this job role follow the
instructions at section "Create an IAM role" in [Amazon Fetch and Run
Example](https://aws.amazon.com/blogs/compute/creating-a-simple-fetch-and-run-aws-batch-job/) 
Note: batchJobRole should have full write access to S3 i.e have the
policy AmazonS3FullAccess attached to it.


### Setup of AWS Batch Entities for the workflow
AWS Batch has a notion of 
1. Job Definition - job definition is something that allows you to
use your container image in Amazon EC2 Repository to run one or many
AWS Batch jobs. 
2. Compute Environment - what sort of compute nodes you want your jobs
to run on.
3. Job Queue - the queue that feeds the jobs to a compute environment.

Currently, with Pegasus you can only use one of each for a workflow
i.e the same job definition, compute environment and job queue need to
be used for all jobs in the workflow. 

To create these we will use the pegasus-aws-batch client and edit the
sample files in conf/ directory before proceeding
* sample-job-definition.json : Edit the attribute named image and replace
it with the ARN of the container image you built for your account
* sample-compute-env.json : Edit the attributes subnets and
securityGroupIds 

Update the pegasusrc with your aws account and aws region to use
```ini
pegasus.aws.region=us-west-2
pegasus.aws.account=[your aws account id - digits]
```
Run the command pegasus-aws-batch 
```bash
$  pegasus-aws-batch --conf ./conf/pegasusrc --prefix pegasus-awsbatch-example --create --compute-environment ./conf/sample-compute-env.json --job-definition ./conf/sample-job-definition.json --job-queue ./conf/sample-job-queue.json 


..

2018-01-18 15:16:00.771 INFO  [Synch] Created Job Definition
arn:aws:batch:us-west-2:405596411149:job-definition/pegasus-awsbatch-example-job-definition:1
2018-01-18 15:16:07.034 INFO  [Synch] Created Compute Environment
arn:aws:batch:us-west-2:XXXXXXXXXX:compute-environment/pegasus-awsbatch-example-compute-env
2018-01-18 15:16:11.291 INFO  [Synch] Created Job Queue
arn:aws:batch:us-west-2:XXXXXXXXXX:job-queue/pegasus-awsbatch-example-job-queue

2018-01-18 15:16:11.292 INFO  [PegasusAWSBatch] Time taken to execute
is 12.194 seconds

```


## Run the example

Once you have completed the setup , you can run the example using the
submit script.


