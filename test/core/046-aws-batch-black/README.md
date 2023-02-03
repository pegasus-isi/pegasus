The DockerFile in the directory builds the container that we use for AWS Batch
Relies on the fetch and run script to setup Pegasus jobs
https://aws.amazon.com/blogs/compute/creating-a-simple-fetch-and-run-aws-batch-job/

# Building and pushing the docker container

## authenticating docker client to your aws ecr registry.
aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin xxxxxxxx.dkr.ecr.us-west-2.amazonaws.com

## tag the local image to aws ecr naming scheme
docker tag awsbatch/pegasus_minimal:latest xxxxxxxx.dkr.ecr.us-west-2.amazonaws.com/awsbatch/pegasus-minimal

## push the image
docker push xxxxxxxx.dkr.ecr.us-west-2.amazonaws.com/awsbatch/pegasus-minimal

