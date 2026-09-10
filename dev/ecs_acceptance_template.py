"""Isolated acceptance CloudFormation, no imported production resources.

Creating the stack leaves DesiredCount=0. Starting tasks is a separate gate.
"""
import json
import re
from pathlib import Path


def ref(name):
    return {"Ref": name}


def att(name, field="Arn"):
    return {"Fn::GetAtt": [name, field]}


def sub(value):
    return {"Fn::Sub": value}


def statement(actions, resources):
    return {"Effect": "Allow", "Action": actions, "Resource": resources}


def policy(statements):
    return {"Version": "2012-10-17", "Statement": statements}


def template(name, monitor_url, deadline, images):
    if not re.fullmatch(r"monitor-ecs-acceptance-[a-z0-9-]{1,20}", name):
        raise ValueError("test namespace required")
    if not re.fullmatch(r"https://[a-z0-9-]+\.trycloudflare\.com", monitor_url):
        raise ValueError("dedicated temporary receiver origin required")
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", deadline):
        raise ValueError("explicit UTC deadline required")
    if set(images) != {"nginx", "reject", "synthetic"}:
        raise ValueError("three exact test image tags required")
    for image in images.values():
        if not re.fullmatch(r"[a-z0-9][a-z0-9._-]{1,80}", image):
            raise ValueError("invalid test image tag")
    resources = {}
    tags = [{"Key": "MonitorAcceptanceRun", "Value": name}]

    def add(key, kind, properties, **extra):
        resources[key] = {"Type": kind, "Properties": properties, **extra}

    def role(key, service, statements):
        trust = {"Effect": "Allow", "Principal": {"Service": service}, "Action": "sts:AssumeRole"}
        if service == "ecs-tasks.amazonaws.com":
            trust["Condition"] = {"StringEquals": {"aws:SourceAccount": ref("AWS::AccountId")},
                                  "ArnLike": {"aws:SourceArn": sub("arn:aws:ecs:${AWS::Region}:${AWS::AccountId}:*")}}
        elif service == "scheduler.amazonaws.com":
            trust["Condition"] = {"StringEquals": {"aws:SourceAccount": ref("AWS::AccountId")},
                                  "ArnEquals": {"aws:SourceArn": sub("arn:aws:scheduler:${AWS::Region}:${AWS::AccountId}:schedule-group/default")}}
        add(key, "AWS::IAM::Role", {"RoleName": name + "-" + key.lower(), "Tags": tags,
            "AssumeRolePolicyDocument": policy([trust]),
            "Policies": [{"PolicyName": "isolated-only", "PolicyDocument": policy(statements)}]})

    cluster_arn = sub("arn:aws:ecs:${AWS::Region}:${AWS::AccountId}:cluster/" + name)
    service_arn = sub("arn:aws:ecs:${AWS::Region}:${AWS::AccountId}:service/" + name + "/synthetic-collector-test")
    add("Vpc", "AWS::EC2::VPC", {"CidrBlock": "10.231.0.0/24", "EnableDnsSupport": True, "EnableDnsHostnames": True, "Tags": tags})
    add("InternetGateway", "AWS::EC2::InternetGateway", {"Tags": tags})
    add("GatewayAttachment", "AWS::EC2::VPCGatewayAttachment", {"VpcId": ref("Vpc"), "InternetGatewayId": ref("InternetGateway")})
    add("Subnet", "AWS::EC2::Subnet", {"VpcId": ref("Vpc"), "CidrBlock": "10.231.0.0/26",
        "AvailabilityZone": {"Fn::Select": [0, {"Fn::GetAZs": ""}]}, "Tags": tags})
    add("RouteTable", "AWS::EC2::RouteTable", {"VpcId": ref("Vpc"), "Tags": tags})
    add("InternetRoute", "AWS::EC2::Route", {"RouteTableId": ref("RouteTable"), "DestinationCidrBlock": "0.0.0.0/0", "GatewayId": ref("InternetGateway")}, DependsOn="GatewayAttachment")
    add("RouteAssociation", "AWS::EC2::SubnetRouteTableAssociation", {"RouteTableId": ref("RouteTable"), "SubnetId": ref("Subnet")})
    add("SecurityGroup", "AWS::EC2::SecurityGroup", {"VpcId": ref("Vpc"), "GroupDescription": "Synthetic acceptance: no inbound, HTTPS outbound only",
        "SecurityGroupIngress": [], "SecurityGroupEgress": [{"IpProtocol": "tcp", "FromPort": 443, "ToPort": 443, "CidrIp": "0.0.0.0/0"}], "Tags": tags})
    add("Cluster", "AWS::ECS::Cluster", {"ClusterName": name, "ClusterSettings": [{"Name": "containerInsights", "Value": "disabled"}], "Tags": tags})
    add("Archive", "AWS::S3::Bucket", {"BucketName": sub(name + "-${AWS::AccountId}"), "Tags": tags,
        "PublicAccessBlockConfiguration": {k: True for k in ["BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy", "RestrictPublicBuckets"]},
        "OwnershipControls": {"Rules": [{"ObjectOwnership": "BucketOwnerEnforced"}]},
        "BucketEncryption": {"ServerSideEncryptionConfiguration": [{"ServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]},
        "LifecycleConfiguration": {"Rules": [{"Id": "test-expiry", "Status": "Enabled", "ExpirationInDays": 1, "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1}}]}})
    add("ArchivePolicy", "AWS::S3::BucketPolicy", {"Bucket": ref("Archive"), "PolicyDocument": policy([
        {"Effect": "Deny", "Principal": "*", "Action": "s3:*", "Resource": [att("Archive"), sub("${Archive.Arn}/*")], "Condition": {"Bool": {"aws:SecureTransport": "false"}}}])})
    add("Repository", "AWS::ECR::Repository", {"RepositoryName": name, "ImageTagMutability": "IMMUTABLE", "EmptyOnDelete": True,
        "EncryptionConfiguration": {"EncryptionType": "AES256"}, "ImageScanningConfiguration": {"ScanOnPush": False}, "Tags": tags})
    for key in ["BridgeSecret", "EvidenceSecret"]:
        add(key, "AWS::SecretsManager::Secret", {"Name": name + "-" + key.lower(), "GenerateSecretString": {"PasswordLength": 48, "ExcludePunctuation": True}, "Tags": tags})
    add("TaskLogs", "AWS::Logs::LogGroup", {"LogGroupName": "/monitor/" + name, "RetentionInDays": 1, "Tags": tags})
    add("BridgeLogs", "AWS::Logs::LogGroup", {"LogGroupName": "/aws/lambda/" + name + "-bridge", "RetentionInDays": 1, "Tags": tags})
    role("TaskRole", "ecs-tasks.amazonaws.com", [statement(["s3:PutObject", "s3:GetObject"],
        {"Fn::Join": ["", [att("Archive"), "/isolated/${aws:userid}/*"]]})])
    role("ExecutionRole", "ecs-tasks.amazonaws.com", [statement("ecr:GetAuthorizationToken", "*"),
        statement(["ecr:BatchCheckLayerAvailability", "ecr:GetDownloadUrlForLayer", "ecr:BatchGetImage"], att("Repository")),
        statement(["logs:CreateLogStream", "logs:PutLogEvents"], att("TaskLogs")),
        statement("secretsmanager:GetSecretValue", ref("EvidenceSecret"))])
    role("BridgeRole", "lambda.amazonaws.com", [statement("secretsmanager:GetSecretValue", ref("BridgeSecret")),
        statement(["logs:CreateLogStream", "logs:PutLogEvents"], att("BridgeLogs"))])
    add("ReceiverRole", "AWS::IAM::Role", {"RoleName": name + "-receiver", "Tags": tags,
        "AssumeRolePolicyDocument": policy([{"Effect": "Allow", "Principal": {"AWS": sub("arn:aws:iam::${AWS::AccountId}:user/yanglei")}, "Action": "sts:AssumeRole"}]),
        "Policies": [{"PolicyName": "read-only-test", "PolicyDocument": policy([
            statement("ecs:DescribeTasks", sub("arn:aws:ecs:${AWS::Region}:${AWS::AccountId}:task/" + name + "/*")),
            {**statement("ecs:ListTasks", "*"), "Condition": {"ArnEquals": {"ecs:cluster": cluster_arn}}},
            statement("ecs:DescribeTaskDefinition", "*"),
            {**statement("s3:ListBucket", att("Archive")), "Condition": {"StringLike": {"s3:prefix": "isolated/*"}}},
            statement("s3:GetObject", sub("${Archive.Arn}/isolated/*"))])}]})
    add("Api", "AWS::ApiGateway::RestApi", {"Name": name + "-registration", "EndpointConfiguration": {"Types": ["REGIONAL"]}, "Tags": tags})
    add("RegisterResource", "AWS::ApiGateway::Resource", {"RestApiId": ref("Api"), "ParentId": att("Api", "RootResourceId"), "PathPart": "register"})
    bridge_code = (Path(__file__).resolve().parents[1] / "deploy/ecs-log-bridge/handler.py").read_text()
    add("Bridge", "AWS::Lambda::Function", {"FunctionName": name + "-bridge", "Runtime": "python3.13", "Architectures": ["arm64"],
        "Handler": "index.handler", "Role": att("BridgeRole"), "Timeout": 15, "MemorySize": 128,
        "Code": {"ZipFile": bridge_code}, "Tags": tags,
        "Environment": {"Variables": {"ECS_LOG_SCOPE": "isolated", "ECS_LOG_API_ID": ref("Api"), "ECS_LOG_STAGE": "isolated",
            "ECS_LOG_ACCOUNT_ID": ref("AWS::AccountId"), "ECS_LOG_BRIDGE_SECRET_ARN": ref("BridgeSecret"), "ECS_LOG_MONITOR_REGISTER_URL": monitor_url + "/internal/ecs/v1/register"}}}, DependsOn="BridgeLogs")
    add("BridgeInvoke", "AWS::Lambda::Permission", {"Action": "lambda:InvokeFunction", "FunctionName": ref("Bridge"), "Principal": "apigateway.amazonaws.com",
        "SourceAccount": ref("AWS::AccountId"), "SourceArn": sub("arn:aws:execute-api:${AWS::Region}:${AWS::AccountId}:${Api}/isolated/POST/register")})
    add("RegisterMethod", "AWS::ApiGateway::Method", {"RestApiId": ref("Api"), "ResourceId": ref("RegisterResource"), "HttpMethod": "POST", "AuthorizationType": "AWS_IAM",
        "Integration": {"Type": "AWS_PROXY", "IntegrationHttpMethod": "POST", "TimeoutInMillis": 20000,
            "Uri": sub("arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${Bridge.Arn}/invocations")}})
    add("ApiDeployment", "AWS::ApiGateway::Deployment", {"RestApiId": ref("Api")}, DependsOn="RegisterMethod")
    add("ApiStage", "AWS::ApiGateway::Stage", {"RestApiId": ref("Api"), "DeploymentId": ref("ApiDeployment"), "StageName": "isolated",
        "MethodSettings": [{"ResourcePath": "/*", "HttpMethod": "*", "ThrottlingBurstLimit": 10, "ThrottlingRateLimit": 5}], "Tags": tags})
    add("RegisterPolicy", "AWS::IAM::Policy", {"PolicyName": "isolated-registration", "Roles": [ref("TaskRole")],
        "PolicyDocument": policy([statement("execute-api:Invoke", sub("arn:aws:execute-api:${AWS::Region}:${AWS::AccountId}:${Api}/isolated/POST/register"))])})

    def mount(volume, path, ro=False):
        return {"SourceVolume": volume, "ContainerPath": path, "ReadOnly": ro}

    def container(cname, image, env, mounts):
        return {"Name": cname, "Image": sub("${Repository.RepositoryUri}:" + image), "Essential": True, "User": "100:101",
                "ReadonlyRootFilesystem": True, "StopTimeout": 45, "LinuxParameters": {"Capabilities": {"Drop": ["ALL"]}},
                "Environment": [{"Name": k, "Value": v} for k, v in env.items()], "MountPoints": mounts,
                "LogConfiguration": {"LogDriver": "awslogs", "Options": {"awslogs-group": ref("TaskLogs"), "awslogs-region": ref("AWS::Region"), "awslogs-stream-prefix": "test", "mode": "non-blocking", "max-buffer-size": "1m"}}}

    base_env = {"ECSLOG_SCOPE": "isolated", "ECSLOG_SERVICE_ARN": service_arn, "ECSLOG_PRODUCER_CONTAINER": "synthetic",
                "ECSLOG_MONITOR_URL": monitor_url, "ECSLOG_REGISTER_URL": sub("https://${Api}.execute-api.${AWS::Region}.amazonaws.com/isolated/register"),
                "ECSLOG_AUDIENCE": name, "ECSLOG_STATE_ROOT": "/data/ecs", "AWS_REGION": ref("AWS::Region"),
                "ECSLOG_ARCHIVE_ENABLED": "true", "ECSLOG_ARCHIVE_BUCKET": ref("Archive"), "ECSLOG_ARCHIVE_PREFIX": "isolated/", "ECSLOG_DEFERRED_ARCHIVE_ACK": "true",
                "ECSLOG_ARCHIVE_CLOSURE": "true", "ECSLOG_FINAL_LOG_ROOT": "/logs"}
    init = container("init", images["synthetic"], {}, [mount("Logs", "/logs"), mount("NginxState", "/state-nginx"), mount("RejectState", "/state-reject")])
    init.pop("LinuxParameters")  # Fargate does not permit adding CHOWN back after DROP ALL.
    init.update({"Essential": False, "User": "0:0",
                 "EntryPoint": ["/bin/sh", "-ec"], "Command": ["chown 100:101 /logs /state-nginx /state-reject; chmod 755 /logs; chmod 700 /state-nginx /state-reject"]})
    nginx = container("nginxcollector", images["nginx"], {**base_env, "ECSLOG_KIND": "nginx", "NGINXCOLLECTOR_LOG_PATH": "/logs/access.jsonl",
        "NGINXCOLLECTOR_ERROR_LOG_PATH": "/logs/error.log", "NGINXCOLLECTOR_ERROR_TIMEZONE": "UTC", "NGINXCOLLECTOR_INTERVAL_SECONDS": "5",
        "NGINXCOLLECTOR_EVIDENCE_HMAC_KEY_ID": "isolated-v1"}, [mount("Logs", "/logs", True), mount("NginxState", "/data/ecs")])
    nginx["Secrets"] = [{"Name": "NGINXCOLLECTOR_EVIDENCE_HMAC_KEY", "ValueFrom": ref("EvidenceSecret")}]
    reject = container("rejectcollector", images["reject"], {**base_env, "ECSLOG_KIND": "reject", "COLLECTOR_LOG_GLOB": "/logs/new-api.log",
        "COLLECTOR_LOG_TIMEZONE": "UTC", "COLLECTOR_FLUSH_SECONDS": "5"}, [mount("Logs", "/logs", True), mount("RejectState", "/data/ecs")])
    for c in [nginx, reject]:
        c["DependsOn"] = [{"ContainerName": "init", "Condition": "SUCCESS"}]
    producer = container("synthetic", images["synthetic"], {"ECSLOG_SCOPE": "isolated"}, [mount("Logs", "/logs")])
    # Reverse stop dependencies: producer exits BEFORE collectors receive TERM.
    producer["DependsOn"] = [{"ContainerName": c, "Condition": "START"} for c in ["nginxcollector", "rejectcollector"]]
    producer["StopTimeout"] = 10
    add("TaskDefinition", "AWS::ECS::TaskDefinition", {"Family": name, "Cpu": "512", "Memory": "1024", "NetworkMode": "awsvpc",
        "RequiresCompatibilities": ["FARGATE"], "RuntimePlatform": {"CpuArchitecture": "X86_64", "OperatingSystemFamily": "LINUX"},
        "TaskRoleArn": att("TaskRole"), "ExecutionRoleArn": att("ExecutionRole"), "ContainerDefinitions": [init, nginx, reject, producer],
        "Volumes": [{"Name": n} for n in ["Logs", "NginxState", "RejectState"]], "Tags": tags})
    add("Service", "AWS::ECS::Service", {"ServiceName": "synthetic-collector-test", "Cluster": ref("Cluster"), "TaskDefinition": ref("TaskDefinition"),
        "DesiredCount": 0, "LaunchType": "FARGATE", "PlatformVersion": "1.4.0", "EnableExecuteCommand": False,
        "DeploymentConfiguration": {"MaximumPercent": 100, "MinimumHealthyPercent": 0, "DeploymentCircuitBreaker": {"Enable": True, "Rollback": False}},
        "NetworkConfiguration": {"AwsvpcConfiguration": {"AssignPublicIp": "ENABLED", "Subnets": [ref("Subnet")], "SecurityGroups": [ref("SecurityGroup")]}},
        "PropagateTags": "SERVICE", "Tags": tags}, DependsOn=["InternetRoute", "RouteAssociation", "RegisterPolicy", "ApiStage", "BridgeInvoke"])
    role("StopRole", "scheduler.amazonaws.com", [statement("ecs:UpdateService", service_arn)])
    add("StopSchedule", "AWS::Scheduler::Schedule", {"Name": name + "-stop", "State": "ENABLED", "ScheduleExpression": "at(" + deadline + ")",
        "ScheduleExpressionTimezone": "UTC", "FlexibleTimeWindow": {"Mode": "OFF"}, "Target": {"Arn": "arn:aws:scheduler:::aws-sdk:ecs:updateService", "RoleArn": att("StopRole"),
            "Input": json.dumps({"Cluster": name, "Service": "synthetic-collector-test", "DesiredCount": 0}),
            "RetryPolicy": {"MaximumRetryAttempts": 3, "MaximumEventAgeInSeconds": 300}}}, DependsOn="Service")
    outputs = {k: {"Value": v} for k, v in {"Cluster": cluster_arn, "Service": service_arn, "Bucket": ref("Archive"),
        "Repository": att("Repository", "RepositoryUri"), "TaskRole": att("TaskRole"), "ReceiverRole": att("ReceiverRole"),
        "BridgeSecret": ref("BridgeSecret"), "EvidenceSecret": ref("EvidenceSecret"), "TaskLogs": ref("TaskLogs"),
        "RegisterURL": sub("https://${Api}.execute-api.${AWS::Region}.amazonaws.com/isolated/register"), "StopSchedule": ref("StopSchedule"), "Vpc": ref("Vpc")}.items()}
    return {"AWSTemplateFormatVersion": "2010-09-09", "Description": "Synthetic-only Monitor ECS acceptance; zero tasks initially; no production imports", "Resources": resources, "Outputs": outputs}
