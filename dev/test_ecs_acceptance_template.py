import json
import unittest

from ecs_acceptance_template import att, policy, ref, statement, sub, template


class IsolationTemplateTest(unittest.TestCase):
    def setUp(self):
        self.t = template("monitor-ecs-acceptance-fixture", "https://fixture.trycloudflare.com", "2026-09-09T23:00:00",
                          {"nginx": "nginx-fixture", "reject": "reject-fixture", "synthetic": "producer-fixture"})
        self.r = self.t["Resources"]

    def test_no_production_or_paid_gateway(self):
        raw = json.dumps(self.t)
        for forbidden in ["nexusapi-prod", "Fn::ImportValue", "AWS::RDS", "AWS::ElasticLoadBalancing", "AWS::EC2::NatGateway", "AWS::EC2::VPCPeeringConnection", "AdministratorAccess"]:
            self.assertNotIn(forbidden, raw)
        sg = self.r["SecurityGroup"]["Properties"]
        self.assertEqual(sg["VpcId"], {"Ref": "Vpc"})
        self.assertEqual(sg["SecurityGroupIngress"], [])
        self.assertEqual(sg["SecurityGroupEgress"][0]["ToPort"], 443)

    def test_zero_start_bounded_compute_stop_order(self):
        service = self.r["Service"]["Properties"]
        self.assertEqual(service["DesiredCount"], 0)
        self.assertNotIn("LoadBalancers", service)
        self.assertEqual(service["DeploymentConfiguration"]["MaximumPercent"], 100)
        task = self.r["TaskDefinition"]["Properties"]
        self.assertEqual((task["Cpu"], task["Memory"]), ("512", "1024"))
        containers = {c["Name"]: c for c in task["ContainerDefinitions"]}
        self.assertEqual(containers["synthetic"]["DependsOn"], [{"ContainerName": n, "Condition": "START"} for n in ["nginxcollector", "rejectcollector"]])
        for name in ["nginxcollector", "rejectcollector"]:
            self.assertEqual(containers[name]["DependsOn"], [{"ContainerName": "init", "Condition": "SUCCESS"}])
            self.assertTrue(containers[name]["ReadonlyRootFilesystem"])
            self.assertEqual(containers[name]["StopTimeout"], 45)
            env = {entry["Name"]: entry["Value"] for entry in containers[name]["Environment"]}
            self.assertEqual(env["ECSLOG_ARCHIVE_CLOSURE"], "true")
            self.assertEqual(env["ECSLOG_FINAL_LOG_ROOT"], "/logs")
            logs = [m for m in containers[name]["MountPoints"] if m["ContainerPath"] == "/logs"]
            self.assertEqual(len(logs), 1)
            self.assertTrue(logs[0]["ReadOnly"])

    def test_roles_and_scheduled_stop_are_scoped(self):
        self.assertEqual(self.r["RegisterMethod"]["Properties"]["AuthorizationType"], "AWS_IAM")
        stop = self.r["StopRole"]["Properties"]["Policies"][0]["PolicyDocument"]["Statement"]
        self.assertEqual(len(stop), 1)
        self.assertEqual(stop[0]["Action"], "ecs:UpdateService")
        self.assertIn("synthetic-collector-test", str(stop[0]["Resource"]))
        schedule = self.r["StopSchedule"]["Properties"]
        self.assertEqual(schedule["State"], "ENABLED")
        self.assertEqual(json.loads(schedule["Target"]["Input"]), {
            "Cluster": "monitor-ecs-acceptance-fixture", "Service": "synthetic-collector-test", "DesiredCount": 0})
        self.assertNotIn("ReservedConcurrentExecutions", self.r["Bridge"]["Properties"])
        self.assertIn("${aws:userid}", str(self.r["TaskRole"]))

    def test_reject_unscoped_inputs(self):
        with self.assertRaises(ValueError):
            template("nexusapi-prod-cluster", "https://fixture.trycloudflare.com", "2026-09-09T23:00:00", {})

    def test_task_role_exact_archive_and_registration_permissions(self):
        role = self.r["TaskRole"]["Properties"]
        self.assertNotIn("ManagedPolicyArns", role)
        self.assertEqual(role["Policies"], [{"PolicyName": "isolated-only", "PolicyDocument": policy([
            statement(["s3:PutObject", "s3:GetObject"],
                      {"Fn::Join": ["", [att("Archive"), "/isolated/${aws:userid}/*"]]})])}])
        registration = self.r["RegisterPolicy"]["Properties"]
        self.assertEqual(registration["Roles"], [ref("TaskRole")])
        self.assertEqual(registration["PolicyDocument"], policy([
            statement("execute-api:Invoke", sub("arn:aws:execute-api:${AWS::Region}:${AWS::AccountId}:${Api}/isolated/POST/register"))]))
        self.assertEqual(role["AssumeRolePolicyDocument"]["Statement"][0]["Condition"], {
            "StringEquals": {"aws:SourceAccount": ref("AWS::AccountId")},
            "ArnLike": {"aws:SourceArn": sub("arn:aws:ecs:${AWS::Region}:${AWS::AccountId}:*")}})

    def test_no_unreviewed_identity_policies(self):
        roles = {k for k, v in self.r.items() if v["Type"] == "AWS::IAM::Role"}
        self.assertEqual(roles, {"TaskRole", "ExecutionRole", "BridgeRole", "ReceiverRole", "StopRole"})
        policies = {k for k, v in self.r.items() if v["Type"] in {"AWS::IAM::Policy", "AWS::IAM::ManagedPolicy"}}
        self.assertEqual(policies, {"RegisterPolicy"})
        for name in roles:
            self.assertEqual(len(self.r[name]["Properties"]["Policies"]), 1)
            self.assertNotIn("ManagedPolicyArns", self.r[name]["Properties"])
        self.assertEqual(self.r["StopRole"]["Properties"]["Policies"][0]["PolicyDocument"], policy([
            statement("ecs:UpdateService", sub("arn:aws:ecs:${AWS::Region}:${AWS::AccountId}:service/monitor-ecs-acceptance-fixture/synthetic-collector-test"))]))

    def test_archive_cannot_be_public_or_accept_plaintext_transport(self):
        archive = self.r["Archive"]["Properties"]
        self.assertEqual(archive["PublicAccessBlockConfiguration"], {key: True for key in (
            "BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy", "RestrictPublicBuckets")})
        self.assertEqual(archive["BucketEncryption"], {"ServerSideEncryptionConfiguration": [
            {"ServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]})
        self.assertEqual(self.r["ArchivePolicy"]["Properties"]["PolicyDocument"], policy([
            {"Effect": "Deny", "Principal": "*", "Action": "s3:*",
             "Resource": [att("Archive"), sub("${Archive.Arn}/*")],
             "Condition": {"Bool": {"aws:SecureTransport": "false"}}}]))

    def test_execution_role_cannot_fetch_business_secrets_or_other_images(self):
        role = self.r["ExecutionRole"]["Properties"]
        self.assertNotIn("ManagedPolicyArns", role)
        self.assertEqual(role["Policies"][0]["PolicyDocument"], policy([
            statement("ecr:GetAuthorizationToken", "*"),
            statement(["ecr:BatchCheckLayerAvailability", "ecr:GetDownloadUrlForLayer", "ecr:BatchGetImage"], att("Repository")),
            statement(["logs:CreateLogStream", "logs:PutLogEvents"], att("TaskLogs")),
            statement("secretsmanager:GetSecretValue", ref("EvidenceSecret"))]))

    def test_bridge_cannot_read_archive_or_change_ecs(self):
        role = self.r["BridgeRole"]["Properties"]
        self.assertNotIn("ManagedPolicyArns", role)
        self.assertEqual(role["Policies"][0]["PolicyDocument"], policy([
            statement("secretsmanager:GetSecretValue", ref("BridgeSecret")),
            statement(["logs:CreateLogStream", "logs:PutLogEvents"], att("BridgeLogs"))]))
        invoke = self.r["BridgeInvoke"]["Properties"]
        self.assertEqual(invoke["SourceAccount"], ref("AWS::AccountId"))
        self.assertEqual(invoke["SourceArn"], sub("arn:aws:execute-api:${AWS::Region}:${AWS::AccountId}:${Api}/isolated/POST/register"))

    def test_receiver_has_only_test_scoped_read_permissions(self):
        role = self.r["ReceiverRole"]["Properties"]
        self.assertNotIn("ManagedPolicyArns", role)
        cluster = "monitor-ecs-acceptance-fixture"
        self.assertEqual(role["Policies"][0]["PolicyDocument"], policy([
            statement("ecs:DescribeTasks", sub("arn:aws:ecs:${AWS::Region}:${AWS::AccountId}:task/" + cluster + "/*")),
            {**statement("ecs:ListTasks", "*"), "Condition": {"ArnEquals": {"ecs:cluster": sub("arn:aws:ecs:${AWS::Region}:${AWS::AccountId}:cluster/" + cluster)}}},
            statement("ecs:DescribeTaskDefinition", "*"),
            {**statement("s3:ListBucket", att("Archive")), "Condition": {"StringLike": {"s3:prefix": "isolated/*"}}},
            statement("s3:GetObject", sub("${Archive.Arn}/isolated/*"))]))

    def test_legacy_synthetic_template_is_not_two_producer_acceptance(self):
        from ecs_task_contract import check_task
        with self.assertRaisesRegex(ValueError, "five-container layout"):
            check_task(self.r["TaskDefinition"]["Properties"])


if __name__ == "__main__":
    unittest.main()
