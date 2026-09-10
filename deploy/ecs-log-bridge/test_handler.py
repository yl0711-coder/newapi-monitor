import base64
import copy
import json
import io
import unittest
from unittest.mock import patch

import handler


class BridgeTests(unittest.TestCase):
    def setUp(self):
        self.settings = {"ECS_LOG_SCOPE": "isolated", "ECS_LOG_API_ID": "fixture-api", "ECS_LOG_STAGE": "isolated", "ECS_LOG_ACCOUNT_ID": "123456789012"}
        self.body = {"service_arn": "fixture-service", "task_arn": "fixture-task", "container": "nginx", "runtime_id": "runtime", "lane": "access", "public_key": "fixture"}
        self.caller = "arn:aws:sts::123456789012:assumed-role/fixture/" + "a" * 32
        self.event = {"resource": "/register", "httpMethod": "POST", "body": json.dumps(self.body), "requestContext": {
            "apiId": "fixture-api", "stage": "isolated", "accountId": "123456789012", "httpMethod": "POST", "resourcePath": "/register", "identity": {"userArn": self.caller}}}

    def test_principal_comes_from_context_not_headers(self):
        self.event["headers"] = {"caller_arn": "forged", "X-Caller-ARN": "forged"}
        self.assertEqual(handler.trusted_registration(self.event, self.settings)["caller_arn"], self.caller)
        self.body["caller_arn"] = self.caller
        self.event["body"] = json.dumps(self.body)
        with self.assertRaises(ValueError):
            handler.trusted_registration(self.event, self.settings)

    def test_base64_and_strict_json(self):
        self.event["isBase64Encoded"] = True
        self.event["body"] = base64.b64encode(json.dumps(self.body).encode()).decode()
        self.assertEqual(handler.trusted_registration(self.event, self.settings)["lane"], "access")
        for raw in ('{"lane":"access","lane":"error"}', "[]", "null", "x" * (handler.MAX_BODY + 1)):
            with self.subTest(raw=raw[:40]):
                self.event["isBase64Encoded"] = False
                self.event["body"] = raw
                with self.assertRaises(ValueError):
                    handler.trusted_registration(self.event, self.settings)

    def test_wrong_gateway_or_iam_context_rejected(self):
        for field, value in (("apiId", "other"), ("stage", "prod"), ("accountId", "other"), ("identity", {}),
                             ("httpMethod", "GET"), ("resourcePath", "/other"), ("authorizer", {"userArn": self.caller})):
            with self.subTest(field=field):
                event = copy.deepcopy(self.event)
                event["requestContext"][field] = value
                with self.assertRaises(PermissionError):
                    handler.trusted_registration(event, self.settings)

    def test_no_forward_on_unauthenticated_input(self):
        self.event["requestContext"]["identity"] = {}
        with patch.dict(handler.os.environ, self.settings, clear=True), patch.object(handler, "forward") as forward:
            self.assertEqual(handler.handler(self.event, None)["statusCode"], 403)
            forward.assert_not_called()

    def test_network_errors_do_not_expose_secrets(self):
        with patch.dict(handler.os.environ, self.settings, clear=True), patch.object(handler, "forward", side_effect=RuntimeError("DO-NOT-LEAK")):
            response = handler.handler(self.event, None)
            self.assertEqual(response["statusCode"], 503)
            self.assertNotIn("DO-NOT-LEAK", json.dumps(response))

    def test_forward_uses_only_fixed_endpoint_and_redacts_errors(self):
        settings = dict(self.settings, ECS_LOG_MONITOR_REGISTER_URL="https://monitor.example/internal/ecs/v1/register")
        for code, expected in ((401, 401), (409, 409), (429, 429), (302, 502), (500, 502)):
            with self.subTest(code=code):
                error = handler.urllib.error.HTTPError(settings["ECS_LOG_MONITOR_REGISTER_URL"], code, "DO-NOT-LEAK", {}, io.BytesIO(b"DO-NOT-LEAK"))
                with patch.object(handler, "bridge_secret", return_value="fixture-bridge-secret"), patch.object(handler.urllib.request, "build_opener") as build:
                    build.return_value.open.side_effect = error
                    response = handler.forward(self.body, settings)
                    self.assertEqual(response["statusCode"], expected)
                    self.assertNotIn("DO-NOT-LEAK", json.dumps(response))
                    request = build.return_value.open.call_args.args[0]
                    self.assertEqual(request.full_url, settings["ECS_LOG_MONITOR_REGISTER_URL"])
                    self.assertEqual(request.get_header("Authorization"), "Bearer fixture-bridge-secret")

    def test_unsafe_monitor_url_never_reads_secret(self):
        for endpoint in ("http://monitor.example/internal/ecs/v1/register", "https://user:secret@monitor.example/internal/ecs/v1/register", "https://monitor.example/other", "https://monitor.example/internal/ecs/v1/register?next=evil"):
            with patch.object(handler, "bridge_secret") as secret:
                with self.assertRaises(ValueError):
                    handler.forward(self.body, dict(self.settings, ECS_LOG_MONITOR_REGISTER_URL=endpoint))
                secret.assert_not_called()

    def test_registration_capabilities_preserved_with_legacy_compatibility(self):
        base = {"ok": True, "version": 1, "node": "fixture", "lane": "access", "audience": "isolated", "lease_until": 123}
        cases = [({}, True), ({"archive_closure_v2": True, "final_boundary_v1": True}, True),
                 ({"archive_closure_v2": False}, True), ({"final_boundary_v1": "true"}, False),
                 ({"final_boundary_v1": 1}, False), ({"unexpected": True}, False),
                 ({"final_newapi_files_v1": True}, True), ({"final_newapi_files_v1": False}, True),
                 ({"final_newapi_files_v1": "true"}, False), ({"final_newapi_files_v1": 1}, False)]
        for extra, valid in cases:
            with self.subTest(extra=extra):
                ack = dict(base, **extra)
                with patch.object(handler, "bridge_secret", return_value="fixture"), patch.object(handler.urllib.request, "build_opener") as build:
                    response = build.return_value.open.return_value
                    response.status = 200
                    response.read.return_value = json.dumps(ack).encode()
                    settings = dict(self.settings, ECS_LOG_MONITOR_REGISTER_URL="https://monitor.example/internal/ecs/v1/register")
                    if valid:
                        self.assertEqual(json.loads(handler.forward(self.body, settings)["body"]), ack)
                    else:
                        with self.assertRaises(ValueError):
                            handler.forward(self.body, settings)


if __name__ == "__main__":
    unittest.main()
