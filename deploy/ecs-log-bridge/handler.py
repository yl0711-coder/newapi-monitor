"""Isolated REST API Gateway AWS_IAM registration bridge; not a public login API.

The Lambda resource policy must allow ONLY the configured Gateway method ARN.
Never distribute its Secrets Manager credential or allow task roles InvokeFunction.
"""

import base64
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request

MAX_BODY = 16 * 1024
MAX_REPLY = 16 * 1024
FIELDS = {"service_arn", "task_arn", "container", "runtime_id", "lane", "public_key"}
ACK_REQUIRED = {"ok", "version", "node", "lane", "audience", "lease_until"}
ACK_CAPABILITIES = {"archive_closure_v2", "final_boundary_v1", "final_newapi_files_v1"}
CALLER = re.compile(r"^arn:aws:sts::[0-9]{12}:assumed-role/[A-Za-z0-9_+=,.@-]+/[a-f0-9]{32}$")


def strict_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON field")
        result[key] = value
    return result


def reply(code, body):
    return {"statusCode": code, "headers": {"Content-Type": "application/json", "Cache-Control": "no-store"},
            "body": json.dumps(body, separators=(",", ":")), "isBase64Encoded": False}


def trusted_registration(event, settings):
    if settings.get("ECS_LOG_SCOPE") != "isolated":
        raise ValueError("bridge disabled")
    context = event.get("requestContext") or {}
    identity = context.get("identity") or {}
    caller = identity.get("userArn", "")
    if (not settings.get("ECS_LOG_API_ID") or settings.get("ECS_LOG_STAGE") != "isolated"
            or not re.fullmatch(r"[0-9]{12}", settings.get("ECS_LOG_ACCOUNT_ID", ""))
            or context.get("apiId") != settings["ECS_LOG_API_ID"]
            or context.get("stage") != settings["ECS_LOG_STAGE"]
            or context.get("accountId") != settings.get("ECS_LOG_ACCOUNT_ID")
            or context.get("httpMethod") != "POST" or event.get("httpMethod") != "POST"
            or context.get("resourcePath") != "/register" or event.get("resource") != "/register"
            or context.get("authorizer") or not isinstance(caller, str) or not CALLER.fullmatch(caller)):
        raise PermissionError("trusted AWS_IAM context required")
    body = event.get("body")
    if not isinstance(body, str) or len(body) > MAX_BODY * 2:
        raise ValueError("invalid body")
    raw = base64.b64decode(body, validate=True) if event.get("isBase64Encoded") is True else body.encode("utf-8")
    if len(raw) > MAX_BODY:
        raise ValueError("body too large")
    data = json.loads(raw, object_pairs_hook=strict_object)
    if not isinstance(data, dict) or set(data) != FIELDS or any(not isinstance(value, str) or not value for value in data.values()):
        raise ValueError("invalid registration fields")
    # Never use a header or body field for the principal, even if signed by a task.
    data["caller_arn"] = caller
    return data


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def bridge_secret(settings):
    # Runtime-provided boto3; lazy import permits dependency-free isolated tests.
    import boto3
    from botocore.config import Config
    arn = settings.get("ECS_LOG_BRIDGE_SECRET_ARN", "")
    if not arn.startswith("arn:aws:secretsmanager:"):
        raise ValueError("missing secret ARN")
    client = boto3.client("secretsmanager", config=Config(connect_timeout=2, read_timeout=2, retries={"total_max_attempts": 2}))
    value = client.get_secret_value(SecretId=arn).get("SecretString", "")
    if len(value) < 32 or len(value) > 4096 or any(ch.isspace() for ch in value):
        raise ValueError("invalid dedicated bridge secret")
    return value


def forward(data, settings):
    endpoint = settings.get("ECS_LOG_MONITOR_REGISTER_URL", "")
    url = urllib.parse.urlsplit(endpoint)
    if (url.scheme != "https" or not url.hostname or url.username or url.password or url.query or url.fragment
            or url.path != "/internal/ecs/v1/register"):
        raise ValueError("invalid fixed Monitor endpoint")
    request = urllib.request.Request(endpoint, data=json.dumps(data, separators=(",", ":")).encode(), method="POST",
                                     headers={"Content-Type": "application/json", "Authorization": "Bearer " + bridge_secret(settings)})
    # Ignore ambient proxies; verify TLS using the runtime trust store, no redirects.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    try:
        response = opener.open(request, timeout=5)
    except urllib.error.HTTPError as error:
        # Do not return arbitrary backend error text, credentials or network details.
        code = error.code
        error.close()
        return reply(code if code in (400, 401, 403, 409, 429, 503) else 502, {"error": "registration rejected or unavailable"})
    with response:
        raw = response.read(MAX_REPLY + 1)
        if response.status != 200 or len(raw) > MAX_REPLY:
            raise ValueError("invalid Monitor response")
        ack = json.loads(raw, object_pairs_hook=strict_object)
        if (not isinstance(ack, dict) or ack.get("ok") is not True or ack.get("version") != 1
                or not ACK_REQUIRED <= set(ack) <= ACK_REQUIRED | ACK_CAPABILITIES
                or any(type(ack[key]) is not bool for key in ACK_CAPABILITIES & set(ack))):
            raise ValueError("invalid registration ACK")
        return reply(200, ack)


def handler(event, _context):
    try:
        registration = trusted_registration(event, os.environ)
    except PermissionError:
        return reply(403, {"error": "trusted AWS_IAM context required"})
    except (ValueError, TypeError, AttributeError, UnicodeError):
        return reply(400, {"error": "invalid isolated registration"})
    try:
        return forward(registration, os.environ)
    except Exception:
        # The platform may log request IDs/status; never log input, tokens or body.
        return reply(503, {"error": "registration service unavailable"})
