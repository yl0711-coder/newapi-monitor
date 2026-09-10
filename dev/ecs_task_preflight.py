"""Read-only local ECS TaskDefinition Properties diff; never calls AWS or deploys.

Inputs must be complete CloudFormation Properties, NOT redacted snapshots or
lower-camel-case DescribeTaskDefinition responses. No input values are printed.
Production checks validate a proposal only; they do not enable or deploy it.
"""
import argparse
import json

from ecs_cutover_preflight import InvalidPlan, load_manifest, manifest_digest
from ecs_task_contract import check_business_unchanged


def check_plan(before, after, reviewed_initializer_sha256=None, scope="isolated"):
    if type(before) is not dict or type(after) is not dict:
        raise ValueError("complete TaskDefinition Properties objects required")
    if "ContainerDefinitions" not in before or "ContainerDefinitions" not in after:
        raise ValueError("CloudFormation Properties required; AWS response conversion must be reviewed separately")
    result = check_business_unchanged(before, after, reviewed_initializer_sha256=reviewed_initializer_sha256,
                                      scope=scope)
    return {**result, "static_checks_passed": True, "contract_scope": scope,
            "authorization_verified": False, "baseline_sha256": manifest_digest(before),
            "proposal_sha256": manifest_digest(after),
            "remaining_gates": ["complete_current_source_definitions", "initializer_image_and_command_review",
                                "collector_images_and_credentials_review", "iam_and_storage_review",
                                "explicit_shadow_scope_implementation", "release_approval"]}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--proposal", required=True)
    parser.add_argument("--reviewed-initializer-sha256",
                        help="fingerprint supplied from separate review, not calculated as implicit approval")
    parser.add_argument("--scope", choices=("isolated", "production"), default="isolated")
    args = parser.parse_args(argv)
    try:
        result = check_plan(load_manifest(args.baseline), load_manifest(args.proposal),
                            args.reviewed_initializer_sha256, args.scope)
    except (InvalidPlan, ValueError, TypeError, KeyError, OSError, RecursionError):
        # Configuration keys and values can contain credentials. Never echo an
        # exception raised while inspecting input, file paths, or either source.
        print(json.dumps({"static_checks_passed": False, "production_ready": False,
                          "deployment_authorized": False,
                          "error": "task contract rejected; check complete definitions, business invariants and separately reviewed initializer fingerprint"}))
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
