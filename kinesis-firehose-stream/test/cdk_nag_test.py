import pytest
import sys
import os
from aws_cdk import App, Aspects, Stack
from aws_cdk.assertions import Annotations, Match
from cdk_nag import AwsSolutionsChecks, NagSuppressions

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# Import all stacks
from stack.pipeline import PipelineStack
from stack.lakeformation import LakeFormationStack
from stack.firehose import FirehoseStack


def create_test_stack(stack_class, stack_name):
    # Update the app to include context
    app = App(
        context={
            "table_bucket_name": "streamtablebucket",
            "table_name": "transactions",
            "namespace": "analytics",
            "bucket_name": "streambucket",
        }
    )
    stack = stack_class(app, stack_name)
    Aspects.of(stack).add(AwsSolutionsChecks())

    if stack_name == "PipelineStack":
        NagSuppressions.add_stack_suppressions(
            stack,
            [
                {
                    "id": "AwsSolutions-IAM4",
                    "reason": "The IAM user, role, or group uses AWS managed policies. [ iam::aws:policy/service-role/AWSLambdaBasicExecutionRole ] This is a sample application and hence suppressing this error.",
                },
                {
                    "id": "AwsSolutions-L1",
                    "reason": "The non-container Lambda function is not configured to use the latest runtime version.",
                },
            ],
        )

    if stack_name == "FirehoseStack":
        NagSuppressions.add_stack_suppressions(
            stack,
            [
                {
                    "id": "AwsSolutions-KDF1",
                    "reason": "The Kinesis Data Firehose delivery stream does not have server-side encryption enabled. This is a sample application and hence suppressing this error.",
                },
            ],
        )

    # Add other stack-specific suppressions here
    if stack_name == "LakeFormationStack":
        NagSuppressions.add_stack_suppressions(
            stack,
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "The IAM entity contains wildcard permissions and does not have a cdk-nag rule suppression with evidence for those permission.",
                }
            ],
        )

    return stack


@pytest.fixture(
    params=[
        (PipelineStack, "PipelineStack"),
        (LakeFormationStack, "LakeFormationStack"),
        (FirehoseStack, "FirehoseStack"),
        # Add more stacks here
    ]
)
def test_stack(request):
    stack_class, stack_name = request.param
    return create_test_stack(stack_class, stack_name)


def test_no_unsuppressed_warnings(test_stack):
    warnings = Annotations.from_stack(test_stack).find_warning(
        "*", Match.string_like_regexp("AwsSolutions-.*")
    )
    assert len(warnings) == 0, f"Unsuppressed warnings found in {test_stack.stack_name}"


def test_no_unsuppressed_errors(test_stack):
    errors = Annotations.from_stack(test_stack).find_error(
        "*", Match.string_like_regexp("AwsSolutions-.*")
    )
    assert len(errors) == 0, f"Unsuppressed errors found in {test_stack.stack_name}"
