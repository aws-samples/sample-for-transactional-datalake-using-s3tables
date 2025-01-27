#!/usr/bin/env python3
import os
import aws_cdk as cdk
from stack.pipeline import (
    PipelineStack,
)
from stack.lakeformation import (
    LakeFormationStack,
)
from stack.firehose import (
    FirehoseStack,
)

app = cdk.App()
pipeline_stack = PipelineStack(
    app,
    "PipelineStack",
)

lakeformation_stack = LakeFormationStack(
    app,
    "LakeFormationStack",
)
lakeformation_stack.add_dependency(pipeline_stack)

firehose_stack = FirehoseStack(
    app,
    "FirehoseStack",
)
firehose_stack.add_dependency(lakeformation_stack)


app.synth()
