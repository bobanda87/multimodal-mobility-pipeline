"""
Switch one EventBridge Scheduler schedule on or off without touching the rest of the stack.

  AWS_PROFILE=<profile> python serverless/schedule.py <ScheduleName> ENABLED|DISABLED
  AWS_PROFILE=<profile> python serverless/schedule.py --list

The stacks create their schedules DISABLED; this starts them one at a time, so each can be checked before the
next. Names: see --list (e.g. OsloBysykkelFunctionSchedule, NormalizeFunctionSchedule).
"""
import os
import sys

import boto3


def client():
    return boto3.Session(profile_name=os.getenv("AWS_PROFILE"), region_name=os.getenv("AWS_REGION", "eu-north-1")).client("scheduler")


def main():
    if len(sys.argv) == 2 and sys.argv[1] == "--list":
        c = boto3.Session(profile_name=os.getenv("AWS_PROFILE"), region_name=os.getenv("AWS_REGION", "eu-north-1")).client("cloudformation")
        for stack in ("phd-collectors", "phd-normalizer", "phd-api"):
            try:
                names = [r["PhysicalResourceId"] for r in c.describe_stack_resources(StackName=stack)["StackResources"]
                         if r["ResourceType"] == "AWS::Scheduler::Schedule"]
            except Exception:
                continue
            for name in names:
                s = client().get_schedule(Name=name)
                print(f"{stack:16} {name:40} {s['State']:9} {s['ScheduleExpression']}")
        return
    if len(sys.argv) != 3 or sys.argv[2] not in ("ENABLED", "DISABLED"):
        sys.exit(__doc__)
    name, state = sys.argv[1], sys.argv[2]
    c = client()
    s = c.get_schedule(Name=name)
    args = {k: s[k] for k in ("Name", "ScheduleExpression", "FlexibleTimeWindow", "Target") if k in s}
    for k in ("GroupName", "ScheduleExpressionTimezone", "Description"):
        if s.get(k):
            args[k] = s[k]
    c.update_schedule(**args, State=state)
    print(name, "->", c.get_schedule(Name=name)["State"])


if __name__ == "__main__":
    main()
