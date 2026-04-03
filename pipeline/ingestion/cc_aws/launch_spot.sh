#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# Launch a cheap EC2 spot instance in us-east-1 to process Common Crawl
# ============================================================================
#
# This creates a c5.xlarge spot instance (~$0.05/hr vs $0.17 on-demand)
# in us-east-1 (same region as CC data = free S3 access).
#
# The instance:
#   - Installs Python deps
#   - Clones the repo
#   - Runs the CC pipeline
#   - Pushes results to BigQuery
#   - Self-terminates when done
#
# Prerequisites:
#   1. AWS CLI configured with credentials
#   2. A key pair created: aws ec2 create-key-pair --key-name kyivnotkiev --region us-east-1
#   3. GCP service account key for BigQuery access
#
# Usage:
#   ./launch_spot.sh
#
# ============================================================================

REGION="us-east-1"
INSTANCE_TYPE="c5.xlarge"  # 4 vCPU, 8GB RAM — good for WARC processing
AMI="ami-0c7217cdde317cfec"  # Amazon Linux 2023 in us-east-1 (update if needed)
KEY_NAME="kyivnotkiev"

echo "Launching EC2 spot instance..."
echo "  Region: $REGION"
echo "  Type: $INSTANCE_TYPE (~\$0.05/hr spot)"
echo ""

# User data script — runs on instance boot
USER_DATA=$(cat <<'USERDATA'
#!/bin/bash
set -ex

# Install deps
dnf install -y python3.11 python3.11-pip git
pip3.11 install boto3 warcio requests google-cloud-bigquery pyyaml

# Clone repo
cd /home/ec2-user
git clone https://github.com/IvanDobrovolsky/kyivnotkiev.git
cd kyivnotkiev

# Run pipeline (all crawls)
python3.11 -m pipeline.ingestion.cc_aws.run 2>&1 | tee /home/ec2-user/cc_pipeline.log

# Upload log to S3
aws s3 cp /home/ec2-user/cc_pipeline.log s3://kyivnotkiev-cc-results/logs/$(date +%Y%m%d_%H%M%S).log

# Self-terminate
INSTANCE_ID=$(curl -s http://169.254.169.254/latest/meta-data/instance-id)
aws ec2 terminate-instances --instance-ids $INSTANCE_ID --region us-east-1
USERDATA
)

# Request spot instance
aws ec2 run-instances \
    --region "$REGION" \
    --image-id "$AMI" \
    --instance-type "$INSTANCE_TYPE" \
    --key-name "$KEY_NAME" \
    --instance-market-options '{"MarketType":"spot","SpotOptions":{"SpotInstanceType":"one-time"}}' \
    --iam-instance-profile Name=kyivnotkiev-ec2-role \
    --user-data "$USER_DATA" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=kyivnotkiev-cc-processor},{Key=Project,Value=kyivnotkiev}]" \
    --output table \
    --query 'Instances[0].[InstanceId,InstanceType,Placement.AvailabilityZone,State.Name]'

echo ""
echo "Spot instance launched! It will:"
echo "  1. Install dependencies"
echo "  2. Clone the repo"
echo "  3. Process all 28 crawls (2013-2026)"
echo "  4. Push matches to BigQuery"
echo "  5. Self-terminate when done"
echo ""
echo "Monitor: aws ec2 describe-instances --filters 'Name=tag:Name,Values=kyivnotkiev-cc-processor' --region us-east-1"
echo "Logs: aws s3 ls s3://kyivnotkiev-cc-results/logs/"
