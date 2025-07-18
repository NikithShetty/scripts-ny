#!/bin/bash

# Set your cluster name and region
CLUSTER_NAME="eks-kurukshetra"
REGION="ap-south-1"

echo "Fetching node groups for cluster: $CLUSTER_NAME in region: $REGION"
NODEGROUPS=$(aws eks list-nodegroups \
    --cluster-name "$CLUSTER_NAME" \
    --region "$REGION" \
    --query "nodegroups" \
    --output text)

SUBNETS_SEEN=""
NODEGROUP_COUNT=0
SUBNET_COUNT=0

echo "Subnets used by worker node groups:"

for NODEGROUP in $NODEGROUPS; do
    echo " - Node Group: $NODEGROUP"
    NODEGROUP_COUNT=$((NODEGROUP_COUNT + 1))
    SUBNETS=$(aws eks describe-nodegroup \
        --cluster-name "$CLUSTER_NAME" \
        --nodegroup-name "$NODEGROUP" \
        --region "$REGION" \
        --query "nodegroup.subnets" \
        --output text)
    
    for SUBNET in $SUBNETS; do
        echo "    • $SUBNET"
        if ! echo "$SUBNETS_SEEN" | grep -qw "$SUBNET"; then
            SUBNETS_SEEN="$SUBNETS_SEEN $SUBNET"
            SUBNET_COUNT=$((SUBNET_COUNT + 1))
        fi
    done
done

echo ""
echo "Summary:"
echo " - Total Node Groups: $NODEGROUP_COUNT"
echo " - Total Unique Subnets: $SUBNET_COUNT"
echo ""
echo "All subnets:"
for SUBNET in $SUBNETS_SEEN; do
    echo "  $SUBNET"
done
