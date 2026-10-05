# KEDA SQS Autoscaling Demo (AWS EKS)

A hands-on demo that shows how **KEDA (Kubernetes Event-driven Autoscaling)** scales workloads based on
external events instead of CPU/memory metrics.

The lab covers **two KEDA scalers**:

| Part | Trigger | Workload | Scales when |
|------|---------|----------|-------------|
| **Part 1** | `aws-sqs-queue` | Python SQS worker (`sqs-worker`) | Messages land in the SQS queue |
| **Part 2** | `cron` | nginx (`nginx-deployment`) | A scheduled time window starts |

**Part 1** scales a Python SQS worker from **0 → N replicas** based on the number of messages in an
**AWS SQS queue**. When the queue is empty, the worker scales all the way down to **zero**
(no idle pods, no cost). Push a few messages, and KEDA brings the workers up automatically within seconds.

> ### 📌 Lab Environment Notes
>
> - **Public subnets** – the cluster and all workloads run in the **public subnets** of the VPC
>   (`privateNetworking: false` in `keda-eks-cluster.yaml`, subnets in `ap-south-2a/b/c`).
>   Worker nodes get public IPs, so pods reach the internet directly (image pulls from ECR, AWS API
>   calls). This is convenient for a lab — for production, use **private subnets + NAT gateway**.
> - **EC2 instances** – the `keda-workers` managed node group runs **2 × `t3.small`** EC2 instances
>   that host the pods. These bill hourly: always run the [Cleanup](#cleanup) steps (or at least
>   delete the cluster) when you finish the hands-on.
> - **Admin EC2 instance (no SSH)** – every command in this hands-on was executed from a dedicated
>   **admin EC2 instance**. Its IAM role has two policies attached:
>   **`AmazonSSMManagedInstanceCore`** + **`AdministratorAccess`**.
>   **SSH was never used** — the shell was opened through **AWS Systems Manager (SSM) Session Manager**,
>   so there are no key pairs, no port 22 open, and no SSH security-group rules.
>   Remember to **terminate this instance** in the [Cleanup](#cleanup) steps as well.

---

## Architecture

```
                       ┌────────────────────────────────────────────┐
                       │              Amazon EKS                    │
                       │            (keda-sqs-lab)                  │
                       │                                            │
  aws sqs              │   ┌──────────────┐      ┌───────────────┐  │
  send-message  ───────┼──▶│  SQS Queue   │◀─────│  KEDA         │  │
                       │   │ keda-demo-   │ poll │  Operator     │  │
                       │   │ queue        │ every│  (aws-sqs-    │  │
                       │   └──────┬───────┘  5s  │   trigger)    │  │
                       │          │              └──────┬────────┘  │
                       │          │ receive/delete      │ scales    │
                       │          ▼                     ▼           │
                       │   ┌────────────────────────────────────┐   │
                       │   │  Deployment: sqs-worker            │   │
                       │   │  replicas: 0  ──▶  0..10 (HPA)     │   │
                       │   │  IRSA → sqs:Receive/DeleteMessage  │   │
                       │   └────────────────────────────────────┘   │
                       └────────────────────────────────────────────┘
```

**Scaling flow**

1. KEDA polls the queue's `ApproximateNumberOfMessages` every **5 seconds**.
2. `queueLength: 5` → desired replicas = `ceil(messages / 5)`.
3. `minReplicaCount: 0`, `maxReplicaCount: 10`.
4. `cooldownPeriod: 30` → after the queue drains, pods scale back to **0**.

---

## Repository Structure

```
keda-sqs-demo/
├── keda-eks-cluster.yaml          # eksctl cluster config (EKS 1.36, ap-south-2)
├── iam/
│   ├── trust-policy.json          # IRSA trust for the worker SA
│   ├── sqs-worker-policy.json     # IAM policy – consume SQS messages
│   ├── keda-trust-policy.json     # IRSA trust for the KEDA operator
│   └── keda-sqs-policy.json       # IAM policy – read queue attributes
├── kubernetes/
│   ├── keda-sqs-worker-sa.yaml    # ServiceAccount with IRSA role annotation
│   ├── keda-aws-auth.yaml         # TriggerAuthentication (podIdentity: aws)
│   ├── deployment.yaml            # sqs-worker Deployment (0 replicas)
│   └── scaled-object.yaml         # KEDA ScaledObject (aws-sqs-queue trigger)
├── worker/
│   ├── app.py                     # Python SQS consumer (boto3)
│   ├── Dockerfile
│   ├── requirements.txt
│   └── .dockerignore
├── scripts/
│   └── send-messages.sh           # Load-test helper – pushes N messages
└── cron/                          # Part 2: KEDA cron scaler (nginx)
    ├── deployment.yaml
    └── scaled-object.yaml
```

---

## Prerequisites

| Tool | Purpose |
|------|---------|
| AWS CLI v2 | AWS API calls |
| `eksctl` | Create/manage the EKS cluster |
| `kubectl` | Kubernetes manifests |
| Helm 3 | Install KEDA |
| Docker | Build & push the worker image |

### Where the commands run: Admin EC2 + SSM Session Manager

This experiment was **not** run from a local laptop and **no SSH access** was used. All commands
were executed on an **admin EC2 instance** launched in the same account/VPC:

| Aspect | Detail |
|--------|--------|
| Access method | **AWS Systems Manager Session Manager** (`Start session` from the console, or `aws ssm start-session`) |
| SSH | **Not used** — no key pair, no port 22, no inbound SSH rules required |
| IAM role policies | **`AmazonSSMManagedInstanceCore`** (lets SSM agent register the instance) + **`AdministratorAccess`** (full access to create the EKS cluster, SQS, IAM, ECR …) |
| Installed on it | AWS CLI v2, `eksctl`, `kubectl`, Helm, Docker, Python, plus base packages (`git`, `curl`, `wget`, `unzip`, `jq`, `tar`, `gzip`) |

```bash
# Open a shell on the admin instance — SSH-free
aws ssm start-session --target i-xxxxxxxxxxxxxxxxx
```

> `AmazonSSMManagedInstanceCore` is what makes Session Manager work;
> `AdministratorAccess` is what allows the instance role itself to create every resource in this lab
> (no static AWS keys were stored on disk).

**Base packages (mandatory)** — right after opening the session, install these first.
Without them the hands-on won't work (git/curl/wget are needed to fetch tools and manifests,
`jq` for parsing AWS CLI JSON output, `unzip`/`tar`/`gzip` to extract their archives):

```bash
sudo dnf install -y \
    git \
    curl \
    wget \
    unzip \
    jq \
    tar \
    gzip
```

**Install kubectl** (v1.36.1 — matches the cluster version in `keda-eks-cluster.yaml`):

```bash
curl -LO "https://dl.k8s.io/release/v1.36.1/bin/linux/amd64/kubectl"
chmod +x kubectl
sudo mv kubectl /usr/local/bin/
```

**Install Helm 3:**

```bash
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
```

**Install eksctl:**

```bash
ARCH=amd64
PLATFORM=$(uname -s)_$ARCH

curl -sLO "https://github.com/eksctl-io/eksctl/releases/latest/download/eksctl_${PLATFORM}.tar.gz"

tar -xzf eksctl_${PLATFORM}.tar.gz -C /tmp

sudo mv /tmp/eksctl /usr/local/bin

rm eksctl_${PLATFORM}.tar.gz
```

> AWS CLI v2 comes preinstalled on the Amazon Linux AMI (`aws --version` to confirm).

**Verify everything is installed and you're authenticated to the right account:**

```bash
aws --version
kubectl version --client
helm version
eksctl version
aws sts get-caller-identity
```

`aws sts get-caller-identity` should return **your account ID** — the admin instance role's
credentials are picked up automatically (no `aws configure` needed).

**Set the lab environment variables:**

```bash
export AWS_REGION=ap-south-2
export CLUSTER_NAME=keda-sqs-lab
export NAMESPACE=keda-demo
export SERVICE_ACCOUNT=keda-sqs-worker
export QUEUE_NAME=keda-demo-queue
export IAM_ROLE_NAME=keda-sqs-worker-role
export IAM_POLICY_NAME=keda-sqs-worker-policy
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
```

---

## Part 1 – SQS Queue Scaler (Step-by-Step)

### 1. Create the EKS Cluster

```bash
eksctl create cluster -f keda-eks-cluster.yaml
```

> 2-node `t3.small` managed node group in `ap-south-2` using your VPC's **public subnets**
> (`privateNetworking: false`), so the nodes/pods get public IPs. This also writes the kubeconfig entry.

Verify:

```bash
kubectl get nodes
```

### 2. Enable IAM OIDC Provider (IRSA)

First, fetch the cluster's OIDC issuer URL:

```bash
aws eks describe-cluster \
  --name keda-sqs-lab \
  --region ap-south-2 \
  --query 'cluster.identity.oidc.issuer' \
  --output text
```

It prints something like:

```
https://oidc.eks.ap-south-2.amazonaws.com/id/8AEDEB49D7DA7BD280865D8884D9BEDF
```

> This OIDC provider ID is exactly what the trust policies in `iam/trust-policy.json` and
> `iam/keda-trust-policy.json` are pinned to (`Federated: arn:aws:iam::<account>:oidc-provider/...`).

Then associate it with IAM so Kubernetes service accounts can assume AWS roles (IRSA):

```bash
eksctl utils associate-iam-oidc-provider \
  --cluster $CLUSTER_NAME \
  --region $AWS_REGION \
  --approve
```

### 3. Create the SQS Queue

```bash
aws sqs create-queue --queue-name $QUEUE_NAME --region $AWS_REGION

export QUEUE_URL=$(aws sqs get-queue-url \
  --queue-name $QUEUE_NAME --region $AWS_REGION \
  --query QueueUrl --output text)

aws sqs get-queue-attributes \
  --queue-url "$QUEUE_URL" \
  --attribute-names ApproximateNumberOfMessages \
  --region $AWS_REGION
```

### 4. Worker IAM Role (IRSA)

Create the policy that lets the worker consume messages:

```bash
aws iam create-policy \
  --policy-name $IAM_POLICY_NAME \
  --policy-document file://iam/sqs-worker-policy.json

export POLICY_ARN=arn:aws:iam::${AWS_ACCOUNT_ID}:policy/${IAM_POLICY_NAME}
```

Create the role trusted by the cluster's OIDC provider for the worker ServiceAccount:

```bash
aws iam create-role \
  --role-name $IAM_ROLE_NAME \
  --assume-role-policy-document file://iam/trust-policy.json \
  --description "IRSA role for KEDA SQS worker"

aws iam attach-role-policy \
  --role-name $IAM_ROLE_NAME \
  --policy-arn $POLICY_ARN

export WORKER_ROLE_ARN=$(aws iam get-role \
  --role-name $IAM_ROLE_NAME --query 'Role.Arn' --output text)
```

> `iam/trust-policy.json` restricts `system:serviceaccount:keda-demo:keda-sqs-worker` and is
> pinned to your account's EKS OIDC provider.

### 5. KEDA Scaler IAM Role

```bash
export KEDA_ROLE_NAME=keda-sqs-scaler-role
export KEDA_POLICY_NAME=keda-sqs-scaler-policy

aws iam create-policy \
  --policy-name $KEDA_POLICY_NAME \
  --policy-document file://iam/keda-sqs-policy.json

export KEDA_POLICY_ARN=arn:aws:iam::${AWS_ACCOUNT_ID}:policy/${KEDA_POLICY_NAME}

aws iam create-role \
  --role-name $KEDA_ROLE_NAME \
  --assume-role-policy-document file://iam/keda-trust-policy.json \
  --description "IRSA role for KEDA SQS scaler"

aws iam attach-role-policy \
  --role-name $KEDA_ROLE_NAME \
  --policy-arn $KEDA_POLICY_ARN

export KEDA_ROLE_ARN=$(aws iam get-role \
  --role-name $KEDA_ROLE_NAME --query 'Role.Arn' --output text)
```

> The KEDA operator only needs `sqs:GetQueueAttributes` (`iam/keda-sqs-policy.json`).

### 6. Install KEDA with IRSA

```bash
helm repo add kedacore https://kedacore.github.io/charts
helm repo update

helm install keda kedacore/keda \
  --namespace keda \
  --create-namespace \
  --set podIdentity.aws.irsa.enabled=true \
  --set podIdentity.aws.irsa.roleArn="$KEDA_ROLE_ARN"
```

```bash
kubectl get po -n keda
kubectl get sa -n keda
```

### 7. Namespace, ServiceAccount & TriggerAuthentication

```bash
kubectl create namespace $NAMESPACE
kubectl apply -f kubernetes/keda-sqs-worker-sa.yaml
kubectl apply -f kubernetes/keda-aws-auth.yaml
```

* `keda-sqs-worker-sa.yaml` → ServiceAccount annotated with `eks.amazonaws.com/role-arn` (worker role).
* `keda-aws-auth.yaml` → `TriggerAuthentication` using `podIdentity.provider: aws`, referenced by the ScaledObject.

```bash
kubectl get sa keda-sqs-worker -n keda-demo -o yaml
```

### 8. Build & Push the Worker Image

```bash
aws ecr create-repository --repository-name keda-sqs-worker --region $AWS_REGION

export ECR_REGISTRY=${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com
export IMAGE_URI=${ECR_REGISTRY}/keda-sqs-worker:v1

aws ecr get-login-password --region $AWS_REGION | \
  docker login --username AWS --password-stdin $ECR_REGISTRY

docker build -t $IMAGE_URI worker/
docker push $IMAGE_URI
```

The worker (`worker/app.py`) long-polls the queue for 20s, "processes" each message for
`WORK_TIME=10` seconds, then deletes it.

### 9. Deploy the Worker (initially 0 replicas)

```bash
kubectl apply -f kubernetes/deployment.yaml
kubectl get deployment -n keda-demo
```

You should see `0/0` – nothing is running yet.

### 10. Apply the KEDA ScaledObject

```bash
kubectl apply -f kubernetes/scaled-object.yaml
kubectl get scaledobject sqs-worker-scaler -n keda-demo
kubectl get hpa -n keda-demo
```

`kubernetes/scaled-object.yaml` key settings:

| Field | Value | Meaning |
|-------|-------|---------|
| `minReplicaCount` | `0` | Scale to zero when idle |
| `maxReplicaCount` | `10` | Upper limit |
| `queueLength` | `5` | ~1 pod per 5 messages |
| `pollingInterval` | `5` | Check queue every 5s |
| `cooldownPeriod` | `30` | Wait 30s before scaling to 0 |

---

## Part 1 – Test: SQS Autoscaling in Action

**1. Confirm the queue is empty and no pods are running:**

```bash
aws sqs get-queue-attributes \
  --queue-url "$QUEUE_URL" \
  --attribute-names ApproximateNumberOfMessages \
  --region $AWS_REGION

kubectl get pods -n keda-demo
```

**2. Send 10 messages:**

```bash
chmod +x scripts/send-messages.sh
QUEUE_URL=$QUEUE_URL AWS_REGION=$AWS_REGION scripts/send-messages.sh 10
```

**3. Watch KEDA react:**

```bash
kubectl get scaledobject sqs-worker-scaler -n keda-demo -w
kubectl get hpa -n keda-demo -w
kubectl get pods -n keda-demo -w
```

Within ~5–10 seconds you should see the HPA go from 0 → **2 replicas** (10 messages ÷ 5 per pod).

**4. Follow the worker logs:**

```bash
kubectl logs -f deployment/sqs-worker -n keda-demo
```

Expected output:

```
SQS worker started
Queue: https://sqs.ap-south-2.amazonaws.com/.../keda-demo-queue
Received message: job-1
Processing job-1 for 10 seconds...
Completed message: job-1
```

**5. Watch it scale back to zero:**

After the queue drains and the 30s cooldown elapses, KEDA deletes the HPA-managed pods:

```bash
kubectl get pods -n keda-demo
# No resources found  ✓
```

---

## Part 2 – Cron Scaler (Scheduled Autoscaling)

The second part of the hands-on uses KEDA's **`cron` trigger** – no queue or metrics involved.
It scales an nginx deployment to **5 replicas inside a fixed daily time window**, and back to
**0** when the window ends. Useful for predictable load (reports, batch jobs, traffic spikes).

**1. Deploy nginx:**

```bash
kubectl apply -f cron/deployment.yaml
kubectl get deployment nginx-deployment
```

**2. Apply the cron ScaledObject:**

```bash
kubectl apply -f cron/scaled-object.yaml
kubectl get scaledobject cron-scaledobject
kubectl get hpa -n default
```

`cron/scaled-object.yaml` key settings:

| Field | Value | Meaning |
|-------|-------|---------|
| `type` | `cron` | Time-based trigger |
| `timezone` | `Asia/Kolkata` | Timezone for the schedule |
| `start` | `30 * * * *` | Scale **up** at minute 30 of every hour |
| `end` | `45 * * * *` | Scale **down** at minute 45 |
| `desiredReplicas` | `5` | Target replica count inside the window |
| `minReplicaCount` | `0` | Idle outside the window |
| `pollingInterval` | `30` | KEDA re-checks the schedule every 30s |

**3. Watch it scale:**

```bash
kubectl get pods -l app=nginx -w
```

Between `:30` and `:45` of each hour (Asia/Kolkata) you should see **5 nginx pods** running.
Outside that window the ScaledObject drops back to **0 replicas**.

> To test quickly without waiting an hour, temporarily change `start`/`end` in
> `cron/scaled-object.yaml` (e.g. a 10-minute window around the current time) and re-apply:
>
> ```bash
> kubectl apply -f cron/scaled-object.yaml
> ```

---

## Troubleshooting

| Symptom | Check |
|---------|-------|
| ScaledObject `Unready` | `kubectl describe scaledobject sqs-worker-scaler -n keda-demo` – usually an auth/queue URL error |
| `Failed to get queue attributes` | IRSA role on `keda-operator`, policy `iam/keda-sqs-policy.json`, OIDC provider |
| Pods `CrashLoopBackOff` (worker) | `kubectl logs deployment/sqs-worker -n keda-demo` – missing `QUEUE_URL` or IRSA perms |
| Worker can't receive/delete | IRSA role on `keda-sqs-worker` SA matches `iam/trust-policy.json` `sub` claim |
| No HPA created | `kubectl get events -n keda-demo` and confirm `keda` pods are `Running` |
| Cron pods never scale up | Confirm the current time is inside the `start`/`end` window in `cron/scaled-object.yaml` (timezone `Asia/Kolkata`), then `kubectl describe scaledobject cron-scaledobject` |

Useful commands:

```bash
kubectl get events -n keda-demo --sort-by=.lastTimestamp
kubectl describe hpa -n keda-demo
kubectl logs -n keda -l app.kubernetes.io/name=keda-operator
```

---

## Cleanup

> ⚠️ **Deleting the cluster terminates the 2 × `t3.small` EC2 instances** of the `keda-workers`
> managed node group — that is what stops the node-group billing. The **admin EC2 instance is NOT
> part of the cluster**, so you must terminate it separately (step 4). Always verify afterwards.

**1. Delete Kubernetes resources & KEDA:**

```bash
kubectl delete -f kubernetes/scaled-object.yaml
kubectl delete -f kubernetes/deployment.yaml
kubectl delete namespace keda-demo

kubectl delete -f cron/scaled-object.yaml
kubectl delete -f cron/deployment.yaml

helm uninstall keda -n keda
```

**2. Delete the EKS cluster (and its EC2 instances):**

```bash
eksctl delete cluster --name $CLUSTER_NAME --region $AWS_REGION
```

`eksctl` removes the managed node group, its Auto Scaling group, and terminates the underlying
**EC2 instances**.

**3. Delete the remaining AWS resources (SQS, ECR, IAM):**

```bash
aws sqs delete-queue --queue-url "$QUEUE_URL" --region $AWS_REGION
aws ecr delete-repository --repository-name keda-sqs-worker --region $AWS_REGION --force

aws iam detach-role-policy --role-name $IAM_ROLE_NAME   --policy-arn $POLICY_ARN
aws iam delete-role   --role-name $IAM_ROLE_NAME
aws iam delete-policy --policy-arn $POLICY_ARN

aws iam detach-role-policy --role-name $KEDA_ROLE_NAME  --policy-arn $KEDA_POLICY_ARN
aws iam delete-role   --role-name $KEDA_ROLE_NAME
aws iam delete-policy --policy-arn $KEDA_POLICY_ARN
```

**4. Terminate the admin EC2 instance (last — you run commands from it):**

```bash
aws ec2 terminate-instances \
  --instance-ids i-xxxxxxxxxxxxxxxxx \
  --region $AWS_REGION
```

Or **EC2 console → Instances → select the admin instance → Instance state → Terminate**.

> Terminating it also releases its ENI/Elastic IP. Its IAM role (`AmazonSSMManagedInstanceCore` +
> `AdministratorAccess`) and instance profile can be deleted afterwards if you don't plan to reuse them.

**5. Verify no EC2 instances are left over:**

```bash
aws ec2 describe-instances \
  --region $AWS_REGION \
  --filters "Name=instance-state-name,Values=pending,running,stopping,stopped" \
  --query 'Reservations[].Instances[].{ID:InstanceId,Type:InstanceType,Name:Tags[?Key==`Name`].Value|[0]}' \
  --output table
```

Or check the **EC2 console → Instances** — there should be no `t3.small` nodes and no admin
instance from this lab.

> **If cluster deletion fails or hangs**, clean up manually so you stop paying for EC2:
>
> 1. Delete the cluster's **CloudFormation stacks** (node group stack first, then the cluster stack).
> 2. Confirm the Auto Scaling group is gone, then **terminate** any remaining `t3.small` instances
>    in the EC2 console.
> 3. Release any leftover **Elastic IPs / ENIs** created for the worker nodes.

---

> Update account ID, region and resource names in the manifests before re-running in your own AWS account.
