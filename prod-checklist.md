# Production Readiness Checklist – PR Template

> **Warning:** This checklist must be completed before adding a new Application/Service

> Any ❌ in "must-have" items means the service **cannot be deployed to production**.

## Part 1 – Must-Haves 


| Section  | Must-Have | ✅ / ❌ / N/A | Notes / Action |
|----------|-----------|----------------|----------------|
| **Security** | App/service reviewed by Cybersecurity (Tim Z) | | |
| **Critical Functionality** | Core features implemented; input validation & error handling in place | | |
| **Authentication/Authorization** | Control's supported authentication and authorization integration configured correctly | | |
| **CI/CD Pipeline** | Source Code Repository| | |
|  |Build, test, deploy automated (Github Actions)| | |
| |staging deployment successful(adback2)| | |
| | rollback plan defined | | |
| **Tests** | Unit + integration tests pass; regression tests complete | | |
| **Observability**| Logs, metrics, traces|||
||health checks in place | | |
| **Documentation** | README |||
|| diagrams |||
||  maintainers & support contacts listed | | |

---

## Part 2 – Kubernetes Manifest Quick Checklist

| Section | Item | ✅ / ❌ / N/A | Notes / Action |
|---------|------|---------------|----------------|
| **Security** | PodSecurityContext / SecurityContext defined (e.g., runAsNonRoot, capabilities limited) | | |
| | No hardcoded secrets; using Secret / ConfigMap | | |
| | NetworkPolicies applied if needed | | |
| | ServiceAccount uses least privilege (avoid default if possible) | | |
| **Resources** | CPU / Memory requests defined | | |
| | CPU / Memory limits defined | | |
| **Deployment / Lifecycle** | Liveness probe defined | | |
| | Readiness probe defined | | |
| | RollingUpdate / deployment strategy defined | | |
| | Replicas / HPA defined if scalable | | |
| **Observability** | Logs written to stdout/stderr | | |
| | Metrics exposed if needed | | |
| **Best Practices** | Labels & annotations clear (app, version, team) | | |
| | ConfigMaps / Secrets versioned & referenced properly | | |
| | Images use version tags, not “latest” | | |
||  Use IfNotPresent imagePullPolicy |||

---

## Notes / Additional Comments
<!-- Use this section to provide any additional context, known issues, or reminders for the reviewer -->

