# Day 15 — Interview Questions: ADF CI/CD with Azure DevOps

> 12 questions covering the adf_publish flow, ARM templates, approval gates, rollback, secrets, and ADF-specific CI/CD patterns.
> Attempt each question before checking `interview-solutions.md`.

---

## ARM Templates and the adf_publish Branch

**Q1 (Warm-up)**
What is the `adf_publish` branch and who creates it? Why should you never merge it into `main`?

---

**Q2 (Conceptual)**
When you click Publish in ADF Studio, two files are generated in `adf_publish`. What are they and what does each contain?

---

**Q3 (Scenario)**
A developer edits a pipeline in ADF Studio, saves it, but forgets to click Publish before going on leave. A week later, the CI/CD pipeline runs but the change is not deployed. Why?

---

**Q4 (Conceptual)**
What is `deploymentMode: Validation` in the ARM deployment task? What does it catch and what does it NOT catch?

---

## Triggers, Deployments, and Rollback

**Q5 (Tricky)**
Why must you stop ADF triggers before deploying an ARM template? What happens if you deploy while a scheduled trigger is active?

---

**Q6 (Scenario)**
An ADF deployment to production fails halfway through — some pipelines updated, some did not. The factory is now in an inconsistent state. How do you recover?

---

**Q7 (Advanced)**
Explain the Blue/Green deployment pattern for ADF. What problem does it solve that a standard in-place ARM deploy cannot?

---

## Azure DevOps Concepts

**Q8 (Warm-up)**
What is a Service Connection in Azure DevOps? Why is it used instead of putting Azure credentials directly in the pipeline YAML?

---

**Q9 (Conceptual)**
What is the difference between a Variable Group and a pipeline variable in Azure DevOps? When would you use each for ADF CI/CD?

---

**Q10 (Scenario)**
Your CD pipeline has two stages: Deploy Staging and Deploy Production. You want to ensure:
- Production only deploys from the `adf_publish` branch
- Production requires two approvers, not one
- Production can only be deployed between 9am–5pm Monday to Friday

How do you configure this in Azure DevOps?

---

## Environment Parameterisation

**Q11 (Conceptual)**
How do you parameterise ADF pipelines for three environments (dev, staging, production) without duplicating pipeline logic?

---

**Q12 (Advanced)**
A linked service connection string is currently hardcoded in `production-parameters.json`. A security audit flags this as a risk. What is the correct solution and what changes are needed in ADF, Azure DevOps, and the ARM deployment?
