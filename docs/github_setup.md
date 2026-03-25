# GitHub Publish Checklist

Use this checklist when turning the project into a polished GitHub portfolio repository.

## 1. Create the repository
```bash
git init
git add .
git commit -m "Build City Rides Data Platform project"
git branch -M main
git remote add origin <your-github-repo-url>
git push -u origin main
```

## 2. Enable collaboration features
- Confirm GitHub Actions is enabled so `.github/workflows/ci.yml` runs on pushes and pull requests.
- Keep the issue templates and PR template in place so the repo looks maintained.
- Add a repository description such as: `End-to-end data engineering project using Python, DuckDB, dbt, and Streamlit.`

## 3. Make the README work for recruiters
- Keep the architecture diagram near the top.
- Add one screenshot of the Streamlit dashboard.
- Mention the medallion layers, quality gates, and `pipeline_runs` audit table.
- Keep the quickstart command copy-paste ready.

## 4. Optional improvements before sharing
- Add a license file once you decide how the project can be reused.
- Tag a release such as `v1.0.0` after the repo is stable.
- Turn on branch protection for `main`.
- Add a cloud follow-up branch that swaps DuckDB for Snowflake, BigQuery, or Redshift.
