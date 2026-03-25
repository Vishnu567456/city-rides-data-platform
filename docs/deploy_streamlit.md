# Deploy the Dashboard (Streamlit Community Cloud)

This project can be deployed for free on Streamlit Community Cloud.

## Steps
1. Push your latest changes to GitHub.
2. Go to Streamlit Community Cloud and sign in with GitHub.
3. Click **New app**.
4. Choose your repo: `Vishnu567456/city-rides-lakehouse`.
5. Set the main file path to `app.py`.
6. Click **Deploy**.

## Notes
- The dashboard expects `data/warehouse.duckdb` to exist. For a hosted demo, generate a sample DB locally and commit it, or update the app to load sample data files.
- If you prefer not to commit the DB, you can modify `app.py` to load sample parquet files or ship a small CSV in `docs/`.
