# Quick Start: Using Your Kimball Framework in Databricks Notebooks

## Step 1: Build the Wheel (Already Done! ✓)

Your wheel has been built:

```
dist/kimball_framework-0.1.0-py3-none-any.whl
```

## Step 2: Upload to Databricks

### Option A: Via Databricks VSCode Extension (Easiest)

1. In VSCode, open the Databricks extension panel
2. Navigate to "Workspace" or "DBFS"
3. Right-click and select "Upload File"
4. Upload `dist/kimball_framework-0.1.0-py3-none-any.whl`
5. Upload to `/FileStore/wheels/` (create if needed)

### Option B: Via Databricks UI

1. Go to https://dbc-7bded16a-1785.cloud.databricks.com
2. Click "Data" in the left sidebar
3. Click "Create Table" → "Upload File"
4. Upload `kimball_framework-0.1.0-py3-none-any.whl`
5. Note the DBFS path shown

## Step 3: Use in Your Notebook

Add this as the **first cell** in `Kimball_Demo.ipynb`:

```python
# Install the Kimball framework
%pip install /dbfs/FileStore/wheels/kimball_framework-0.1.0-py3-none-any.whl

# Restart Python kernel to use the new package
dbutils.library.restartPython()
```

Then in the next cell:

```python
# Now you can import and use the framework
from kimball.dimension import DimensionProcessor
from kimball.fact import FactProcessor
from kimball.hashing import generate_hash_key

print("✓ Kimball framework loaded successfully!")
```

## Alternative: Install Directly from Local Build

If you're developing and want to test changes quickly:

```python
# In your notebook
%pip install /Workspace/Repos/<your-username>/kimball_project/databricks_kimball_framework

# Or if using the bundle explorer
%pip install -e /Workspace/<path-to-your-repo>
```

## For Production: Install as Cluster Library

1. Go to your cluster configuration
2. Click "Libraries" tab
3. Click "Install New"
4. Select "DBFS"
5. Enter: `dbfs:/FileStore/wheels/kimball_framework-0.1.0-py3-none-any.whl`
6. Click "Install"
7. Restart cluster

Now the framework is available in ALL notebooks on that cluster!

## Troubleshooting

### "Module not found" error

- Make sure you ran the `%pip install` cell
- Make sure you restarted the Python kernel (`dbutils.library.restartPython()`)

### "Wheel not found" error

- Check the DBFS path is correct
- Verify the file was uploaded successfully

### Need to update the framework?

1. Make your code changes locally
2. Validate: `python scripts/validate_fixes.py`
3. Rebuild: `.venv\Scripts\python -m build`
4. Upload the new wheel (it will have the same name)
5. Restart the cluster or re-run the `%pip install` cell

---

**You're ready to run your Kimball Demo notebook!** 🚀

Just add the installation cell at the top, upload the wheel, and you're good to go.
