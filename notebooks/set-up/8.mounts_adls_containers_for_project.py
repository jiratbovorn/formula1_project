# Databricks notebook source
def mounts_adls(storage_account_name, container_name):
    #Get secrets from vault
    client_id = dbutils.secrets.get(scope="formula1-scope",key="formula1-app-client-id")
    tenant_id = dbutils.secrets.get(scope="formula1-scope",key="formula1-app-tenant-id")
    client_secret = dbutils.secrets.get(scope="formula1-scope",key="formula1-app-client-secret")
    #Set spark configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

    #Mount the storage account container
    dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

    display(dbutils.fs.mounts())


# COMMAND ----------

# MAGIC %md
# MAGIC ####Mount raw container

# COMMAND ----------

mounts_adls('formula1dl1dl1', 'raw')

# COMMAND ----------

mounts_adls('formula1dl1dl1', 'presentation')

# COMMAND ----------

mounts_adls('formula1dl1dl1', 'processed')

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

mounts_adls('formula1dl1dl1', 'demo')

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dl1dl1/demo")

# COMMAND ----------

