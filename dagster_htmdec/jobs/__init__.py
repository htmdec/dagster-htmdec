from dagster import define_asset_job


pdv_job = define_asset_job(name="pdv_processing_job", selection="processed_pdv_data")
