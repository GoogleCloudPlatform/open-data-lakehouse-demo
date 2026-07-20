import os
import yaml

from dataclasses import dataclass, asdict, field

@dataclass()
class AppConfig:
    project_id: str
    location: str
    full_bq_connection_name: str
    gcs_bucket_name: str
    iceberg_catalog_bucket_name: str
    bq_connection_name: str = field(init=False)
    subnetwork_id: str
    spark_service_account: str
    staging_bq_dataset: str
    main_bq_dataset: str
    kafka_bootstrap: str
    kafka_topic: str
    kafka_alert_topic: str
    spark_tmp_bucket: str
    raw_mta_csv_path_in_gcs: str = field(init=False)

    def __post_init__(self):
        self.bq_connection_name = self.full_bq_connection_name.split("/")[-1]
        self.raw_mta_csv_path_in_gcs = (
            "mta-raw/mta-manual-downloaded-data_MTA_Subway_Hourly_Ridership.csv"
        )

_global_config = None

def load_config() -> AppConfig:
    global _global_config
    if not _global_config:
        config_path = os.path.join(os.path.dirname(__file__), "../app_config.yaml")
        with open(config_path, "r") as file:
            _global_config = AppConfig(**yaml.safe_load(file) or {})
    return _global_config

def save_config(_config: AppConfig) -> None:
    config_path = os.path.join(os.path.dirname(__file__), "app_config.yaml")
    try:
        with open(config_path, "w") as file:
            yaml.safe_dump(asdict(_config), file)
    except Exception as e:
        raise ValueError("Unable to save the config", e)
    global _global_config
    _global_config = _config
