from pydantic_settings import BaseSettings, SettingsConfigDict


# load settings from env or defaults
class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "geoetl"
    db_user: str = "geoetl"
    db_password: str = "geoetl"

    inpe_base_url: str = "https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/diario/Brasil"
    # base directory for data and logs
    data_dir: str = "data"


# singleton settings instance
settings = Settings()
