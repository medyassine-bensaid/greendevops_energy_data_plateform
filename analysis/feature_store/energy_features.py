# feature_store/energy_features.py
from feast import Entity, FeatureView, Field, FileSource
from feast.types import Float64, Int64, String
from datetime import timedelta
import pandas as pd

job_entity = Entity(name='job_name', join_keys=['job_name'])

energy_source = FileSource(
    name='energy_features_source',
    path='feature_store/energy_features.parquet',
    timestamp_field='date',
)

energy_feature_view = FeatureView(
    name='job_energy_features',
    entities=[job_entity],
    ttl=timedelta(days=90),
    schema=[
        Field(name='energy_per_second',     dtype=Float64),
        Field(name='log_energy',            dtype=Float64),
        Field(name='cpu_ram_ratio',         dtype=Float64),
        Field(name='total_energy_lag1',     dtype=Float64),
        Field(name='total_energy_j_roll7_mean', dtype=Float64),
        Field(name='energy_efficiency_score',   dtype=Float64),
        Field(name='arch_code',             dtype=Int64),
        Field(name='msrc_code',             dtype=Int64),
        Field(name='category_code',         dtype=Int64),
    ],
    source=energy_source,
)