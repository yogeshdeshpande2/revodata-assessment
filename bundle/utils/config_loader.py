import yaml
from pathlib import Path

def load_config(config_path, env=None):
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    env = env or config.get('env', 't')
    return config['variables'][env]

# Usage example:
# config = load_config('bundle/configs/env_config.yaml', env='t')
# catalog_name = config['catalog_name']
# bronze_schema_name = config['bronze_schema_name']
# ...