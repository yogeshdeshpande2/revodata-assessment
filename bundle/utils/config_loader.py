import yaml

def load_config(config_path, env=None):
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    env = env or config.get('env', 't')
    return config['variables'][env]