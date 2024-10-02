import yaml
from icecream import ic

with open(r"glyphdeck\config\logger_config.yaml", 'w') as f:
    try:
        data = yaml.safe_load(f)
        print("\nBefore")
        ic(data)
        data["private_data"]["log_input"] = True
        print("\nAfter")
        ic(data)

    except yaml.YAMLError as exc:
        ic(exc)
