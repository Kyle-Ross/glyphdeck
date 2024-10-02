import oyaml as yaml

path = r"glyphdeck\config\logger_config.yaml"

with open(path, "r") as f:
    data = yaml.safe_load(f)
    data["private_data"]["log_input"] = False

with open(path, "w") as f:
    yaml.dump(data, f)
