def read_config():
  config = {}
  with open("client.properties") as fh:
    for line in fh:
      line = line.strip()
      if len(line) != 0 and line[0] != "#":
        parameter, value = line.strip().split('=', 1)
        config[parameter] = value.strip()
        
  config["group.id"] = "python-group-1"
  config["auto.offset.reset"] = "earliest"
  return config
