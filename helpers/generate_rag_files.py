from execute_dcli_cmd import execute_dcli_cmd
import json

# Nodes to get information from for generating RAG files
DB_NODE = "scaqat20adm07"
CELL_NODE = "scaqat20celadm10"

# db_metric_definitions.json
dcli_output = execute_dcli_cmd(f"dcli -l root -c {DB_NODE} cellcli -e list metricdefinition detail")
dcli_output = dcli_output.replace(f"{DB_NODE}: ", "")
blocks = [block for block in dcli_output.strip().split("\n\n") if block.strip()]
metrics = []
for block in blocks:
    metric = {}
    lines = block.strip().split("\n")
    for line in lines:
        if ":" in line:
            key, value = line.split(":", 1)
            value = value.strip().strip('"')
            metric[key.strip()] = value
    if metric:
        metrics.append(metric)
with open("../rag/db_metric_definitions.json", "w") as file:
    json.dump(metrics, file, indent=2)

# cell_metric_definitions.json
dcli_output = execute_dcli_cmd(f"dcli -l root -c {CELL_NODE} cellcli -e list metricdefinition detail")
dcli_output = dcli_output.replace(f"{CELL_NODE}: ", "")
blocks = [block for block in dcli_output.strip().split("\n\n") if block.strip()]
metrics = []
for block in blocks:
    metric = {}
    lines = block.strip().split("\n")
    for line in lines:
        if ":" in line:
            key, value = line.split(":", 1)
            value = value.strip().strip('"')
            metric[key.strip()] = value
    if metric:
        metrics.append(metric)
with open("../rag/cell_metric_definitions.json", "w") as file:
    json.dump(metrics, file, indent=2)

# dbmcli_help.txt
dcli_output = execute_dcli_cmd(f"dcli -l root -c {DB_NODE} \"dbmcli -e help | grep -v -E '(ALTER|CREATE|DEREGISTER|DROP|GRANT|LIST|REGISTER|REVOKE)$' | tail -n +4 | xargs -I {{}} bash -c 'echo; echo \\\"Help for {{}}\\\"; cellcli -e help {{}}'\"")
dcli_output = dcli_output.replace(f"{DB_NODE}: ", "")
with open("../rag/dbmcli_help.txt", "w") as file:
    print(dcli_output[1:-1], file=file)

# cellcli_help.txt
dcli_output = execute_dcli_cmd(f"dcli -l root -c {CELL_NODE} \"cellcli -e help | grep -v -E '(ALTER|CREATE|DEREGISTER|DROP|GRANT|LIST|REGISTER|REVOKE)$' | tail -n +4 | xargs -I {{}} bash -c 'echo; echo \\\"Help for {{}}\\\"; cellcli -e help {{}}'\"")
dcli_output = dcli_output.replace(f"{CELL_NODE}: ", "")
with open("../rag/cellcli_help.txt", "w") as file:
    print(dcli_output[1:-1], file=file)

# dbmcli_describe.txt
dcli_output = execute_dcli_cmd(f"dcli -l root -c {DB_NODE} \"dbmcli -e help | awk '{{print $2}}' | tail -n +4 | sort | uniq | tail -n +2 | xargs -I {{}} bash -c 'echo; echo \\\"Attributes for {{}}\\\"; echo; dbmcli -e describe {{}}'\"")
dcli_output = dcli_output.replace(f"{DB_NODE}: ", "")
with open("../rag/dbmcli_describe.txt", "w") as file:
    for line in dcli_output.strip().split('\n'):
        if not line.startswith("Attributes for") and not len(line) == 0:
            parts = line.split(maxsplit=1)
            if len(parts) > 1:
                if parts[1].startswith("modifiable"):
                    parts[1] = parts[1][11:].strip()
                if parts[1].startswith("hidden"):
                    parts[1] = parts[1][6:].strip()
                if parts[1]:
                    line = (": ").join(parts)
                else:
                    line = parts[0]
            else:
                line = parts[0]
        print(line, file=file)

# cellcli_describe.txt
dcli_output = execute_dcli_cmd(f"dcli -l root -c {CELL_NODE} \"cellcli -e help | awk '{{print $2}}' | tail -n +4 | sort | uniq | tail -n +2 | xargs -I {{}} bash -c 'echo; echo \\\"Attributes for {{}}\\\"; echo; cellcli -e describe {{}}'\"")
dcli_output = dcli_output.replace(f"{CELL_NODE}: ", "")
with open("../rag/cellcli_describe.txt", "w") as file:
    for line in dcli_output.strip().split('\n'):
        if not line.startswith("Attributes for") and not len(line) == 0:
            parts = line.split(maxsplit=1)
            if len(parts) > 1:
                if parts[1].startswith("modifiable"):
                    parts[1] = parts[1][11:].strip()
                if parts[1].startswith("hidden"):
                    parts[1] = parts[1][6:].strip()
                if parts[1]:
                    line = (": ").join(parts)
                else:
                    line = parts[0]
            else:
                line = parts[0]
        print(line, file=file)