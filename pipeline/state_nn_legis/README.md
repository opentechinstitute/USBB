1. Download csv of spreadsheet
2. Convert to json:
```
cat NN\ Map\ -\ Legislation.csv | python -c 'import csv,json,sys; print(json.dumps({d.pop("State"): d for d in csv.DictReader(sys.stdin)}, indent=4))' >> NNMap.json
```
3. Move the state name into the objects instead of being a key:
```
jq '[to_entries[] | .key as $state | .value | .["state"]=$state]' NNMap.json >> NNMap_named_array.json
```
4. Merge the named array file with the state geoJSON:
```
jq --slurpfile states NNMap_named_array.json '(reduce ($states[0][]) as $item ({}; .[$item.state] = $item)) as $states | (.features| .[].properties |= . + $states[.name//""])' state-net-neutrality.json > NNDatas
et.json
```