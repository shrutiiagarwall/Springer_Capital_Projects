import yaml
with open('/opt/airflow/config/schema_contract.yaml') as f:
    c = yaml.safe_load(f)
print('global:', c.get('global'))
print()
for col in c.get('columns', []):
    if col['name'] in ('num_arguments', 'event_type', 'split', 'trigger_indices'):
        print(f"col {col['name']}: {col}")
