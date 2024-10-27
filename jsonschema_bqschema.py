def convert_json_type_to_bigquery(json_type):
    """Convert JSON schema type to BigQuery type."""
    type_mapping = {
        'string': 'STRING',
        'integer': 'INTEGER',
        'number': 'FLOAT',
        'boolean': 'BOOLEAN',
        'object': 'RECORD',
        'array': 'REPEATED',
        'null': 'STRING'
    }
    return type_mapping.get(json_type.lower(), 'STRING')

def convert_json_schema_to_bigquery(json_schema):
    """
    Convert JSON schema to BigQuery schema format.
    
    Args:
        json_schema (dict): JSON schema definition
        
    Returns:
        list: BigQuery schema fields
    """
    bigquery_schema = []
    
    def process_properties(properties, required_fields=None):
        fields = []
        required_fields = required_fields or []
        
        for field_name, field_def in properties.items():
            field = {
                'name': field_name,
                'mode': 'REQUIRED' if field_name in required_fields else 'NULLABLE'
            }
            
            if 'type' in field_def:
                json_type = field_def['type']
                # Handle array type
                if isinstance(json_type, list):
                    # If type is ["string", "null"], use STRING
                    json_type = next((t for t in json_type if t != 'null'), 'string')
                
                if json_type == 'array':
                    field['mode'] = 'REPEATED'
                    if 'items' in field_def:
                        if 'type' in field_def['items']:
                            field['type'] = convert_json_type_to_bigquery(field_def['items']['type'])
                        if field_def['items'].get('type') == 'object':
                            field['type'] = 'RECORD'
                            field['fields'] = process_properties(field_def['items'].get('properties', {}))
                else:
                    field['type'] = convert_json_type_to_bigquery(json_type)
                    
                    # Handle nested objects
                    if json_type == 'object' and 'properties' in field_def:
                        field['type'] = 'RECORD'
                        field['fields'] = process_properties(
                            field_def['properties'],
                            field_def.get('required', [])
                        )
            
            # Add description if available
            if 'description' in field_def:
                field['description'] = field_def['description']
                
            fields.append(field)
            
        return fields
    
    # Process root level properties
    if 'properties' in json_schema:
        bigquery_schema = process_properties(
            json_schema['properties'],
            json_schema.get('required', [])
        )
    
    return bigquery_schema

# Example usage
if __name__ == "__main__":
    # Sample JSON schema
    json_schema = {
        "type": "object",
        "required": ["id", "name"],
        "properties": {
            "id": {
                "type": "integer",
                "description": "Unique identifier"
            },
            "name": {
                "type": "string",
                "description": "User's name"
            },
            "email": {
                "type": ["string", "null"],
                "description": "User's email"
            },
            "tags": {
                "type": "array",
                "items": {
                    "type": "string"
                },
                "description": "User tags"
            },
            "address": {
                "type": "object",
                "properties": {
                    "street": {"type": "string"},
                    "city": {"type": "string"},
                    "country": {"type": "string"}
                },
                "description": "User's address"
            }
        }
    }
    
    # Convert to BigQuery schema
    bigquery_schema = convert_json_schema_to_bigquery(json_schema)
    
    # Print the result
    import json
    print(json.dumps(bigquery_schema, indent=2))
