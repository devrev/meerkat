import { ConjunctionExpression } from '../../types/duckdb-serialization-types/serialization/ParsedExpression';
import { inTransform } from './in';

describe('In transforms Tests', () => {
  it('Should throw error if values are undefined', () => {
    expect(() =>
        inTransform({
        member: 'country',
        operator: 'contains',
        memberInfo: {
          name: 'country',
          sql: 'table.country',
          type: 'string',
        },
      })
    ).toThrow();
  });

  it('Should return the correct value for string member', () => {
    // Now uses optimized subquery approach for all cases
    const result = inTransform({
      member: 'country',
      operator: 'contains',
      values: ['US'],
      memberInfo: {
        name: 'country',
        sql: 'table.country',
        type: 'string',
      },
    });
    
    // Check it returns a subquery structure
    expect(result).toHaveProperty('class', 'SUBQUERY');
    expect(result).toHaveProperty('type', 'SUBQUERY');
    expect(result).toHaveProperty('subquery_type', 'ANY');
  });

  it('Should return the correct value for string_array member', () => {
    const output = inTransform({
      member: 'country',
      operator: 'contains',
      values: ['US', 'Germany', 'Israel'],
      memberInfo: {
        name: 'country',
        sql: 'table.country',
        type: 'string_array',
      },
    }) as ConjunctionExpression;
    expect(output).toEqual( {
        "alias": "",
        "catalog": "",
        "children":  [
            {
            "alias": "",
            "class": "COLUMN_REF",
            "column_names":  [
                "country",
            ],
            "type": "COLUMN_REF",
        },
            {
            "alias": "",
            "children":  [
                {
                "alias": "",
                "class": "CONSTANT",
                "type": "VALUE_CONSTANT",
                "value":  {
                "is_null": false,
                "type":  {
                    "id": "VARCHAR",
                    "type_info": null,
                },
                "value": "US",
                },
            },
            {
                "alias": "",
                "class": "CONSTANT",
                "type": "VALUE_CONSTANT",
                "value":  {
                    "is_null": false,
                    "type":  {
                        "id": "VARCHAR",
                        "type_info": null,
                    },
                    "value": "Germany",
                },
            },
            {
            "alias": "",
            "class": "CONSTANT",
            "type": "VALUE_CONSTANT",
            "value":  {
                "is_null": false,
                "type":  {
                        "id": "VARCHAR",
                        "type_info": null,
                    },
                    "value": "Israel",
                },
            }],
            "class": "OPERATOR",
            "type": "ARRAY_CONSTRUCTOR",
        },
        ],
        "class": "FUNCTION",
        "distinct": false,
        "export_state": false,
        "filter": null,
        "function_name": "&&",
        "is_operator": true,
        "order_bys":  {
        "orders":  [],
        "type": "ORDER_MODIFIER",
        },
        "schema": "",
        "type": "FUNCTION",
    });
  });
});
