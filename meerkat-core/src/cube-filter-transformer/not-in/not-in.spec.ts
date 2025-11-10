import { ConjunctionExpression } from '../../types/duckdb-serialization-types/serialization/ParsedExpression';
import { notInTransform } from './not-in';

describe('Not In transforms Tests', () => {
  it('Should throw error if values are undefined', () => {
    expect(() =>
        notInTransform({
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
    const result = notInTransform({
      member: 'country',
      operator: 'contains',
      values: ['US'],
      memberInfo: {
        name: 'country',
        sql: 'table.country',
        type: 'string',
      },
    });
    
    // Check it returns OPERATOR_NOT wrapping a subquery
    expect(result).toHaveProperty('class', 'OPERATOR');
    expect(result).toHaveProperty('type', 'OPERATOR_NOT');
    expect(result.children[0]).toHaveProperty('class', 'SUBQUERY');
  });

  it('Should return the correct value for string_array member', () => {
    const output = notInTransform({
      member: 'country',
      operator: 'contains',
      values: ['US', 'Germany', 'Israel'],
      memberInfo: {
        name: 'country',
        sql: 'table.country',
        type: 'string_array',
      },
    }) as ConjunctionExpression;
    expect(output).toEqual({
        "alias": "",
        "children": [
           {
            "alias": "",
            "catalog": "",
            "children": [
               {
                "alias": "",
                "class": "COLUMN_REF",
                "column_names": [
                  "country",
                ],
                "type": "COLUMN_REF",
              },
               {
                "alias": "",
                "children": [
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
                  },
                ],
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
              "orders": [],
              "type": "ORDER_MODIFIER",
            },
            "schema": "",
            "type": "FUNCTION",
          },
        ],
        "class": "OPERATOR",
        "type": "OPERATOR_NOT",
    });
  });
});
