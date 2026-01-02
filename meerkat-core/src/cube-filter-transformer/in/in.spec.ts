import { ConjunctionExpression } from '../../types/duckdb-serialization-types/serialization/ParsedExpression';
import { CreateColumnRefOptions } from '../base-condition-builder/base-condition-builder';
import { inTransform } from './in';

describe('In transforms Tests', () => {
  describe('useDotNotation: false', () => {
    const options: CreateColumnRefOptions = {
      isAlias: false,
      useDotNotation: false,
    };

    it('Should throw error if values are undefined', () => {
      expect(() =>
        inTransform(
          {
            member: 'country',
            operator: 'contains',
            memberInfo: {
              name: 'country',
              sql: 'table.country',
              type: 'string',
            },
          },
          options
        )
      ).toThrow();
    });

    it('Should return the correct value for string member', () => {
      const expectedOutput = {
        alias: '',
        children: [
          {
            alias: '',
            class: 'COLUMN_REF',
            column_names: ['country'],
            type: 'COLUMN_REF',
          },
          {
            alias: '',
            class: 'CONSTANT',
            type: 'VALUE_CONSTANT',
            value: {
              is_null: false,
              type: {
                id: 'VARCHAR',
                type_info: null,
              },
              value: 'US',
            },
          },
        ],
        class: 'OPERATOR',
        type: 'COMPARE_IN',
      };
      expect(
        inTransform(
          {
            member: 'country',
            operator: 'contains',
            values: ['US'],
            memberInfo: {
              name: 'country',
              sql: 'table.country',
              type: 'string',
            },
          },
          options
        )
      ).toEqual(expectedOutput);
    });

    it('Should return the correct value for string_array member', () => {
      const output = inTransform(
        {
          member: 'country',
          operator: 'contains',
          values: ['US', 'Germany', 'Israel'],
          memberInfo: {
            name: 'country',
            sql: 'table.country',
            type: 'string_array',
          },
        },
        options
      ) as ConjunctionExpression;
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

  describe('useDotNotation: true', () => {
    const options: CreateColumnRefOptions = {
      isAlias: true,
      useDotNotation: true,
    };

    it('Should throw error if values are undefined', () => {
      expect(() =>
        inTransform(
          {
            member: 'country',
            operator: 'contains',
            memberInfo: {
              name: 'country',
              sql: 'table.country',
              type: 'string',
            },
          },
          options
        )
      ).toThrow();
    });

    it('Should return the correct value for string member', () => {
      const expectedOutput = {
        alias: '',
        children: [
          {
            alias: '',
            class: 'COLUMN_REF',
            column_names: ['country'],
            type: 'COLUMN_REF',
          },
          {
            alias: '',
            class: 'CONSTANT',
            type: 'VALUE_CONSTANT',
            value: {
              is_null: false,
              type: {
                id: 'VARCHAR',
                type_info: null,
              },
              value: 'US',
            },
          },
        ],
        class: 'OPERATOR',
        type: 'COMPARE_IN',
      };
      expect(
        inTransform(
          {
            member: 'country',
            operator: 'contains',
            values: ['US'],
            memberInfo: {
              name: 'country',
              sql: 'table.country',
              type: 'string',
            },
          },
          options
        )
      ).toEqual(expectedOutput);
    });

    it('Should return the correct value for string_array member', () => {
      const output = inTransform(
        {
          member: 'country',
          operator: 'contains',
          values: ['US', 'Germany', 'Israel'],
          memberInfo: {
            name: 'country',
            sql: 'table.country',
            type: 'string_array',
          },
        },
        options
      ) as ConjunctionExpression;
      expect(output).toEqual({
        alias: '',
        catalog: '',
        children: [
          {
            alias: '',
            class: 'COLUMN_REF',
            column_names: ['country'],
            type: 'COLUMN_REF',
          },
          {
            alias: '',
            children: [
              {
                alias: '',
                class: 'CONSTANT',
                type: 'VALUE_CONSTANT',
                value: {
                  is_null: false,
                  type: {
                    id: 'VARCHAR',
                    type_info: null,
                  },
                  value: 'US',
                },
              },
              {
                alias: '',
                class: 'CONSTANT',
                type: 'VALUE_CONSTANT',
                value: {
                  is_null: false,
                  type: {
                    id: 'VARCHAR',
                    type_info: null,
                  },
                  value: 'Germany',
                },
              },
              {
                alias: '',
                class: 'CONSTANT',
                type: 'VALUE_CONSTANT',
                value: {
                  is_null: false,
                  type: {
                    id: 'VARCHAR',
                    type_info: null,
                  },
                  value: 'Israel',
                },
              },
            ],
            class: 'OPERATOR',
            type: 'ARRAY_CONSTRUCTOR',
          },
        ],
        class: 'FUNCTION',
        distinct: false,
        export_state: false,
        filter: null,
        function_name: '&&',
        is_operator: true,
        order_bys: {
          orders: [],
          type: 'ORDER_MODIFIER',
        },
        schema: '',
        type: 'FUNCTION',
      });
    });
  });
});
