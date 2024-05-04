import { BASIC_JOIN_PATH, CIRCULAR_JOIN_PATH, CIRCULAR_TABLE_SCHEMA, CIRCULAR_TABLE_SCHEMA_SINGLE_JOIN_PATH, COMPLEX_JOIN_PATH, EXPECTED_CIRCULAR_TABLE_SCHEMA_INTERMEDIATE_JOIN_PATH, EXPECTED_OUTPUT_WITH_ONE_DEPTH, EXPECTED_OUTPUT_WITH_TWO_DEPTH, INTERMEDIATE_JOIN_PATH, LINEAR_TABLE_SCHEMA, SINGLE_NODE_JOIN_PATH } from './__fixtures__/joins.fixtures';
import { getNestedTableSchema } from './get-possible-nodes';

describe('Table schema functions', () => {
  describe('graph with no loops', () => {
    it('Test single node join path', async () => {
      const nestedSchema = await getNestedTableSchema(
        LINEAR_TABLE_SCHEMA,
        SINGLE_NODE_JOIN_PATH,
        1
      );

      expect(nestedSchema).toEqual({
        name: 'node1',
        measures: [],
        dimensions: [
          {
            schema: {
              name: 'id',
              sql: 'node1.id',
            },
            children: [
              {
                name: 'node2',
                measures: [],
                dimensions: [
                  {
                    schema: {
                      name: 'id',
                      sql: 'node2.id',
                    },
                    children: [],
                  },
                  {
                    schema: {
                      name: 'node11_id',
                      sql: 'node2.node11_id',
                    },
                    children: [],
                  },
                ],
              },
              {
                name: 'node3',
                measures: [],
                dimensions: [
                  {
                    schema: {
                      name: 'id',
                      sql: 'node3.id',
                    },
                    children: [],
                  },
                ],
              },
              {
                name: 'node6',
                measures: [],
                dimensions: [
                  {
                    schema: {
                      name: 'id',
                      sql: 'node6.id',
                    },
                    children: [],
                  },
                ],
              },
            ],
          },
        ],
      });
    });

    it('Test basic join path with depth 0 (should return original graph)', async () => {
      const nestedSchema = await getNestedTableSchema(
        LINEAR_TABLE_SCHEMA,
        BASIC_JOIN_PATH,
        0
      );

      expect(nestedSchema).toEqual({
        name: 'node1',
        measures: [],
        dimensions: [
          {
            schema: {
              name: 'id',
              sql: 'node1.id',
            },
            children: [
              {
                name: 'node2',
                measures: [],
                dimensions: [
                  {
                    schema: {
                      name: 'id',
                      sql: 'node2.id',
                    },
                    children: [],
                  },
                  {
                    schema: {
                      name: 'node11_id',
                      sql: 'node2.node11_id',
                    },
                    children: [],
                  },
                ],
              },
            ],
          },
        ],
      });
    });

    it('Test basic join path with depth 1', async () => {
      const nestedSchema = await getNestedTableSchema(
        LINEAR_TABLE_SCHEMA,
        BASIC_JOIN_PATH,
        1
      );

      expect(nestedSchema).toEqual({
        name: 'node1',
        measures: [],
        dimensions: [
          {
            schema: {
              name: 'id',
              sql: 'node1.id',
            },
            children: [
              {
                name: 'node2',
                measures: [],
                dimensions: [
                  {
                    schema: {
                      name: 'id',
                      sql: 'node2.id',
                    },
                    children: [
                      {
                        name: 'node4',
                        measures: [],
                        dimensions: [
                          {
                            schema: {
                              name: 'id',
                              sql: 'node4.id',
                            },
                            children: [],
                          },
                        ],
                      },
                    ],
                  },
                  {
                    schema: {
                      name: 'node11_id',
                      sql: 'node2.node11_id',
                    },
                    children: [
                      {
                        name: 'node11',
                        measures: [],
                        dimensions: [
                          {
                            schema: {
                              name: 'id',
                              sql: 'node11.id',
                            },
                            children: [],
                          },
                        ],
                      },
                    ],
                  },
                ],
              },
              {
                name: 'node3',
                measures: [],
                dimensions: [
                  {
                    schema: {
                      name: 'id',
                      sql: 'node3.id',
                    },
                    children: [],
                  },
                ],
              },
              {
                name: 'node6',
                measures: [],
                dimensions: [
                  {
                    schema: {
                      name: 'id',
                      sql: 'node6.id',
                    },
                    children: [],
                  },
                ],
              },
            ],
          },
        ],
      });
    });

    it('Test basic join path with depth 2', async () => {
      const nestedSchema = await getNestedTableSchema(
        LINEAR_TABLE_SCHEMA,
        BASIC_JOIN_PATH,
        2
      );

      expect(nestedSchema).toEqual({
        name: 'node1',
        measures: [],
        dimensions: [
          {
            schema: {
              name: 'id',
              sql: 'node1.id',
            },
            children: [
              {
                name: 'node2',
                measures: [],
                dimensions: [
                  {
                    schema: {
                      name: 'id',
                      sql: 'node2.id',
                    },
                    children: [
                      {
                        name: 'node4',
                        measures: [],
                        dimensions: [
                          {
                            schema: {
                              name: 'id',
                              sql: 'node4.id',
                            },
                            children: [
                              {
                                name: 'node5',
                                measures: [],
                                dimensions: [
                                  {
                                    schema: {
                                      name: 'id',
                                      sql: 'node5.id',
                                    },
                                    children: [],
                                  },
                                ],
                              },
                              {
                                name: 'node6',
                                measures: [],
                                dimensions: [
                                  {
                                    schema: {
                                      name: 'id',
                                      sql: 'node6.id',
                                    },
                                    children: [],
                                  },
                                ],
                              },
                              {
                                name: 'node7',
                                measures: [],
                                dimensions: [
                                  {
                                    schema: {
                                      name: 'id',
                                      sql: 'node7.id',
                                    },
                                    children: [],
                                  },
                                ],
                              },
                            ],
                          },
                        ],
                      },
                    ],
                  },
                  {
                    schema: {
                      name: 'node11_id',
                      sql: 'node2.node11_id',
                    },
                    children: [
                      {
                        name: 'node11',
                        measures: [],
                        dimensions: [
                          {
                            schema: {
                              name: 'id',
                              sql: 'node11.id',
                            },
                            children: [],
                          },
                        ],
                      },
                    ],
                  },
                ],
              },
              {
                name: 'node3',
                measures: [],
                dimensions: [
                  {
                    schema: {
                      name: 'id',
                      sql: 'node3.id',
                    },
                    children: [
                      {
                        name: 'node5',
                        measures: [],
                        dimensions: [
                          {
                            schema: {
                              name: 'id',
                              sql: 'node5.id',
                            },
                            children: [],
                          },
                        ],
                      },
                    ],
                  },
                ],
              },
              {
                name: 'node6',
                measures: [],
                dimensions: [
                  {
                    schema: {
                      name: 'id',
                      sql: 'node6.id',
                    },
                    children: [
                      {
                        name: 'node9',
                        measures: [],
                        dimensions: [
                          {
                            schema: {
                              name: 'id',
                              sql: 'node9.id',
                            },
                            children: [],
                          },
                        ],
                      },
                    ],
                  },
                ],
              },
            ],
          },
        ],
      });
    });

    it('Test intermediate join path with depth 0 (should return original graph)', async () => {
      const nestedSchema = await getNestedTableSchema(
        LINEAR_TABLE_SCHEMA,
        INTERMEDIATE_JOIN_PATH,
        0
      );

      expect(nestedSchema).toEqual({
        name: 'node1',
        measures: [],
        dimensions: [
          {
            schema: {
              name: 'id',
              sql: 'node1.id',
            },
            children: [
              {
                name: 'node2',
                measures: [],
                dimensions: [
                  {
                    schema: {
                      name: 'id',
                      sql: 'node2.id',
                    },
                    children: [
                      {
                        name: 'node4',
                        measures: [],
                        dimensions: [
                          {
                            schema: {
                              name: 'id',
                              sql: 'node4.id',
                            },
                            children: [],
                          },
                        ],
                      },
                    ],
                  },
                  {
                    schema: {
                      name: 'node11_id',
                      sql: 'node2.node11_id',
                    },
                    children: [],
                  },
                ],
              },
              {
                name: 'node3',
                measures: [],
                dimensions: [
                  {
                    schema: {
                      name: 'id',
                      sql: 'node3.id',
                    },
                    children: [],
                  },
                ],
              },
            ],
          },
        ],
      });
    });

    it('Test intermediate join path with depth 1', async () => {
      const nestedSchema = await getNestedTableSchema(
        LINEAR_TABLE_SCHEMA,
        INTERMEDIATE_JOIN_PATH,
        1
      );

      expect(nestedSchema).toEqual({
        name: 'node1',
        measures: [],
        dimensions: [
          {
            schema: {
              name: 'id',
              sql: 'node1.id',
            },
            children: [
              {
                name: 'node2',
                measures: [],
                dimensions: [
                  {
                    schema: {
                      name: 'id',
                      sql: 'node2.id',
                    },
                    children: [
                      {
                        name: 'node4',
                        measures: [],
                        dimensions: [
                          {
                            schema: {
                              name: 'id',
                              sql: 'node4.id',
                            },
                            children: [
                              {
                                name: 'node5',
                                measures: [],
                                dimensions: [
                                  {
                                    schema: {
                                      name: 'id',
                                      sql: 'node5.id',
                                    },
                                    children: [],
                                  },
                                ],
                              },
                              {
                                name: 'node6',
                                measures: [],
                                dimensions: [
                                  {
                                    schema: {
                                      name: 'id',
                                      sql: 'node6.id',
                                    },
                                    children: [],
                                  },
                                ],
                              },
                              {
                                name: 'node7',
                                measures: [],
                                dimensions: [
                                  {
                                    schema: {
                                      name: 'id',
                                      sql: 'node7.id',
                                    },
                                    children: [],
                                  },
                                ],
                              },
                            ],
                          },
                        ],
                      },
                    ],
                  },
                  {
                    schema: {
                      name: 'node11_id',
                      sql: 'node2.node11_id',
                    },
                    children: [
                      {
                        name: 'node11',
                        measures: [],
                        dimensions: [
                          {
                            schema: {
                              name: 'id',
                              sql: 'node11.id',
                            },
                            children: [],
                          },
                        ],
                      },
                    ],
                  },
                ],
              },
              {
                name: 'node3',
                measures: [],
                dimensions: [
                  {
                    schema: {
                      name: 'id',
                      sql: 'node3.id',
                    },
                    children: [
                      {
                        name: 'node5',
                        measures: [],
                        dimensions: [
                          {
                            schema: {
                              name: 'id',
                              sql: 'node5.id',
                            },
                            children: [],
                          },
                        ],
                      },
                    ],
                  },
                ],
              },
              {
                name: 'node6',
                measures: [],
                dimensions: [
                  {
                    schema: {
                      name: 'id',
                      sql: 'node6.id',
                    },
                    children: [],
                  },
                ],
              },
            ],
          },
        ],
      });
    });

    it('Test complex join path with depth 1', async () => {
      const nestedSchema = await getNestedTableSchema(
        LINEAR_TABLE_SCHEMA,
        COMPLEX_JOIN_PATH,
        1
      );

      expect(nestedSchema).toEqual(EXPECTED_OUTPUT_WITH_ONE_DEPTH);
    });

    it('Test complex complex join path with depth 2', async () => {
      const nestedSchema = await getNestedTableSchema(
        LINEAR_TABLE_SCHEMA,
        COMPLEX_JOIN_PATH,
        2
      );

      expect(nestedSchema).toEqual(EXPECTED_OUTPUT_WITH_TWO_DEPTH);
    });
  })

  describe('graph with loops', () => {
    it('Test circular graphs', async () => {
      const nestedSchema = await getNestedTableSchema(
        CIRCULAR_TABLE_SCHEMA,
        SINGLE_NODE_JOIN_PATH,
        2
      );
      expect(nestedSchema).toEqual(CIRCULAR_TABLE_SCHEMA_SINGLE_JOIN_PATH);
    });
    it('Test circular graphs', async () => {
      const nestedSchema = await getNestedTableSchema(
        CIRCULAR_TABLE_SCHEMA,
        INTERMEDIATE_JOIN_PATH,
        2
      );
      console.log(JSON.stringify(nestedSchema, null, 2))
      expect(nestedSchema).toEqual(EXPECTED_CIRCULAR_TABLE_SCHEMA_INTERMEDIATE_JOIN_PATH);
    });
    it('Should fail for circular selection path', async () => {
      try {
        await getNestedTableSchema(CIRCULAR_TABLE_SCHEMA, CIRCULAR_JOIN_PATH, 2)
        fail("Expected asyncFunction to throw an error");
      } catch (e) {
        if (e instanceof Error) {
          // Check the error message if e is an Error object
          expect(e.message).toBe("A loop was detected in the joins paths, [[{\"left\":\"node1\",\"right\":\"node3\",\"on\":\"id\"}],[{\"left\":\"node1\",\"right\":\"node2\",\"on\":\"id\"},{\"left\":\"node2\",\"right\":\"node4\",\"on\":\"id\"},{\"left\":\"node4\",\"right\":\"node1\",\"on\":\"id\"}]]");
        } else {
          fail("Expected e to be an instance of Error");
        }
      }
    });
  })
});


