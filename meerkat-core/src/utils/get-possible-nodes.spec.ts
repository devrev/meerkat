import { getNestedTableSchema } from './get-possible-nodes';

describe('Table schema functions', () => {
  const tableSchema = [
    {
      name: 'node1',
      dimensions: [
        {
          name: 'id',
          sql: 'node1.id',
        },
      ],
      measures: [],
      sql: 'select * from node1',
      joins: [
        { sql: 'node1.id = node2.id' },
        { sql: 'node1.id = node3.id' },
        { sql: 'node1.id = node6.id' },
      ],
    },
    {
      name: 'node2',
      dimensions: [
        {
          name: 'id',
          sql: 'node2.id',
        },
        {
          name: 'node11_id',
          sql: 'node2.node11_id',
        },
      ],
      measures: [],
      sql: 'select * from node2',
      joins: [
        { sql: 'node2.id = node4.id' },
        { sql: 'node2.node11_id = node11.id' },
      ],
    },
    {
      name: 'node3',
      dimensions: [
        {
          name: 'id',
          sql: 'node3.id',
        },
      ],
      measures: [],
      sql: 'select * from node3',
      joins: [{ sql: 'node3.id = node5.id' }],
    },
    {
      name: 'node4',
      dimensions: [
        {
          name: 'id',
          sql: 'node4.id',
        },
      ],
      measures: [],
      sql: 'select * from node4',
      joins: [
        { sql: 'node4.id = node5.id' },
        { sql: 'node4.id = node6.id' },
        { sql: 'node4.id = node7.id' },
      ],
    },
    {
      name: 'node5',
      measures: [],
      dimensions: [
        {
          name: 'id',
          sql: 'node5.id',
        },
      ],
      sql: 'select * from node5',
      joins: [{ sql: 'node5.id = node8.id' }],
    },
    {
      name: 'node6',
      dimensions: [
        {
          name: 'id',
          sql: 'node6.id',
        },
      ],
      measures: [],
      sql: 'select * from node6',
      joins: [{ sql: 'node6.id = node9.id' }],
    },
    {
      name: 'node7',
      dimensions: [
        {
          name: 'id',
          sql: 'node7.id',
        },
      ],
      measures: [],
      sql: 'select * from node7',
      joins: [{ sql: 'node7.id = node10.id' }],
    },
    {
      name: 'node8',
      dimensions: [
        {
          name: 'id',
          sql: 'node8.id',
        },
      ],
      measures: [],
      sql: 'select * from node8',
      joins: [],
    },
    {
      name: 'node9',
      dimensions: [
        {
          name: 'id',
          sql: 'node9.id',
        },
      ],
      measures: [],
      sql: 'select * from node9',
      joins: [{ sql: 'node9.id = node10.id' }],
    },
    {
      name: 'node10',
      dimensions: [
        {
          name: 'id',
          sql: 'node10.id',
        },
      ],
      measures: [],
      sql: 'select * from node10',
      joins: [],
    },
    {
      name: 'node11',
      dimensions: [
        {
          name: 'id',
          sql: 'node11.id',
        },
      ],
      measures: [],
      sql: 'select * from node11',
      joins: [],
    },
  ];

  const basicJoinPath = [
    [
      {
        left: 'node1',
        right: 'node2',
        on: 'id',
      },
    ],
  ];

  const singleNodeJoinPath = [
    [
      {
        left: 'node1',
      },
    ],
  ];

  const intermediateJoinPath = [
    [
      {
        left: 'node1',
        right: 'node2',
        on: 'id',
      },
      {
        left: 'node2',
        right: 'node4',
        on: 'id',
      },
    ],
    [
      {
        left: 'node1',
        right: 'node3',
        on: 'id',
      },
    ],
  ];

  const complexJoinPath = [
    [
      {
        left: 'node1',
        right: 'node2',
        on: 'id',
      },
      {
        left: 'node2',
        right: 'node4',
        on: 'id',
      },
      {
        left: 'node4',
        right: 'node7',
        on: 'id',
      },
    ],
    [
      {
        left: 'node1',
        right: 'node3',
        on: 'id',
      },
    ],
    [
      {
        left: 'node1',
        right: 'node2',
        on: 'id',
      },
      {
        left: 'node2',
        right: 'node11',
        on: 'node11_id',
      },
    ],
  ];

  it('Test single node join path', async () => {
    const nestedSchema = await getNestedTableSchema(
      tableSchema,
      singleNodeJoinPath,
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
      tableSchema,
      basicJoinPath,
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
      tableSchema,
      basicJoinPath,
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
      tableSchema,
      basicJoinPath,
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
      tableSchema,
      intermediateJoinPath,
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
      tableSchema,
      intermediateJoinPath,
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
      tableSchema,
      complexJoinPath,
      1
    );

    expect(nestedSchema).toEqual(expectedOutputWithOneDepth);
  });

  it('Test complex complex join path with depth 2', async () => {
    const nestedSchema = await getNestedTableSchema(
      tableSchema,
      complexJoinPath,
      2
    );

    expect(nestedSchema).toEqual(expectedOutputWithTwoDepth);
  });
});

const expectedOutputWithOneDepth = {
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
                          name: 'node7',
                          measures: [],
                          dimensions: [
                            {
                              schema: {
                                name: 'id',
                                sql: 'node7.id',
                              },
                              children: [
                                {
                                  name: 'node10',
                                  measures: [],
                                  dimensions: [
                                    {
                                      schema: {
                                        name: 'id',
                                        sql: 'node10.id',
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
};

const expectedOutputWithTwoDepth = {
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
                          name: 'node7',
                          measures: [],
                          dimensions: [
                            {
                              schema: {
                                name: 'id',
                                sql: 'node7.id',
                              },
                              children: [
                                {
                                  name: 'node10',
                                  measures: [],
                                  dimensions: [
                                    {
                                      schema: {
                                        name: 'id',
                                        sql: 'node10.id',
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
                          name: 'node5',
                          measures: [],
                          dimensions: [
                            {
                              schema: {
                                name: 'id',
                                sql: 'node5.id',
                              },
                              children: [
                                {
                                  name: 'node8',
                                  measures: [],
                                  dimensions: [
                                    {
                                      schema: {
                                        name: 'id',
                                        sql: 'node8.id',
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
                      children: [
                        {
                          name: 'node8',
                          measures: [],
                          dimensions: [
                            {
                              schema: {
                                name: 'id',
                                sql: 'node8.id',
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
};
