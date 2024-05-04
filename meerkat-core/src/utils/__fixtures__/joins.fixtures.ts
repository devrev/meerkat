import { TableSchema } from '../../types/cube-types/table';

export const CIRCULAR_TABLE_SCHEMA: TableSchema[] = [
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
            { sql: 'node2.id = node3.id' },
            { sql: 'node2.id = node4.id' },
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
        joins: [
            { sql: 'node3.id = node4.id' },
            { sql: 'node3.id = node1.id' }
        ],
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
        joins: [],
    },
];

export const LINEAR_TABLE_SCHEMA = [
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

export const BASIC_JOIN_PATH = [
    [
        {
            left: 'node1',
            right: 'node2',
            on: 'id',
        },
    ],
];

export const SINGLE_NODE_JOIN_PATH = [
    [
        {
            left: 'node1',
        },
    ],
];

export const INTERMEDIATE_JOIN_PATH = [
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

export const COMPLEX_JOIN_PATH = [
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
    ]
]

export const EXPECTED_OUTPUT_WITH_ONE_DEPTH = {
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

export const EXPECTED_OUTPUT_WITH_TWO_DEPTH = {
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

export const CIRCULAR_TABLE_SCHEMA_SINGLE_JOIN_PATH = {
    "name": "node1",
    "measures": [],
    "dimensions": [
        {
            "schema": {
                "name": "id",
                "sql": "node1.id"
            },
            "children": [
                {
                    "name": "node2",
                    "measures": [],
                    "dimensions": [
                        {
                            "schema": {
                                "name": "id",
                                "sql": "node2.id"
                            },
                            "children": [
                                {
                                    "name": "node3",
                                    "measures": [],
                                    "dimensions": [
                                        {
                                            "schema": {
                                                "name": "id",
                                                "sql": "node3.id"
                                            },
                                            "children": []
                                        }
                                    ]
                                },
                                {
                                    "name": "node4",
                                    "measures": [],
                                    "dimensions": [
                                        {
                                            "schema": {
                                                "name": "id",
                                                "sql": "node4.id"
                                            },
                                            "children": []
                                        }
                                    ]
                                }
                            ]
                        },
                        {
                            "schema": {
                                "name": "node11_id",
                                "sql": "node2.node11_id"
                            },
                            "children": []
                        }
                    ]
                },
                {
                    "name": "node3",
                    "measures": [],
                    "dimensions": [
                        {
                            "schema": {
                                "name": "id",
                                "sql": "node3.id"
                            },
                            "children": [
                                {
                                    "name": "node4",
                                    "measures": [],
                                    "dimensions": [
                                        {
                                            "schema": {
                                                "name": "id",
                                                "sql": "node4.id"
                                            },
                                            "children": []
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        }
    ]
}


export const EXPECTED_CIRCULAR_TABLE_SCHEMA_INTERMEDIATE_JOIN_PATH = {
    "name": "node1",
    "measures": [],
    "dimensions": [
        {
            "schema": {
                "name": "id",
                "sql": "node1.id"
            },
            "children": [
                {
                    "name": "node2",
                    "measures": [],
                    "dimensions": [
                        {
                            "schema": {
                                "name": "id",
                                "sql": "node2.id"
                            },
                            "children": [
                                {
                                    "name": "node4",
                                    "measures": [],
                                    "dimensions": [
                                        {
                                            "schema": {
                                                "name": "id",
                                                "sql": "node4.id"
                                            },
                                            "children": []
                                        }
                                    ]
                                }
                            ]
                        },
                        {
                            "schema": {
                                "name": "node11_id",
                                "sql": "node2.node11_id"
                            },
                            "children": []
                        }
                    ]
                },
                {
                    "name": "node3",
                    "measures": [],
                    "dimensions": [
                        {
                            "schema": {
                                "name": "id",
                                "sql": "node3.id"
                            },
                            "children": []
                        }
                    ]
                }
            ]
        }
    ]
}