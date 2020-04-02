import logging

from Pegasus.service.base import OrderedDict, OrderedSet

log = logging.getLogger(__name__)


def csv_to_json(csv, schema, index):
    """
    Workflow has a 1-to-many relationship with Job
    Job has a 1-to-many relationship with Task

    schema:

        Key: Entity to be included in output
        Value: 'root' OR tuple of string:json_key, entity:parent-1,  entity:parent-2, ..,  entity:parent-n, type

        1. Must be ordered: depth-first order of the required nesting
        2. First element must root element, i.e. other entities can only be nested within this entity
        3. There should only be one element identified as root

        type:
            list, if items should be placed in a collection
            set|OrderedSet, if items should be placed in a collection of unique items
           None, if item should be included inline

    index:

        Key: Entity
        Value: index of the Entity in the csv data.

        Example: For sample input below
            {
                Workflow: 0,
                Job: 1,
                Task: 2,
                WorkflowState: 3
            }

    Sample Input (csv):

        Workflow | Job | Task | WORKFLOW_STATE
        W1       | J1  | T1   | W1WS1
        W1       | J1  | T2   | W1WS1
        W1       | J1  | T3   | W1WS1

        W1       | J2  | T1   | W1WS1

        W2       | J1  | T1   | W2WS1
        W2       | J1  | T2   | W2WS1
        W2       | J2  | NULL | W2WS1

    schema:

        Example:

        OrderedDict([
            (WORKFLOW, 'root'),
            (JOB, ('jobs', WORKFLOW, set)),
            (TASK, ('tasks', WORKFLOW, JOBS, set)),
            (WORKFLOW_STATE, ('workflow_state', WORKFLOW, None)),
        ])

    Sample Output:

        [
            {
                 W1,
                 "jobs": [
                     {
                         J1,
                         "tasks": [
                             T1,
                             T2,
                             T3
                         ]
                     },
                     {
                         J2,
                         "tasks": [
                             T1
                         ]
                     }
                 ],
                 "workflow_state": W1WS1
            },
            {
                 W2,
                 "jobs": [
                     {
                         J1,
                         "tasks": [
                             T1,
                             T2
                         ]
                     },
                     {
                         J2,
                         "tasks": null
                     }
                 ],
                 "workflow_state": W2WS1
            }
        ]

    schema:

        OrderedDict([
            (WORKFLOW, 'root'),
            (JOB, ('jobs', WORKFLOW, set)),
            (TASK, ('tasks', WORKFLOW, set))
        ])

    Sample Output:

    [
        {
             W1,
             "jobs": [
                     J1,
                     J2,
             ],
             "tasks": [
                 J1T1,
                 J1T2,
                 J1T3,
                 J2T1,
             ]
        },
        {
             W2,
             "jobs": [
                     J1,
                     J2,
             ],
             "tasks": [
                 J1T1,
                 J1T2,
             ]
        },
    ]
    """

    # Sanity checks
    if csv is None:
        return None

    if not schema:
        raise ValueError("schema is required")
    elif not isinstance(schema, dict):
        raise ValueError("schema must be of type dictionary")

    if not index:
        raise ValueError("index is required")
    elif not isinstance(index, dict):
        raise ValueError("index must be of type dictionary")

    # Start
    root = [entity for entity in schema][0]

    uniq_dict = {}

    # Pass 1
    for row in csv:
        for entity, entity_def in schema.items():
            if entity_def == "root":
                entity_dict = uniq_dict.setdefault(root, OrderedSet())
                entity_dict.add(row[index[entity]])

            elif isinstance(entity_def, tuple):
                entity_dict = uniq_dict.setdefault(entity, {})

                for parent in entity_def[1:-1]:
                    if parent == entity_def[-2]:
                        if entity_def[-1] is None:
                            obj = row[index[entity]]
                        else:
                            obj = OrderedSet()
                    else:
                        obj = OrderedDict()

                    entity_dict = entity_dict.setdefault(row[index[parent]], obj)

                if row[index[entity]] and hasattr(entity_dict, "add"):
                    entity_dict.add(row[index[entity]])

    # Pass 2
    for row in csv:
        for entity, entity_def in schema.items():
            if entity_def == "root":
                continue

            elif isinstance(entity_def, tuple):
                # Immediate parent
                parent_entity = entity_def[-2]

                p = uniq_dict[parent_entity]
                data = uniq_dict[entity]

                for parent in entity_def[1:-1]:
                    if parent != parent_entity:
                        p = p[row[index[parent]]]

                    data = data[row[index[parent]]]

                if not data:
                    data = None

                row[index[parent_entity]].__setattr__(entity_def[0], data)

    return uniq_dict[root]
