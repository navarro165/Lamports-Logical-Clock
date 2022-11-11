
input_test = [
    {
        "id": 1,
        "type": "customer",
        "events": [{"id": 1, "interface": "query", "money": 400}],
    },
    {
        "id": 2,
        "type": "customer",
        "events": [
            {"id": 2, "interface": "deposit", "money": 170},
            {"id": 3, "interface": "query", "money": 400},
        ],
    },
    {
        "id": 3,
        "type": "customer",
        "events": [
            {"id": 4, "interface": "withdraw", "money": 70},
            {"id": 5, "interface": "query", "money": 400},
        ],
    },
    {"id": 1, "type": "branch", "balance": 400},
    {"id": 2, "type": "branch", "balance": 400},
    {"id": 3, "type": "branch", "balance": 400},
]

expected_output = [
    {
        "pid": 1,
        "data": [
            {"id": 4, "name": "withdraw_propogate_request", "clock": 4},
            {"id": 4, "name": "withdraw_propogate_execute", "clock": 5},
            {"id": 2, "name": "deposit_propogate_request", "clock": 6},
            {"id": 2, "name": "deposit_propogate_execute", "clock": 7},
        ],
    },
    {
        "pid": 2,
        "data": [
            {"id": 2, "name": "deposit_request", "clock": 2},
            {"id": 2, "name": "deposit_execute", "clock": 3},
            {"id": 2, "name": "deposit_propogate_response", "clock": 8},
            {"id": 2, "name": "deposit_propogate_response", "clock": 11},
            {"id": 4, "name": "withdraw_propogate_request", "clock": 12},
            {"id": 4, "name": "withdraw_propogate_execute", "clock": 13},
            {"id": 2, "name": "deposit_response", "clock": 14},
        ],
    },
    {
        "pid": 3,
        "data": [
            {"id": 4, "name": "withdraw_request", "clock": 2},
            {"id": 4, "name": "withdraw_execute", "clock": 3},
            {"id": 4, "name": "withdraw_propogate_response", "clock": 6},
            {"id": 2, "name": "deposit_propogate_request", "clock": 9},
            {"id": 2, "name": "deposit_propogate_execute", "clock": 10},
            {"id": 4, "name": "withdraw_propogate_response", "clock": 14},
            {"id": 4, "name": "withdraw_response", "clock": 15},
        ],
    },
    {
        "eventid": 4,
        "data": [
            {"clock": 2, "name": "withdraw_request"},
            {"clock": 3, "name": "withdraw_execute"},
            {"clock": 4, "name": "withdraw_propogate_request"},
            {"clock": 5, "name": "withdraw_propogate_execute"},
            {"clock": 6, "name": "withdraw_propogate_response"},
            {"clock": 12, "name": "withdraw_propogate_request"},
            {"clock": 13, "name": "withdraw_propogate_execute"},
            {"clock": 14, "name": "withdraw_propogate_response"},
            {"clock": 15, "name": "withdraw_response"},
        ],
    },
    {
        "eventid": 2,
        "data": [
            {"clock": 2, "name": "deposit_request"},
            {"clock": 3, "name": "deposit_execute"},
            {"clock": 6, "name": "deposit_propogate_request"},
            {"clock": 7, "name": "deposit_propogate_execute"},
            {"clock": 8, "name": "deposit_propogate_response"},
            {"clock": 9, "name": "deposit_propogate_request"},
            {"clock": 10, "name": "deposit_propogate_execute"},
            {"clock": 11, "name": "deposit_propogate_response"},
            {"clock": 14, "name": "deposit_response"},
        ],
    },
]
