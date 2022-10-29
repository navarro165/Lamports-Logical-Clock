input_test = [
    {
        "id": 1,
        "type": "customer",
        "events": [
            {
                "id": 1,
                "interface": "query",
                "money": 400
            }
        ]
    },
    {
        "id": 2,
        "type": "customer",
        "events": [
            {
                "id": 2,
                "interface": "deposit",
                "money": 170
            },
            {
                "id": 3,
                "interface": "query",
                "money": 400
            }
        ]
    },
    {
        "id": 3,
        "type": "customer",
        "events": [
            {
                "id": 4,
                "interface": "withdraw",
                "money": 70
            },
            {
                "id": 5,
                "interface": "query",
                "money": 400
            }
        ]
    },
    {
        "id": 1,
        "type": "branch",
        "balance": 400
    },
    {
        "id": 2,
        "type": "branch",
        "balance": 400
    },
    {
        "id": 3,
        "type": "branch",
        "balance": 400
    }
]

expected_output = [
    {
        "id": 1,
        "recv": [
            {
                "interface": "query",
                "result": "success",
                "money": 500
            }
        ]
    },
    {
        "id": 2,
        "recv": [
            {
                "interface": "deposit",
                "result": "success"
            },
            {
                "interface": "query",
                "result": "success",
                "money": 500
            }
        ]
    },
    {
        "id": 3,
        "recv": [
            {
                "interface": "withdraw",
                "result": "success"
            },
            {
                "interface": "query",
                "result": "success",
                "money": 500
            }
        ]
    }
]
