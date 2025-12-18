def state_color(state):
    return {
        "LEADER": "#00c853",      # xanh lá
        "CANDIDATE": "#ffab00",   # vàng cam
        "FOLLOWER": "#2962ff",    # xanh dương
        "STOPPED": "#9e9e9e",     # xám
    }.get(state, "#bbbbbb")