# Drift demo

| phase | champion | frauds | recall | new-pattern recall | legit flagged | challenger recall |
|---|---|---|---|---|---|---|
| A steady | fraud-champion:3..fraud-champion:3 | 22 | 54.5% | - | 3.8% | 59.1% |
| B new pattern | fraud-champion:3..fraud-champion:3 | 140 | 55.0% | 21.0% (13/62) | 5.7% | 50.7% |
| C champion vs challenger | fraud-champion:3..fraud-champion:3 | 109 | 50.5% | 16.2% (6/37) | 18.6% | 53.2% |

criteria: only 108 labelled events, need 2000 (recall gain -0.333)
