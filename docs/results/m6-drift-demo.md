# Drift demo

| phase | champion | frauds | recall | new-pattern recall | legit flagged | challenger recall |
|---|---|---|---|---|---|---|
| A steady | fraud-champion:7..fraud-champion:7 | 24 | 66.7% | - | 9.2% | 75.0% |
| B new pattern | fraud-champion:7..fraud-champion:7 | 112 | 50.0% | 46.9% (38/81) | 9.0% | 86.6% |
| C champion vs challenger | fraud-champion:7..fraud-champion:7 | 88 | 35.2% | 25.0% (15/60) | 8.5% | 51.1% |
| D new champion | fraud-champion:8..fraud-champion:8 | 51 | 51.0% | 59.4% (19/32) | 6.3% | 51.0% |

criteria: criteria met (recall gain +0.159)
fraud-champion: champion v7 -> v8
