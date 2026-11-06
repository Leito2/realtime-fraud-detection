# Drift demo

| phase | champion | frauds | recall | new-pattern recall | legit flagged | challenger recall |
|---|---|---|---|---|---|---|
| A steady | fraud-champion:3..fraud-champion:3 | 24 | 58.3% | - | 4.9% | 62.5% |
| B new pattern | fraud-champion:3..fraud-champion:3 | 112 | 46.4% | 45.7% (37/81) | 8.6% | 48.2% |
| C champion vs challenger | fraud-champion:3..fraud-champion:3 | 88 | 42.0% | 31.7% (19/60) | 11.3% | 69.3% |

criteria: FPR rises by 0.062, limit 0.005 (recall gain +0.273)
