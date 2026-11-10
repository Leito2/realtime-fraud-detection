# IEEE-CIS offline benchmark (not part of the served system)

590,540 transactions, 391 features, fraud rate 3.50%. Temporal split by `TransactionDT`: 354,324 train / 118,108 valid / 118,108 test rows (test fraud rate 3.44%). Recall is at the cutoff that flags 1% of legitimate payments.

| model | PR-AUC | recall at 1% FPR |
|---|---|---|
| rule: amount only | 0.0365 | 0.0079 |
| XGBoost | 0.4723 | 0.3925 |
| LightGBM | 0.25 | 0.2306 |
