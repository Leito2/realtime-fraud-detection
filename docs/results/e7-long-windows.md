# E7 - value of 1 h and 24 h velocity windows

XGBoost, same temporal split (400,000 train / 100,000 test rows), same parameters. Recall is at the cutoff that flags 1% of legitimate payments.

| | 10-minute features only | + 1 h and 24 h |
|---|---|---|
| PR-AUC | 0.5679 | 0.6176 |
| recall at 1% FPR | 0.6425 | 0.7322 |
| recall, account_takeover | 0.9863 | 0.9863 |
| recall, atypical_amount | 0.64 | 0.64 |
| recall, burst | 0.5827 | 0.7519 |
| recall, card_testing | 0.9013 | 0.9333 |
| recall, geo_jump | 0.3 | 0.44 |
| recall, slow_fraud | 0.0111 | 0.2111 |
